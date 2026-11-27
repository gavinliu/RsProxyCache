use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    io,
    ops::Range,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use bytes::Bytes;
use futures_util::stream;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full, Limited, StreamBody, combinators::UnsyncBoxBody};
use hyper::body::{Frame, Incoming};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
    sync::{OwnedMutexGuard, mpsc, watch},
    time::timeout,
};

use tracing::Instrument;

use crate::{
    cache::{self, Cache, CacheVersion, Entry, IntervalWriter, Metadata, Piece, WholeWriter},
    hls::{self, Resource},
    origin::{FinalTarget, FinalUrl, Origin, TIMEOUT},
    range::{self, Requested},
};

pub(crate) type Body = UnsyncBoxBody<Bytes, io::Error>;
type Sender = mpsc::Sender<io::Result<Frame<Bytes>>>;

/// Marks an internal offline-download request. Such requests must always write
/// to the cache and never contend for the player-oriented request lock, so a
/// concurrent playback stream cannot turn a download into a cache bypass.
#[derive(Clone)]
pub(crate) struct OfflineFetch;

pub struct Playback {
    pub(crate) downloads: crate::offline::Downloads,
    recent_variants: Mutex<HashMap<String, String>>,
    pub cache: Arc<Cache>,
    pub origin: Origin,
    pub(crate) signing_key: Option<ring::hmac::Key>,
    request_locks: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    gaps: Mutex<HashMap<GapKey, Weak<GapFlight>>>,
    whole_flights: Mutex<HashMap<String, Weak<WholeFlight>>>,
}

enum BodyWriter {
    Interval(IntervalWriter),
    Whole(WholeWriter),
}

enum Prepared {
    Parts(Vec<Part>),
    Failure(Response<Body>),
    Retry,
}

enum Part {
    Local {
        span: Range<u64>,
        entry: Arc<Entry>,
    },
    RemoteRange {
        body: Incoming,
        span: Range<u64>,
        version: Option<Arc<CacheVersion>>,
        flight: Option<GapLeader>,
    },
    RemoteWhole {
        body: Incoming,
        length: Option<u64>,
        writer: Option<Box<WholeWriter>>,
        flight: Option<WholeLeader>,
    },
    Passthrough {
        body: Incoming,
        length: Option<u64>,
    },
}

#[derive(Eq, Hash, PartialEq)]
struct GapKey {
    version: u64,
    span: Range<u64>,
}

#[derive(Clone, Copy)]
enum GapOutcome {
    Ready,
    Retry,
    Failed(StatusCode),
}

struct GapFlight {
    completion: watch::Sender<Option<GapOutcome>>,
}

struct GapLeader(Arc<GapFlight>);

impl GapLeader {
    fn finish(&self, outcome: GapOutcome) {
        self.0.completion.send_replace(Some(outcome));
    }
}

impl Drop for GapLeader {
    fn drop(&mut self) {
        self.0.completion.send_if_modified(|outcome| {
            if outcome.is_some() {
                return false;
            }
            *outcome = Some(GapOutcome::Retry);
            true
        });
    }
}

enum GapClaim {
    Leader(GapLeader),
    Waiting(watch::Receiver<Option<GapOutcome>>),
}

/// Tracks an in-flight whole-file cache write so concurrent requests for the
/// same representation do not stage duplicate copies against the capacity.
struct WholeFlight {
    metadata: Metadata,
    completion: watch::Sender<bool>,
}

struct WholeLeader(Arc<WholeFlight>);

impl Drop for WholeLeader {
    fn drop(&mut self) {
        self.0.completion.send_replace(true);
    }
}

impl Playback {
    pub fn new(cache: Arc<Cache>, origin: Origin) -> Self {
        Self {
            cache,
            origin,
            downloads: Default::default(),
            recent_variants: Mutex::default(),
            signing_key: None,
            request_locks: Mutex::default(),
            gaps: Mutex::default(),
            whole_flights: Mutex::default(),
        }
    }

    pub async fn request(
        self: Arc<Self>,
        request: Request<Incoming>,
    ) -> Result<Response<Body>, Infallible> {
        static NEXT_REQUEST: AtomicU64 = AtomicU64::new(1);
        let request_id = NEXT_REQUEST.fetch_add(1, Ordering::Relaxed);
        let span = tracing::warn_span!("playback", request_id, cache_key = tracing::field::Empty);
        let started = Instant::now();
        let method = request.method().clone();
        let response = async {
            let response = match self.handle(request).await {
                Ok(response) => response,
                Err(error) => {
                    tracing::warn!(error = %format_args!("{error:#}"), "playback request failed");
                    plain(StatusCode::BAD_GATEWAY, "origin or cache request failed")
                }
            };
            tracing::debug!(%method, status = response.status().as_u16(), elapsed_ms = started.elapsed().as_millis(), "response headers ready");
            response
        }.instrument(span).await;
        Ok(response)
    }

    pub(crate) fn recent_variant(&self, source: &str) -> Option<String> {
        self.recent_variants.lock().unwrap().get(source).cloned()
    }

    pub(crate) async fn handle(
        self: &Arc<Self>,
        request: Request<impl Sized>,
    ) -> Result<Response<Body>> {
        let Some(source) = request
            .uri()
            .path_and_query()
            .and_then(|value| value.as_str().strip_prefix("/v1/"))
        else {
            return Ok(plain(StatusCode::NOT_FOUND, "unknown playback path"));
        };
        if self.signing_key.is_none()
            && let Some(source) = source.strip_prefix("offline/")
        {
            return self.download_request(&request, source);
        }
        let source = if let Some(key) = &self.signing_key {
            let Some(source) = crate::mobile::verify(key, source) else {
                return Ok(plain(StatusCode::NOT_FOUND, "unknown playback path"));
            };
            source
        } else {
            source
        };
        let (resource, source) = Resource::from_path(source);
        let Ok(uri) = self.origin.validate(source) else {
            return Ok(plain(
                StatusCode::BAD_REQUEST,
                "invalid or disallowed source URL",
            ));
        };
        let head = request.method() == Method::HEAD;
        if !head && request.method() != Method::GET {
            let mut response = plain(StatusCode::METHOD_NOT_ALLOWED, "use GET or HEAD");
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return Ok(response);
        }
        if !head
            && resource == Some(Resource::Playlist)
            && let Some(parent) = self.downloads.note_variant_request(source)
        {
            let mut recent = self.recent_variants.lock().unwrap();
            if recent.len() >= 128 {
                recent.clear();
            }
            recent.insert(parent, source.to_owned());
        }
        if matches!(resource, None | Some(Resource::Playlist))
            && let Some(playlist) = self.cache.published_playlist(source)
        {
            return self.playlist_response(&playlist, head);
        }
        if resource == Some(Resource::Playlist)
            || (resource.is_none() && hls::playlist_path(uri.path()))
        {
            let origin = self.origin.fetch(source, Method::GET, None).await?;
            return self.playlist(source, origin, head).await;
        }
        if resource == Some(Resource::Key) {
            let origin = self
                .origin
                .fetch(source, request.method().clone(), None)
                .await?;
            return self.key(origin, head).await;
        }
        let requested = if head {
            Requested::Whole
        } else {
            let mut ranges = request.headers().get_all(header::RANGE).iter();
            match ranges.next() {
                None => Requested::Whole,
                Some(value) => match value.to_str().ok().and_then(Requested::parse) {
                    Some(range) if ranges.next().is_none() => range,
                    _ => return Ok(plain(StatusCode::BAD_REQUEST, "expected one bytes range")),
                },
            }
        };
        let key = cache::key(source);
        tracing::Span::current().record("cache_key", &key);
        tracing::debug!(range = %requested.upstream(), "playback request");

        let offline = request.extensions().get::<OfflineFetch>().is_some();
        let guard = if head || offline {
            None
        } else {
            let lock_key = format!("{key}:{}", requested.upstream());
            let wait = match requested {
                Requested::Whole | Requested::From(_, None) => Duration::from_millis(75),
                Requested::From(_, Some(_)) | Requested::Suffix(_) => Duration::from_millis(500),
            };
            timeout(wait, self.lock(&lock_key)).await.ok()
        };
        // A player may open a new connection before draining an earlier response.
        // After a bounded wait, relay a fresh response without competing for its cache writes.
        let bypass_cache = !head && !offline && guard.is_none();
        let generation = self.cache.refresh();
        let previous = self.cache.metadata(&key);
        if head {
            if let Some(metadata) = previous {
                tracing::debug!("cached metadata hit");
                return media_response(requested, 0..metadata.size, &metadata, empty());
            }
            let origin = self.origin.fetch(source, Method::HEAD, None).await?;
            if resource.is_none()
                && origin.status().is_success()
                && hls::playlist_type(origin.headers())
            {
                let origin = self.origin.fetch(source, Method::GET, None).await?;
                return self.playlist(source, origin, true).await;
            }
            return relay_headers(origin.status(), origin.headers(), empty());
        }

        if let Some(metadata) = &previous
            && !bypass_cache
        {
            if let Some(span) = requested.resolve(metadata.size) {
                match self.prepare(source, &key, span.clone(), metadata).await? {
                    Prepared::Parts(parts) => {
                        let hold = if parts.iter().all(|part| matches!(part, Part::Local { .. })) {
                            None
                        } else {
                            guard
                        };
                        let body =
                            self.stream(parts, hold, Some((source.to_owned(), metadata.clone())));
                        return media_response(requested, span, metadata, body);
                    }
                    Prepared::Failure(response) => return Ok(response),
                    Prepared::Retry => {}
                }
            } else if matches!(requested, Requested::Suffix(0)) {
                return unsatisfiable(metadata.size);
            }
        }

        tracing::debug!("cache miss");
        let mut origin = self
            .origin
            .fetch(source, Method::GET, Some(&requested.upstream()))
            .await?;
        if matches!(requested, Requested::Whole)
            && origin.status() == StatusCode::RANGE_NOT_SATISFIABLE
        {
            origin = self.origin.fetch(source, Method::GET, None).await?;
        }

        if resource.is_none()
            && hls::playlist_type(origin.headers())
            && origin.status().is_success()
        {
            if origin.status() == StatusCode::PARTIAL_CONTENT {
                // A source can cap an open-ended Range response. Fetch the complete manifest.
                let (span, metadata) = partial_metadata(&origin)?;
                if span != (0..metadata.size) {
                    origin = self.origin.fetch(source, Method::GET, None).await?;
                }
            }
            return self.playlist(source, origin, false).await;
        }

        match origin.status() {
            StatusCode::PARTIAL_CONTENT => {
                let (span, metadata) = partial_metadata(&origin)?;
                ensure!(
                    requested.resolve(metadata.size).as_ref() == Some(&span),
                    "unexpected origin byte range"
                );
                let version = if bypass_cache {
                    None
                } else {
                    self.cache.adopt(source, metadata.clone(), generation)
                };
                let part = Part::RemoteRange {
                    body: origin.into_body(),
                    version,
                    span: span.clone(),
                    flight: None,
                };
                let body = self.stream(vec![part], guard, None);
                media_response(requested, span, &metadata, body)
            }
            StatusCode::OK if matches!(requested, Requested::Whole) => {
                identity(origin.headers())?;
                let status = origin.status();
                let headers = origin.headers().clone();
                let length = content_length(&headers)?;
                let part = if !bypass_cache
                    && (offline
                        || resource == Some(Resource::Media)
                        || uri
                            .path()
                            .as_bytes()
                            .last_chunk::<4>()
                            .is_some_and(|suffix| suffix.eq_ignore_ascii_case(b".mp4"))
                        || headers
                            .get(header::CONTENT_TYPE)
                            .and_then(|value| value.to_str().ok())
                            .is_some_and(|value| value.starts_with("video/")))
                {
                    let metadata = origin_metadata(&origin, length.unwrap_or_default())?;
                    let flight = self.claim_whole(&key, &metadata);
                    let writer = flight
                        .as_ref()
                        .and_then(|_| {
                            self.cache
                                .whole_writer(source.to_owned(), metadata, length, generation)
                        })
                        .map(Box::new);
                    Part::RemoteWhole {
                        body: origin.into_body(),
                        length,
                        writer,
                        flight,
                    }
                } else {
                    Part::Passthrough {
                        body: origin.into_body(),
                        length,
                    }
                };
                let body = self.stream(vec![part], guard, None);
                relay_headers(status, &headers, body)
            }
            StatusCode::OK => anyhow::bail!("origin ignored Range"),
            status if status.is_client_error() || status.is_server_error() => {
                relay_headers(status, origin.headers(), empty())
            }
            _ => anyhow::bail!("unexpected origin response"),
        }
    }

    async fn key(&self, origin: Response<Incoming>, head: bool) -> Result<Response<Body>> {
        let status = origin.status();
        let headers = origin.headers().clone();
        let body = if status.is_success() {
            ensure!(status == StatusCode::OK, "unexpected HLS key status");
            identity(&headers)?;
            ensure!(
                content_length(&headers)?.is_none_or(|length| length == 16),
                "invalid AES-128 key length"
            );
            if head {
                empty()
            } else {
                let bytes = collect_limited(origin.into_body(), 16).await?;
                ensure!(bytes.len() == 16, "invalid AES-128 key length");
                full(bytes)
            }
        } else {
            empty()
        };
        let mut response = relay_headers(status, &headers, body)?;
        response
            .headers_mut()
            .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
        Ok(response)
    }

    async fn playlist(
        &self,
        source: &str,
        origin: Response<Incoming>,
        head: bool,
    ) -> Result<Response<Body>> {
        if !origin.status().is_success() {
            return relay_headers(origin.status(), origin.headers(), empty());
        }
        let canonical = self.canonical_playlist(origin).await?;
        self.downloads.note_playlist(source, &canonical);
        if !head {
            self.cache.observe_playlist(source, canonical.clone());
        }
        self.playlist_response(&canonical, head)
    }

    pub(crate) async fn canonical_playlist(&self, origin: Response<Incoming>) -> Result<String> {
        identity(origin.headers())?;
        if origin.status() == StatusCode::PARTIAL_CONTENT {
            let (span, metadata) = partial_metadata(&origin)?;
            ensure!(span == (0..metadata.size), "incomplete HLS playlist");
        } else {
            ensure!(
                origin.status() == StatusCode::OK,
                "unexpected HLS playlist status"
            );
        }
        let base = url::Url::parse(
            &origin
                .extensions()
                .get::<FinalUrl>()
                .context("missing playlist URL")?
                .0,
        )?;
        let body = collect_limited(origin.into_body(), hls::MAX_PLAYLIST_BYTES).await?;
        hls::rewrite(std::str::from_utf8(&body)?, |_, reference| {
            let address = hls::resolve(&base, reference)?;
            self.origin.validate(&address)?;
            Ok(address)
        })
    }

    fn playlist_response(&self, canonical: &str, head: bool) -> Result<Response<Body>> {
        let playlist = hls::rewrite(canonical, |resource, source| {
            self.origin.validate(source)?;
            let path = format!("hls/{}/{source}", resource.name());
            Ok(match &self.signing_key {
                Some(key) => format!("/v1/{}", crate::mobile::sign(key, &path)),
                None => format!("/v1/{path}"),
            })
        })?;
        Ok(Response::builder()
            .header(header::CONTENT_TYPE, "application/vnd.apple.mpegurl")
            .header(header::CONTENT_LENGTH, playlist.len())
            .header(header::CACHE_CONTROL, "no-store")
            .body(if head { empty() } else { full(playlist.into()) })?)
    }

    async fn lock(&self, key: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.request_locks.lock().unwrap();
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(tokio::sync::Mutex::new(()));
                locks.insert(key.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    fn claim_gap(&self, key: GapKey) -> GapClaim {
        let mut gaps = self.gaps.lock().unwrap();
        gaps.retain(|_, flight| flight.strong_count() > 0);
        if let Some(flight) = gaps.get(&key).and_then(Weak::upgrade)
            && flight.completion.borrow().is_none()
        {
            return GapClaim::Waiting(flight.completion.subscribe());
        }
        let flight = Arc::new(GapFlight {
            completion: watch::channel(None).0,
        });
        gaps.insert(key, Arc::downgrade(&flight));
        GapClaim::Leader(GapLeader(flight))
    }

    /// Only one whole-file write per cache key and representation at a time.
    /// A different representation may take over; its adoption retires the old
    /// version so the previous writer stops reserving capacity.
    fn claim_whole(&self, key: &str, metadata: &Metadata) -> Option<WholeLeader> {
        let mut flights = self.whole_flights.lock().unwrap();
        flights.retain(|_, flight| flight.strong_count() > 0);
        if let Some(flight) = flights.get(key).and_then(Weak::upgrade)
            && !*flight.completion.borrow()
            && flight.metadata == *metadata
        {
            return None;
        }
        let flight = Arc::new(WholeFlight {
            metadata: metadata.clone(),
            completion: watch::channel(false).0,
        });
        flights.insert(key.to_owned(), Arc::downgrade(&flight));
        Some(WholeLeader(flight))
    }

    pub(crate) async fn wait_whole(&self, key: &str) {
        let receiver = {
            let mut flights = self.whole_flights.lock().unwrap();
            flights.retain(|_, flight| flight.strong_count() > 0);
            flights
                .get(key)
                .and_then(Weak::upgrade)
                .map(|flight| flight.completion.subscribe())
        };
        if let Some(mut receiver) = receiver {
            let _ = receiver.wait_for(|done| *done).await;
        }
    }

    async fn prepare(
        &self,
        source: &str,
        key: &str,
        span: Range<u64>,
        metadata: &Metadata,
    ) -> Result<Prepared> {
        let plan = loop {
            let plan = self.cache.plan(key, span.clone(), metadata);
            if plan.iter().all(|piece| match piece {
                Piece::Cached(entry, span) => self.cache.open_entry(entry, span).is_ok(),
                Piece::Missing(_) => true,
            }) {
                break plan;
            }
        };
        if !plan.iter().any(|piece| matches!(piece, Piece::Cached(..))) {
            return Ok(Prepared::Retry);
        }
        let version = if plan.iter().any(|piece| matches!(piece, Piece::Missing(_))) {
            let Some(version) = self.cache.version_for_patch(key, metadata) else {
                return Ok(Prepared::Retry);
            };
            Some(version)
        } else {
            None
        };

        tracing::debug!(
            start = span.start,
            end_exclusive = span.end,
            partial = version.is_some(),
            "cache hit"
        );
        let mut pending = VecDeque::from(plan);
        let mut parts = Vec::with_capacity(pending.len());
        // Acquire gaps in increasing byte order, even when a gap has split after waiting.
        while let Some(piece) = pending.pop_front() {
            match piece {
                Piece::Cached(entry, span) => parts.push(Part::Local { span, entry }),
                Piece::Missing(span) => {
                    let version = version.as_ref().context("missing cache version")?;
                    if !self.cache.is_current(version) {
                        return Ok(Prepared::Retry);
                    }
                    let claim = self.claim_gap(GapKey {
                        version: version.id(),
                        span: span.clone(),
                    });
                    let flight = match claim {
                        GapClaim::Leader(flight) => flight,
                        GapClaim::Waiting(mut completion) => {
                            let result = *completion.wait_for(Option::is_some).await?;
                            match result.context("missing gap outcome")? {
                                GapOutcome::Failed(status) => {
                                    return Ok(Prepared::Failure(plain(
                                        status,
                                        "shared origin request failed",
                                    )));
                                }
                                GapOutcome::Retry => return Ok(Prepared::Retry),
                                GapOutcome::Ready => {}
                            }
                            pending.push_front(Piece::Missing(span));
                            continue;
                        }
                    };
                    let current = self.cache.plan(key, span.clone(), metadata);
                    if current
                        .iter()
                        .any(|piece| matches!(piece, Piece::Cached(..)))
                    {
                        flight.finish(GapOutcome::Ready);
                        for piece in current.into_iter().rev() {
                            pending.push_front(piece);
                        }
                        continue;
                    }
                    let response = self.gap_response(source, &span, &flight).await?;
                    let status = response.status();
                    if status == StatusCode::RANGE_NOT_SATISFIABLE {
                        return Ok(Prepared::Retry);
                    }
                    if status.is_client_error() || status.is_server_error() {
                        return Ok(Prepared::Failure(relay_headers(
                            status,
                            response.headers(),
                            empty(),
                        )?));
                    }
                    let (actual_span, actual) = partial_metadata(&response).inspect_err(|_| {
                        flight.finish(GapOutcome::Failed(StatusCode::BAD_GATEWAY));
                    })?;
                    if actual != *metadata {
                        tracing::info!("origin representation changed");
                        self.cache.invalidate(version);
                        return Ok(Prepared::Retry);
                    }
                    if actual_span != span {
                        flight.finish(GapOutcome::Failed(StatusCode::BAD_GATEWAY));
                        anyhow::bail!("unexpected origin byte range");
                    }
                    parts.push(Part::RemoteRange {
                        body: response.into_body(),
                        version: Some(version.clone()),
                        span,
                        flight: Some(flight),
                    });
                }
            }
        }
        if version
            .as_ref()
            .is_some_and(|version| !self.cache.is_current(version))
        {
            return Ok(Prepared::Retry);
        }
        if parts.iter().any(|part| match part {
            Part::Local { span, entry } => self.cache.open_entry(entry, span).is_err(),
            _ => false,
        }) {
            return Ok(Prepared::Retry);
        }
        Ok(Prepared::Parts(parts))
    }

    async fn gap_response(
        &self,
        source: &str,
        span: &Range<u64>,
        flight: &GapLeader,
    ) -> Result<Response<Incoming>> {
        let result = self
            .origin
            .fetch(source, Method::GET, Some(&range::upstream(span)))
            .await;
        match result.as_ref().map(Response::status) {
            Ok(StatusCode::RANGE_NOT_SATISFIABLE | StatusCode::PARTIAL_CONTENT) => {}
            Ok(status) if status.is_client_error() || status.is_server_error() => {
                flight.finish(GapOutcome::Failed(status));
            }
            _ => {
                flight.finish(GapOutcome::Failed(StatusCode::BAD_GATEWAY));
                ensure!(result.is_err(), "origin ignored Range");
            }
        }
        result
    }

    fn stream(
        self: &Arc<Self>,
        parts: Vec<Part>,
        guard: Option<OwnedMutexGuard<()>>,
        local_context: Option<(String, Metadata)>,
    ) -> Body {
        let (sender, receiver) = mpsc::channel(4);
        let playback = self.clone();
        let failure = cache::DOWNLOAD_CACHE_FAILURE
            .try_with(Clone::clone)
            .ok()
            .flatten();
        tokio::spawn(cache::DOWNLOAD_CACHE_FAILURE.scope(failure, async move {
            let _guard = guard;
            for part in parts {
                let result = match part {
                    Part::Local { span, entry } => {
                        match playback.cache.open_entry(&entry, &span) {
                            Ok((file, offset)) => {
                                entry.touch(&file);
                                send_local(&sender, file, offset, span.end - span.start).await
                            }
                            Err(_) => {
                                let (source, metadata) = local_context.as_ref().expect("local playback context");
                                playback.recover_stream_range(&sender, source, metadata, span).await
                            }
                        }
                    }
                    Part::RemoteRange {
                        body,
                        span,
                        version,
                        flight,
                    } => {
                        let length = span.end - span.start;
                        let writer = version.and_then(|version| playback.cache.writer(version, span));
                        let result = send_remote(&sender, body, Some(length), writer.map(BodyWriter::Interval)).await;
                        if let Some(flight) = flight {
                            flight.finish(if result.is_ok() {
                                GapOutcome::Ready
                            } else {
                                GapOutcome::Failed(StatusCode::BAD_GATEWAY)
                            });
                        }
                        result
                    }
                    Part::RemoteWhole { body, length, writer, flight } => {
                        let result = send_remote(&sender, body, length, writer.map(|writer| BodyWriter::Whole(*writer))).await;
                        drop(flight);
                        result
                    }
                    Part::Passthrough { body, length } => {
                        send_remote(&sender, body, length, None).await
                    }
                };
                if let Err(error) = result {
                    if sender.is_closed() {
                        tracing::debug!("client closed playback response");
                    } else {
                        tracing::warn!(error = %format_args!("{error:#}"), "playback stream interrupted");
                        let kind = if error.downcast_ref::<tokio::time::error::Elapsed>().is_some() {
                            io::ErrorKind::TimedOut
                        } else {
                            io::ErrorKind::Other
                        };
                        let _ = sender.send(Err(io::Error::new(kind, error))).await;
                    }
                    break;
                }
            }
        }.in_current_span()));
        StreamBody::new(stream::unfold(receiver, |mut receiver| async {
            receiver.recv().await.map(|frame| (frame, receiver))
        }))
        .boxed_unsync()
    }
    async fn recover_stream_range(
        &self,
        sender: &Sender,
        source: &str,
        metadata: &Metadata,
        span: Range<u64>,
    ) -> Result<()> {
        ensure!(
            metadata.etag.is_some(),
            "cannot recover stream without a strong ETag"
        );
        let response = self
            .origin
            .fetch(source, Method::GET, Some(&range::upstream(&span)))
            .await?;
        ensure!(
            response.status() == StatusCode::PARTIAL_CONTENT,
            "origin did not return the missing stream range"
        );
        let (actual_span, actual) = partial_metadata(&response)?;
        ensure!(
            actual_span == span && actual == *metadata,
            "origin representation changed during playback"
        );
        let length = span.end - span.start;
        // Another request may already have adopted a newer version.
        let writer = self
            .cache
            .version_for_patch(&cache::key(source), metadata)
            .and_then(|version| self.cache.writer(version, span));
        send_remote(
            sender,
            response.into_body(),
            Some(length),
            writer.map(BodyWriter::Interval),
        )
        .await
    }
}

async fn send_local(
    sender: &Sender,
    file: std::fs::File,
    offset: u64,
    mut remaining: u64,
) -> Result<()> {
    let mut file = File::from_std(file);
    file.seek(SeekFrom::Start(offset)).await?;
    let mut buffer = vec![0; 64 * 1024];
    while remaining > 0 {
        let size = (remaining.min(buffer.len() as u64)) as usize;
        let size = file.read(&mut buffer[..size]).await?;
        ensure!(size != 0, "cache file ended early");
        sender
            .send(Ok(Frame::data(Bytes::copy_from_slice(&buffer[..size]))))
            .await?;
        remaining -= size as u64;
    }
    Ok(())
}

async fn send_remote(
    sender: &Sender,
    mut body: Incoming,
    length: Option<u64>,
    mut writer: Option<BodyWriter>,
) -> Result<()> {
    let mut received = 0_u64;
    let mut last = None;
    let disconnected = loop {
        let frame = tokio::select! {
            _ = sender.closed() => break true,
            frame = timeout(TIMEOUT, body.frame()) => frame?,
        };
        let Some(frame) = frame else { break false };
        let Ok(data) = frame?.into_data() else {
            continue;
        };
        if data.is_empty() {
            continue;
        }
        received = received
            .checked_add(data.len() as u64)
            .context("body too large")?;
        ensure!(
            length.is_none_or(|length| received <= length),
            "origin exceeded declared length"
        );
        match &mut writer {
            Some(BodyWriter::Interval(writer)) => writer.write(&data).await,
            Some(BodyWriter::Whole(whole)) => {
                if whole.write(&data).await.is_none() {
                    writer = None;
                }
            }
            None => {}
        }
        // Hold the final bytes until length is verified and the completed interval is published.
        if length == Some(received) {
            last = Some(data);
        } else if sender.send(Ok(Frame::data(data))).await.is_err() {
            break true;
        }
    };
    if disconnected {
        if let Some(BodyWriter::Interval(writer)) = writer {
            writer.finish_interrupted().await;
        }
        return Ok(());
    }
    ensure!(
        length.is_none_or(|length| received == length),
        "origin ended early"
    );
    match writer {
        Some(BodyWriter::Interval(writer)) => writer.finish().await,
        Some(BodyWriter::Whole(writer)) => writer.finish().await,
        None => {}
    }
    if let Some(last) = last {
        sender.send(Ok(Frame::data(last))).await?;
    }
    Ok(())
}

fn partial_metadata(response: &Response<Incoming>) -> Result<(Range<u64>, Metadata)> {
    let headers = response.headers();
    identity(headers)?;
    let (span, size) = headers
        .get(header::CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(range::content_range)
        .context("invalid Content-Range")?;
    ensure!(
        content_length(headers)?.is_none_or(|length| length == span.end - span.start),
        "invalid origin length"
    );
    Ok((span, origin_metadata(response, size)?))
}

fn origin_metadata(response: &Response<Incoming>, size: u64) -> Result<Metadata> {
    let headers = response.headers();
    let target = response
        .extensions()
        .get::<FinalTarget>()
        .context("missing origin target")?;
    let etag = headers
        .get(header::ETAG)
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            value.len() >= 2
                && value.starts_with('"')
                && value.ends_with('"')
                && !value[1..value.len() - 1].contains('"')
        })
        .map(str::to_owned);
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    Ok(Metadata {
        target: target.0.clone(),
        size,
        etag,
        content_type,
    })
}

async fn collect_limited(body: Incoming, limit: usize) -> Result<Bytes> {
    Ok(timeout(TIMEOUT, Limited::new(body, limit).collect())
        .await?
        .map_err(anyhow::Error::from_boxed)?
        .to_bytes())
}

fn content_length(headers: &HeaderMap) -> Result<Option<u64>> {
    headers
        .get(header::CONTENT_LENGTH)
        .map(|value| Ok(value.to_str()?.parse()?))
        .transpose()
}

fn identity(headers: &HeaderMap) -> Result<()> {
    ensure!(
        headers
            .get(header::CONTENT_ENCODING)
            .is_none_or(|value| { value.as_bytes().eq_ignore_ascii_case(b"identity") }),
        "origin encoded the representation"
    );
    Ok(())
}

fn media_response(
    requested: Requested,
    span: Range<u64>,
    metadata: &Metadata,
    body: Body,
) -> Result<Response<Body>> {
    let mut response = Response::builder()
        .header(header::CONTENT_LENGTH, span.end - span.start)
        .header(header::ACCEPT_RANGES, "bytes");
    if !matches!(requested, Requested::Whole) {
        response = response.status(StatusCode::PARTIAL_CONTENT).header(
            header::CONTENT_RANGE,
            format!("bytes {}-{}/{}", span.start, span.end - 1, metadata.size),
        );
    }
    if let Some(content_type) = &metadata.content_type {
        response = response.header(header::CONTENT_TYPE, content_type);
    }
    if let Some(etag) = &metadata.etag {
        response = response.header(header::ETAG, etag);
    }
    Ok(response.body(body)?)
}

fn relay_headers(status: StatusCode, headers: &HeaderMap, body: Body) -> Result<Response<Body>> {
    let mut response = Response::builder().status(status);
    if status.is_success() {
        for name in [
            header::CONTENT_LENGTH,
            header::CONTENT_TYPE,
            header::ACCEPT_RANGES,
            header::ETAG,
            header::LAST_MODIFIED,
        ] {
            if let Some(value) = headers.get(&name) {
                response = response.header(name, value);
            }
        }
    } else if status == StatusCode::RANGE_NOT_SATISFIABLE
        && let Some(value) = headers.get(header::CONTENT_RANGE)
    {
        response = response.header(header::CONTENT_RANGE, value);
    }
    Ok(response.body(body)?)
}

fn unsatisfiable(size: u64) -> Result<Response<Body>> {
    Ok(Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(header::CONTENT_RANGE, format!("bytes */{size}"))
        .body(empty())?)
}

fn plain(status: StatusCode, message: &'static str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(full(Bytes::from_static(message.as_bytes())))
        .unwrap()
}

fn empty() -> Body {
    full(Bytes::new())
}

pub(crate) fn full(bytes: Bytes) -> Body {
    Full::new(bytes)
        .map_err(|never| match never {})
        .boxed_unsync()
}
