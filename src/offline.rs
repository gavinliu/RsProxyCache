use std::{
    collections::{HashMap, HashSet},
    ops::Range,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, ensure};
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::BodyExt;
use serde::Serialize;
use tokio::sync::{Semaphore, watch};

use crate::{
    cache::{Cache, DOWNLOAD_CACHE_FAILURE, DownloadCacheFailure, key, record_download_failure},
    hls::{self, Resource},
    playback::{Body, Playback, full},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum State {
    Running,
    Completed,
    Cancelled,
    Unsupported,
    InsufficientCapacity,
    Failed,
}

#[derive(Clone, Serialize)]
pub(crate) struct Progress {
    state: State,
    processed_bytes: u64,
    bandwidth: Option<u64>,
    queued: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<&'static str>,
}

struct Task {
    requested: Option<u64>,
    progress: Mutex<Progress>,
    cancel: watch::Sender<bool>,
}

pub(crate) struct Downloads {
    slots: Arc<Semaphore>,
    parallelism: usize,
    queue_capacity: usize,
    tasks: Mutex<HashMap<String, Arc<Task>>>,
    variant_parents: Mutex<HashMap<String, String>>,
}

impl Default for Downloads {
    fn default() -> Self {
        Self::new(2, 16)
    }
}

impl Downloads {
    fn new(parallelism: usize, queue_capacity: usize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(parallelism)),
            parallelism,
            queue_capacity,
            tasks: Mutex::default(),
            variant_parents: Mutex::default(),
        }
    }

    pub(crate) fn note_playlist(&self, source: &str, canonical: &str) {
        if !hls::is_master(canonical) {
            return;
        }
        let mut recent = self.variant_parents.lock().unwrap();
        if recent.len() >= 128 {
            recent.clear();
        }
        for variant in hls::variants(canonical) {
            recent
                .entry(variant.source.to_owned())
                .or_insert_with(|| source.to_owned());
        }
    }

    pub(crate) fn note_variant_request(&self, source: &str) -> Option<String> {
        self.variant_parents.lock().unwrap().get(source).cloned()
    }
}

impl Playback {
    pub fn with_download_limits(
        mut self,
        parallelism: usize,
        queue_capacity: usize,
    ) -> Result<Self> {
        ensure!(
            parallelism > 0 && parallelism <= Semaphore::MAX_PERMITS,
            "invalid offline download parallelism"
        );
        ensure!(
            parallelism.checked_add(queue_capacity).is_some(),
            "invalid offline download queue capacity"
        );
        self.downloads = Downloads::new(parallelism, queue_capacity);
        Ok(self)
    }

    pub(crate) fn start_download(
        self: &Arc<Self>,
        source: &str,
        bandwidth: Option<u64>,
    ) -> Result<(StatusCode, Progress)> {
        self.origin.validate(source)?;
        ensure!(bandwidth != Some(0), "invalid bandwidth");
        let key = key(source);
        let mut tasks = self.downloads.tasks.lock().unwrap();
        if let Some(task) = tasks.get(&key) {
            let progress = task.progress.lock().unwrap().clone();
            if progress.state == State::Running {
                let same = task.requested == bandwidth
                    || bandwidth.is_some() && progress.bandwidth == bandwidth;
                return Ok((
                    if same {
                        StatusCode::ACCEPTED
                    } else {
                        StatusCode::CONFLICT
                    },
                    progress,
                ));
            }
        }
        if tasks.len() >= 256 {
            tasks.retain(|_, task| task.progress.lock().unwrap().state == State::Running);
        }
        let active = tasks
            .values()
            .filter(|task| task.progress.lock().unwrap().state == State::Running)
            .count();
        if active >= self.downloads.parallelism + self.downloads.queue_capacity {
            return Ok((
                StatusCode::TOO_MANY_REQUESTS,
                Progress {
                    state: State::Failed,
                    processed_bytes: 0,
                    bandwidth,
                    queued: false,
                    error_code: Some("queue_full"),
                },
            ));
        }
        let slots = self.downloads.slots.clone();
        let permit = slots.clone().try_acquire_owned().ok();
        let initial = Progress {
            state: State::Running,
            processed_bytes: 0,
            bandwidth,
            queued: permit.is_none(),
            error_code: None,
        };
        let (cancel, mut cancelled) = watch::channel(false);
        let task = Arc::new(Task {
            requested: bandwidth,
            progress: Mutex::new(initial.clone()),
            cancel,
        });
        tasks.insert(key, task.clone());
        let playback = self.clone();
        let source = source.to_owned();
        tokio::spawn(async move {
            let cache_failure = Arc::new(Mutex::new(None));
            let result = tokio::select! {
                biased;
                _ = cancelled.wait_for(|value| *value) => Ok(State::Cancelled),
                result = async {
                    let _permit = match permit {
                        Some(permit) => permit,
                        None => slots.acquire_owned().await?,
                    };
                    task.progress.lock().unwrap().queued = false;
                    let mut pins = TaskPins::new(playback.cache.clone());
                    pins.pin(&source);
                    DOWNLOAD_CACHE_FAILURE
                        .scope(Some(cache_failure.clone()), playback.run_download(&source, &task, &mut pins))
                        .await
                } => result,
            };
            let (state, error_code) = match result {
                Ok(State::InsufficientCapacity) => match *cache_failure.lock().unwrap() {
                    Some(DownloadCacheFailure::Capacity) => (State::InsufficientCapacity, None),
                    Some(DownloadCacheFailure::Write) => {
                        (State::Failed, Some("cache_write_failed"))
                    }
                    Some(DownloadCacheFailure::DiskFull) => (State::Failed, Some("disk_full")),
                    Some(DownloadCacheFailure::Publish) | None => {
                        (State::Failed, Some("publish_failed"))
                    }
                },
                Ok(state) => (state, None),
                Err(error) => {
                    tracing::debug!(error = %error, "offline download failed");
                    let code = if error.chain().any(|cause| {
                        cause.is::<tokio::time::error::Elapsed>()
                            || cause
                                .downcast_ref::<std::io::Error>()
                                .is_some_and(|io| io.kind() == std::io::ErrorKind::TimedOut)
                    }) {
                        "origin_timeout"
                    } else if error
                        .downcast_ref::<crate::origin::OriginAddressDenied>()
                        .is_some()
                    {
                        "origin_address_denied"
                    } else if let Some(failure) = error.downcast_ref::<OriginFailure>() {
                        failure.0
                    } else {
                        "download_failed"
                    };
                    (State::Failed, Some(code))
                }
            };
            let mut progress = task.progress.lock().unwrap();
            progress.queued = false;
            progress.state = state;
            progress.error_code = error_code;
        });
        Ok((StatusCode::ACCEPTED, initial))
    }

    pub(crate) fn download_progress(&self, source: &str, cancel: bool) -> Option<Progress> {
        let task = self
            .downloads
            .tasks
            .lock()
            .unwrap()
            .get(&key(source))
            .cloned()?;
        let progress = task.progress.lock().unwrap().clone();
        if cancel && progress.state == State::Running {
            let _ = task.cancel.send(true);
        }
        Some(progress)
    }

    pub(crate) fn download_request(
        self: &Arc<Self>,
        request: &Request<impl Sized>,
        source: &str,
    ) -> Result<Response<Body>> {
        self.origin.validate(source)?;
        let (status, payload) = match *request.method() {
            Method::POST => {
                let bandwidth = request
                    .headers()
                    .get("x-rpc-bandwidth")
                    .map(|value| value.to_str()?.parse::<u64>().map_err(anyhow::Error::from))
                    .transpose()?;
                let (status, progress) = self.start_download(source, bandwidth)?;
                (status, serde_json::to_vec(&progress)?)
            }
            Method::GET | Method::DELETE => {
                match self.download_progress(source, request.method() == Method::DELETE) {
                    Some(progress) => (StatusCode::OK, serde_json::to_vec(&progress)?),
                    None => (StatusCode::NOT_FOUND, b"{}".to_vec()),
                }
            }
            _ => (StatusCode::METHOD_NOT_ALLOWED, b"{}".to_vec()),
        };
        Ok(Response::builder()
            .status(status)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::CACHE_CONTROL, "no-store")
            .body(full(payload.into()))?)
    }

    async fn run_download(
        self: &Arc<Self>,
        source: &str,
        task: &Task,
        pins: &mut TaskPins,
    ) -> Result<State> {
        let uri = self.origin.validate(source)?;
        let is_hls = if hls::playlist_path(uri.path()) {
            true
        } else {
            let response = self.offline_fetch(source, None, Method::HEAD, None).await?;
            hls::playlist_type(response.headers())
        };
        if !is_hls {
            let complete = self.transfer(source, None, None, task).await?;
            if !complete {
                // A concurrent request may still be writing the whole file.
                self.wait_whole(&key(source)).await;
                // All ranges may already be present after a previous failed publication.
                let _ = self.cache.retry_media_publication(source).await;
            }
            return Ok(if complete || self.cache.contains(source, None) {
                State::Completed
            } else {
                State::InsufficientCapacity
            });
        }
        let published = self.cache.published_playlist(source);
        let canonical = match self.cache.original_playlist(source) {
            Some(playlist) => playlist,
            None => self.download_manifest(source).await?,
        };
        if hls::uses_encryption(&canonical) {
            return Ok(State::Unsupported);
        }
        let (selected, playlists) = if hls::is_master(&canonical) {
            let recent = self.recent_variant(source);
            let mut variants = hls::variants(&canonical);
            variants.retain(|variant| {
                task.requested
                    .is_none_or(|wanted| variant.bandwidth == wanted)
            });
            variants.sort_by_key(|variant| {
                (recent.as_deref() != Some(variant.source), variant.bandwidth)
            });
            ensure!(!variants.is_empty(), "requested HLS variant is unavailable");
            let mut selected = None;
            let mut unsupported = false;
            let mut last_error = None;
            for variant in variants {
                task.progress.lock().unwrap().bandwidth = Some(variant.bandwidth);
                if published.as_deref().is_some_and(|playlist| {
                    hls::variants(playlist)
                        .iter()
                        .any(|cached| cached.source == variant.source)
                }) {
                    return Ok(State::Completed);
                }
                match self.variant_playlists(&canonical, &variant).await {
                    Ok(Some(playlists)) => {
                        selected = Some((variant.source.to_owned(), playlists));
                        break;
                    }
                    Ok(None) => unsupported = true,
                    Err(error) => last_error = Some(error),
                }
            }
            match selected {
                Some((variant, playlists)) => (Some(variant), playlists),
                None if unsupported => return Ok(State::Unsupported),
                None => return Err(last_error.context("no playable HLS variant")?),
            }
        } else {
            if published.is_some() {
                return Ok(State::Completed);
            }
            (None, vec![(source.to_owned(), canonical.clone())])
        };
        for (playlist_source, playlist) in playlists {
            pins.pin(&playlist_source);
            let mut references = Vec::new();
            hls::media_references(&playlist, |address, range| {
                references.push((address.to_owned(), range));
                Ok((address.to_owned(), false))
            })?;
            self.cache.observe_playlist(&playlist_source, playlist);
            let mut seen = HashSet::new();
            for (address, range) in references {
                if seen.insert((address.clone(), range.clone())) {
                    pins.pin(&address);
                    if !self
                        .transfer(&address, Some(Resource::Media), range, task)
                        .await?
                    {
                        return Ok(State::InsufficientCapacity);
                    }
                }
            }
            if self.cache.published_playlist(&playlist_source).is_none() {
                return Ok(State::InsufficientCapacity);
            }
        }
        if hls::is_master(&canonical) {
            self.cache.observe_playlist(source, canonical);
        }
        Ok(
            if self
                .cache
                .published_playlist(source)
                .is_some_and(|playlist| {
                    selected.as_ref().is_none_or(|wanted| {
                        hls::variants(&playlist)
                            .iter()
                            .any(|variant| variant.source == wanted)
                    })
                })
            {
                State::Completed
            } else {
                State::InsufficientCapacity
            },
        )
    }

    async fn variant_playlists(
        &self,
        canonical: &str,
        variant: &hls::Variant<'_>,
    ) -> Result<Option<Vec<(String, String)>>> {
        let mut playlists = Vec::new();
        for address in
            std::iter::once(variant.source).chain(hls::default_audio(canonical, variant.attributes))
        {
            let playlist = match self.cache.original_playlist(address) {
                Some(playlist) => playlist,
                None => self.download_manifest(address).await?,
            };
            if hls::uses_encryption(&playlist) {
                return Ok(None);
            }
            ensure!(!hls::is_master(&playlist), "nested HLS master playlist");
            playlists.push((address.to_owned(), playlist));
        }
        Ok(Some(playlists))
    }

    async fn transfer(
        self: &Arc<Self>,
        source: &str,
        resource: Option<Resource>,
        span: Option<Range<u64>>,
        task: &Task,
    ) -> Result<bool> {
        let mut attempts = 0;
        loop {
            if self
                .transfer_once(source, resource, span.clone(), task)
                .await?
            {
                return Ok(true);
            }
            if span.is_some() || attempts >= 2 {
                return Ok(false);
            }
            attempts += 1;
            // A whole-file request may follow an in-flight cache write instead
            // of staging its own copy. Wait for that write to settle; if the
            // writer failed or its player disconnected, take over the write.
            self.wait_whole(&key(source)).await;
            if self.cache.contains(source, None) {
                return Ok(true);
            }
        }
    }

    async fn transfer_once(
        self: &Arc<Self>,
        source: &str,
        resource: Option<Resource>,
        span: Option<Range<u64>>,
        task: &Task,
    ) -> Result<bool> {
        let mut response = self
            .offline_fetch(source, resource, Method::GET, span.as_ref())
            .await?;
        if response.status().is_success()
            && response
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|length| length > self.cache.capacity())
        {
            record_download_failure(DownloadCacheFailure::Capacity);
            return Ok(false);
        }
        if !response.status().is_success() {
            return Err(anyhow::Error::new(OriginFailure("origin_response"))
                .context(format!("media response returned {}", response.status())));
        }
        while let Some(frame) = response.body_mut().frame().await {
            let frame = frame.context(OriginFailure("origin_body_failed"))?;
            if let Some(data) = frame.data_ref() {
                let mut progress = task.progress.lock().unwrap();
                progress.processed_bytes =
                    progress.processed_bytes.saturating_add(data.len() as u64);
            }
        }
        Ok(self.cache.contains(source, span))
    }

    async fn offline_fetch(
        self: &Arc<Self>,
        source: &str,
        resource: Option<Resource>,
        method: Method,
        span: Option<&Range<u64>>,
    ) -> Result<Response<Body>> {
        let target = match resource {
            Some(resource) => format!("hls/{}/{source}", resource.name()),
            None => source.to_owned(),
        };
        let path = match &self.signing_key {
            Some(key) => crate::mobile::sign(key, &target),
            None => target,
        };
        let mut request = Request::builder()
            .uri(format!("/v1/{path}"))
            .method(method)
            .extension(crate::playback::OfflineFetch);
        if let Some(span) = span {
            request = request.header(header::RANGE, crate::range::upstream(span));
        }
        self.handle(request.body(())?).await
    }

    async fn download_manifest(&self, source: &str) -> Result<String> {
        let origin = self.origin.fetch(source, Method::GET, None).await?;
        if !origin.status().is_success() {
            return Err(anyhow::Error::new(OriginFailure("origin_response"))
                .context(format!("playlist origin returned {}", origin.status())));
        }
        self.canonical_playlist(origin).await
    }
}

#[derive(Debug)]
struct OriginFailure(&'static str);

impl std::error::Error for OriginFailure {}

impl std::fmt::Display for OriginFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

struct TaskPins {
    cache: Arc<Cache>,
    keys: HashSet<String>,
}

impl TaskPins {
    fn new(cache: Arc<Cache>) -> Self {
        Self {
            cache,
            keys: HashSet::new(),
        }
    }

    fn pin(&mut self, source: &str) {
        let key = key(source);
        if self.keys.insert(key.clone()) {
            self.cache.pin(key);
        }
    }
}

impl Drop for TaskPins {
    fn drop(&mut self) {
        for key in &self.keys {
            self.cache.unpin(key);
        }
    }
}
