use std::{
    collections::{HashMap, HashSet},
    fs::{self, File},
    io::Write,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

use crate::hls;

mod published;
use anyhow::{Context, Result, ensure};
use published::{covering_entries, publish_complete, publish_playlist};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{fs::File as AsyncFile, io::AsyncWriteExt};

#[derive(Clone, Copy)]
pub(crate) enum DownloadCacheFailure {
    Capacity,
    Write,
    DiskFull,
    Publish,
}

tokio::task_local! {
    pub(crate) static DOWNLOAD_CACHE_FAILURE: Option<Arc<Mutex<Option<DownloadCacheFailure>>>>;
}

#[derive(Debug)]
struct PublicationCapacity;

impl std::fmt::Display for PublicationCapacity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("insufficient space to publish cache")
    }
}

pub(crate) fn record_download_failure(failure: DownloadCacheFailure) {
    let _ = DOWNLOAD_CACHE_FAILURE.try_with(|slot| {
        if let Some(slot) = slot {
            let mut current = slot.lock().unwrap();
            if current.is_none() || matches!(current.as_ref(), Some(DownloadCacheFailure::Capacity))
            {
                *current = Some(failure);
            }
        }
    });
}

fn record_io_failure(error: Option<&std::io::Error>, otherwise: DownloadCacheFailure) {
    record_download_failure(
        if error.is_some_and(|error| error.kind() == std::io::ErrorKind::StorageFull) {
            DownloadCacheFailure::DiskFull
        } else {
            otherwise
        },
    );
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Metadata {
    pub target: String,
    pub size: u64,
    pub etag: Option<String>,
    pub content_type: Option<String>,
}

const INTERNAL: &str = ".rpc";

pub struct CacheVersion {
    source: String,
    key: String,
    metadata: Metadata,
    id: u64,
}

impl CacheVersion {
    pub fn id(&self) -> u64 {
        self.id
    }
}

#[derive(Deserialize, Serialize)]
struct Record {
    source: String,
    key: String,
    published: Option<PathBuf>,
    modified: Option<SystemTime>,
    playlist: Option<PublishedPlaylist>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    playlist_digest: Option<String>,
    span: Range<u64>,
    metadata: Metadata,
}

#[derive(Clone, Deserialize, Serialize)]
struct PublishedPlaylist {
    canonical: String,
    original: String,
    dependencies: Vec<PublishedDependency>,
}

#[derive(Clone, Deserialize, Serialize, Hash, Eq, PartialEq)]
struct PublishedDependency {
    key: String,
    path: PathBuf,
}

pub struct Entry {
    record: Record,
    path: PathBuf,
    sidecar: PathBuf,
    used: Mutex<SystemTime>,
    retired: AtomicBool,
}

impl Entry {
    fn matches_published_file(&self) -> bool {
        let Some(relative) = &self.record.published else {
            return false;
        };
        if !self
            .path
            .ancestors()
            .skip(1)
            .take(relative.components().count() - 1)
            .all(|path| fs::symlink_metadata(path).is_ok_and(|metadata| metadata.is_dir()))
        {
            return false;
        }
        fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.is_file()
                && metadata.len() == self.size()
                && metadata.modified().ok() == self.record.modified
        })
    }

    fn size(&self) -> u64 {
        self.record.span.end - self.record.span.start
    }

    fn open(&self, span: &Range<u64>) -> Result<(File, u64)> {
        if self.record.published.is_some() {
            ensure!(
                self.matches_published_file(),
                "published cache file changed"
            );
        }
        let file = File::open(&self.path)?;
        if !self.retired.load(Ordering::Acquire) {
            fs::metadata(&self.sidecar)?;
        }
        ensure!(
            file.metadata()?.len() == self.size()
                && self.record.modified.is_none_or(|time| file
                    .metadata()
                    .and_then(|meta| meta.modified())
                    .ok()
                    == Some(time)),
            "invalid cache file length"
        );
        Ok((file, span.start - self.record.span.start))
    }

    pub fn touch(&self, file: &File) {
        let now = SystemTime::now();
        *self.used.lock().unwrap() = now;
        let result = if self.record.published.is_some() {
            File::open(&self.sidecar).and_then(|file| file.set_modified(now))
        } else {
            file.set_modified(now)
        };
        if let Err(error) = result {
            cache_warning("update_interval_access_time", &error);
        }
    }
}

impl Drop for Entry {
    fn drop(&mut self) {
        if *self.retired.get_mut() && self.record.published.is_none() {
            remove(&self.path);
        }
    }
}

pub enum Piece {
    Cached(Arc<Entry>, Range<u64>),
    Missing(Range<u64>),
}

pub struct Cache {
    dir: PathBuf,
    capacity: u64,
    index: Mutex<Index>,
    /// Test seams, per instance so parallel tests on different caches never
    /// consume each other's hooks. They fire where an unknown-length whole
    /// finish corrects its recorded size (finish) and between that correction
    /// and the commit (commit).
    #[cfg(test)]
    whole_finish_race: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    #[cfg(test)]
    whole_commit_race: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

#[derive(Default)]
struct Index {
    entries: HashMap<String, Vec<Arc<Entry>>>,
    size: u64,
    reserved: u64,
    /// Staged bytes still held by in-flight writers, per version id, so quota
    /// owned by a retired representation can be reclaimed without waiting for
    /// its (possibly paused) writer to drop.
    reservations: HashMap<u64, (String, u64)>,
    versions: HashMap<String, Weak<CacheVersion>>,
    next_version: u64,
    retired: Vec<(Weak<Entry>, u64)>,
    lease: Option<DirectoryLease>,
    generation: u64,
    playlists: HashMap<String, (Instant, String)>,
    pins: HashMap<String, usize>,
    publishing: HashSet<u64>,
}

struct Reservation {
    cache: Arc<Cache>,
    size: u64,
    generation: u64,
    version: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut index = self.cache.index.lock().unwrap();
        if self.generation == index.generation {
            index.release_reservation(self.version, self.size);
        }
    }
}

impl Cache {
    pub fn open(dir: PathBuf, capacity: u64) -> Result<Arc<Self>> {
        let index = Self::load_index(&dir, capacity)?;
        Ok(Self::with_index(dir, capacity, index))
    }

    pub(crate) fn open_recoverable(dir: PathBuf, capacity: u64) -> Arc<Self> {
        let index = Self::load_index(&dir, capacity).unwrap_or_else(|error| {
            cache_warning("open_directory", &format_args!("{error:#}"));
            Index::default()
        });
        Self::with_index(dir, capacity, index)
    }

    fn with_index(dir: PathBuf, capacity: u64, index: Index) -> Arc<Self> {
        tracing::debug!(
            intervals = index.entries.values().map(Vec::len).sum::<usize>(),
            bytes = index.size,
            "cache opened"
        );
        Arc::new(Self {
            dir,
            capacity,
            index: Mutex::new(index),
            #[cfg(test)]
            whole_finish_race: Mutex::new(None),
            #[cfg(test)]
            whole_commit_race: Mutex::new(None),
        })
    }

    fn load_index(dir: &Path, capacity: u64) -> Result<Index> {
        let lease = DirectoryLease::acquire(dir)?;
        let mut index = Index {
            lease: Some(lease),
            ..Index::default()
        };
        let internal = dir.join(INTERNAL);
        ensure_directory(&internal)?;
        for item in fs::read_dir(&internal)? {
            let path = item?.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                match load(dir, &path) {
                    Ok(entry) => index.insert(entry),
                    Err(_) => {
                        cache_warning("load_interval", &"invalid or unreadable cache record");
                        remove(&path);
                        remove(&path.with_extension("part"));
                    }
                }
            }
        }
        for item in fs::read_dir(&internal)? {
            let path = item?.path();
            if (path.extension().is_some_and(|ext| ext == "part")
                && !path.with_extension("json").is_file())
                || path
                    .file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with(".rpc-stage-"))
            {
                remove(&path);
            }
        }
        index.reconcile_publications();
        let invalid: Vec<_> = index
            .entries
            .values()
            .flatten()
            .filter(|entry| {
                entry.record.playlist.is_some()
                    && !index.valid_published_graph(entry, &mut HashSet::new())
            })
            .map(|entry| entry.record.key.clone())
            .collect();
        for key in invalid {
            index.clear(&key);
        }
        let masters: Vec<_> = index
            .entries
            .values()
            .flatten()
            .filter_map(|entry| {
                let playlist = entry.record.playlist.as_ref()?;
                hls::is_master(&playlist.original)
                    .then(|| (entry.record.source.clone(), playlist.original.clone()))
            })
            .collect();
        for (source, original) in masters {
            index.remember_playlist(&source, original);
        }
        index.evict(capacity);
        Ok(index)
    }

    pub(crate) fn capacity(&self) -> u64 {
        self.capacity
    }

    pub(crate) fn pin(&self, key: String) {
        let mut index = self.index.lock().unwrap();
        self.refresh_index(&mut index);
        *index.pins.entry(key).or_default() += 1;
    }

    pub(crate) fn unpin(&self, key: &str) {
        let mut index = self.index.lock().unwrap();
        if let Some(count) = index.pins.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                index.pins.remove(key);
            }
        }
    }

    pub(crate) fn contains(&self, source: &str, span: Option<Range<u64>>) -> bool {
        let key = key(source);
        let index = self.index.lock().unwrap();
        match span {
            Some(span) => index.entries.get(&key).is_some_and(|entries| {
                entries
                    .last()
                    .and_then(|entry| covering_entries(entries, &entry.record.metadata, &span))
                    .is_some_and(|covered| {
                        covered.iter().all(|(entry, span)| entry.open(span).is_ok())
                    })
            }),
            None => index
                .published(&key)
                .is_some_and(|entry| entry.matches_published_file()),
        }
    }

    pub(crate) async fn retry_media_publication(self: &Arc<Self>, source: &str) -> Result<()> {
        let key = key(source);
        let Some(metadata) = self.metadata(&key) else {
            return Ok(());
        };
        let Some(version) = self.version_for_patch(&key, &metadata) else {
            return Ok(());
        };
        let cache = self.clone();
        let failure = DOWNLOAD_CACHE_FAILURE.try_with(Clone::clone).ok().flatten();
        tokio::task::spawn_blocking(move || {
            DOWNLOAD_CACHE_FAILURE.sync_scope(failure, || {
                publish_complete(&cache, &version).inspect_err(|error| {
                    record_io_failure(error.downcast_ref(), DownloadCacheFailure::Publish);
                })
            })
        })
        .await?
    }

    pub(crate) fn observe_playlist(&self, source: &str, canonical: String) {
        let mut index = self.index.lock().unwrap();
        if self.refresh_index(&mut index) {
            index.remember_playlist(source, canonical);
            index.publish_playlists(&self.dir, self.capacity);
        }
    }

    pub(crate) fn published_playlist(&self, source: &str) -> Option<String> {
        self.playlist_snapshot(source, false)
    }

    pub(crate) fn original_playlist(&self, source: &str) -> Option<String> {
        self.playlist_snapshot(source, true)
    }

    fn playlist_snapshot(&self, source: &str, original: bool) -> Option<String> {
        let mut index = self.index.lock().unwrap();
        if !self.refresh_index(&mut index) {
            return None;
        }
        let key = key(source);
        let mut visited = HashSet::new();
        let entry = index.published(&key)?;
        let playlist = entry.record.playlist.as_ref()?;
        if !index.valid_published_graph(entry, &mut visited) {
            index.clear(&key);
            return None;
        }
        for key in visited {
            if let Some(entry) = index.published_at(&key.key, &key.path)
                && let Ok(file) = File::open(&entry.path)
            {
                entry.touch(&file);
            }
        }
        Some(
            if original {
                &playlist.original
            } else {
                &playlist.canonical
            }
            .clone(),
        )
    }

    pub fn refresh(&self) -> u64 {
        let mut index = self.index.lock().unwrap();
        self.refresh_index(&mut index);
        index.generation
    }

    fn refresh_index(&self, index: &mut Index) -> bool {
        if index
            .lease
            .as_ref()
            .is_some_and(|lease| lease.is_current(&self.dir))
        {
            return true;
        }
        if index.lease.is_some() {
            tracing::warn!("cache directory ownership lost");
            index.reset();
        }
        match Self::load_index(&self.dir, self.capacity) {
            Ok(mut recovered) => {
                recovered.next_version = index.next_version;
                recovered.pins = std::mem::take(&mut index.pins);
                recovered.generation = index.generation + 1;
                *index = recovered;
                tracing::info!("cache directory recovered");
                true
            }
            Err(error) => {
                record_io_failure(error.downcast_ref(), DownloadCacheFailure::Write);
                cache_warning("recover_directory", &format_args!("{error:#}"));
                false
            }
        }
    }

    pub fn open_entry(&self, entry: &Arc<Entry>, span: &Range<u64>) -> Result<(File, u64)> {
        entry.open(span).inspect_err(|error| {
            cache_warning("read_interval", error);
            let mut index = self.index.lock().unwrap();
            if !self.refresh_index(&mut index) {
                return;
            }
            let key = &entry.record.key;
            if let Some(entries) = index.entries.get_mut(key)
                && let Some(position) = entries
                    .iter()
                    .position(|indexed| Arc::ptr_eq(indexed, entry))
            {
                let entry = entries.swap_remove(position);
                if entries.is_empty() {
                    index.entries.remove(key);
                }
                entry.retired.store(true, Ordering::Release);
                remove(&entry.sidecar);
                index.retired.push((Arc::downgrade(&entry), entry.size()));
                index.clear_dependents(key);
            }
        })
    }

    pub fn metadata(&self, key: &str) -> Option<Metadata> {
        loop {
            let entry = self.index.lock().unwrap().entries.get(key)?.last()?.clone();
            if self.open_entry(&entry, &entry.record.span).is_ok() {
                return Some(entry.record.metadata.clone());
            }
        }
    }

    pub fn plan(&self, key: &str, span: Range<u64>, metadata: &Metadata) -> Vec<Piece> {
        let index = self.index.lock().unwrap();
        let Some(entries) = index.entries.get(key) else {
            return vec![Piece::Missing(span)];
        };
        if metadata.etag.is_none() {
            return entries
                .iter()
                .find(|entry| {
                    entry.record.metadata == *metadata
                        && entry.record.span.start <= span.start
                        && entry.record.span.end >= span.end
                })
                .map_or_else(
                    || vec![Piece::Missing(span.clone())],
                    |entry| vec![Piece::Cached(entry.clone(), span.clone())],
                );
        }
        let mut intervals = entries
            .iter()
            .filter(|entry| entry.record.metadata == *metadata)
            .collect::<Vec<_>>();
        intervals.sort_unstable_by_key(|entry| entry.record.span.start);
        let mut intervals = intervals.into_iter().peekable();
        let mut pieces = Vec::new();
        let mut cursor = span.start;
        while cursor < span.end {
            let mut cached = None;
            while intervals
                .peek()
                .is_some_and(|entry| entry.record.span.start <= cursor)
            {
                let entry = intervals.next().unwrap();
                if entry.record.span.end > cursor
                    && cached.is_none_or(|best: &Arc<Entry>| {
                        best.record.span.end < entry.record.span.end
                    })
                {
                    cached = Some(entry);
                }
            }
            if let Some(entry) = cached {
                let next = entry.record.span.end.min(span.end);
                pieces.push(Piece::Cached(entry.clone(), cursor..next));
                cursor = next;
            } else {
                let next = intervals
                    .peek()
                    .map_or(span.end, |entry| entry.record.span.start.min(span.end));
                pieces.push(Piece::Missing(cursor..next));
                cursor = next;
            }
        }
        pieces
    }

    fn reserve(self: &Arc<Self>, size: u64, version: &CacheVersion) -> Option<Reservation> {
        if size == 0 || size > self.capacity {
            record_download_failure(DownloadCacheFailure::Capacity);
            return None;
        }
        let mut index = self.index.lock().unwrap();
        if !self.refresh_index(&mut index) || !index.is_current(version) {
            return None;
        }
        index
            .reserve(self.capacity, size, Some(version))
            .or_else(|| {
                record_download_failure(DownloadCacheFailure::Capacity);
                None
            })?;
        Some(Reservation {
            cache: self.clone(),
            size,
            generation: index.generation,
            version: version.id,
        })
    }

    pub(crate) fn whole_writer(
        self: &Arc<Self>,
        source: String,
        metadata: Metadata,
        length: Option<u64>,
        generation: u64,
    ) -> Option<WholeWriter> {
        if self.capacity == 0 || length.is_some_and(|length| length == 0 || length > self.capacity)
        {
            return None;
        }
        let mut index = self.index.lock().unwrap();
        if !self.refresh_index(&mut index) || index.generation != generation {
            return None;
        }
        let stage = self
            .stage_file()
            .inspect_err(|error| {
                record_io_failure(error.downcast_ref(), DownloadCacheFailure::Write);
                cache_warning("stage_media", error);
            })
            .ok()?;
        drop(index);
        // Adopt at header time so a later finish cannot retire a newer
        // representation adopted while this download was still in flight.
        let version = self.adopt(&source, metadata, generation)?;
        let reservation = Reservation {
            cache: self.clone(),
            size: 0,
            generation,
            version: version.id,
        };
        Some(WholeWriter {
            source,
            version,
            data: StagedInterval::open(reservation, stage, 0..0).ok()?,
        })
    }

    pub fn writer(
        self: &Arc<Self>,
        version: Arc<CacheVersion>,
        span: Range<u64>,
    ) -> Option<IntervalWriter> {
        let chunk_size = if version.metadata.etag.is_some() {
            self.capacity.min(1024 * 1024)
        } else {
            span.end - span.start
        };
        (chunk_size > 0).then(|| IntervalWriter {
            cache: self.clone(),
            version,
            chunk_size,
            end: span.end,
            next: span.start,
            chunk_end: span.start,
            chunk: None,
        })
    }

    pub fn is_current(&self, version: &Arc<CacheVersion>) -> bool {
        self.index.lock().unwrap().is_current(version)
    }

    pub fn invalidate(&self, version: &Arc<CacheVersion>) {
        let mut index = self.index.lock().unwrap();
        if index.is_current(version) {
            index.clear(&version.key);
        }
    }

    fn stage(&self, version: &CacheVersion) -> Result<Option<tempfile::NamedTempFile>> {
        let mut index = self.index.lock().unwrap();
        if !self.refresh_index(&mut index) || !index.is_current(version) {
            return Ok(None);
        }
        self.stage_file().map(Some)
    }

    fn stage_file(&self) -> Result<tempfile::NamedTempFile> {
        Ok(tempfile::Builder::new()
            .prefix(".rpc-stage-")
            .tempfile_in(self.dir.join(INTERNAL))?)
    }

    pub fn adopt(
        &self,
        source: &str,
        metadata: Metadata,
        generation: u64,
    ) -> Option<Arc<CacheVersion>> {
        let key = key(source);
        let mut index = self.index.lock().unwrap();
        if !self.refresh_index(&mut index) || index.generation != generation {
            return None;
        }
        if let Some(active) = index.active(&key)
            && active.metadata == metadata
        {
            return Some(active);
        }
        if index
            .entries
            .get(&key)
            .and_then(|entries| entries.last())
            .is_some_and(|entry| entry.record.metadata != metadata)
        {
            index.clear(&key);
        }
        Some(index.set_version(source, metadata))
    }

    /// Record the received size of an unknown-length whole response and adopt
    /// the corrected representation in a single index lock, verifying the
    /// expected version is still current. A newer representation adopted while
    /// the download was in flight must never be retired by this late finish.
    pub(crate) fn adopt_received_size(
        &self,
        source: &str,
        expected: &CacheVersion,
        size: u64,
        generation: u64,
    ) -> Option<Arc<CacheVersion>> {
        let key = key(source);
        let mut index = self.index.lock().unwrap();
        if !self.refresh_index(&mut index)
            || index.generation != generation
            || !index.is_current(expected)
        {
            return None;
        }
        let mut metadata = expected.metadata.clone();
        metadata.size = size;
        if index
            .entries
            .get(&key)
            .and_then(|entries| entries.last())
            .is_some_and(|entry| entry.record.metadata != metadata)
        {
            index.clear(&key);
        }
        let updated = index.set_version(source, metadata);
        // The same writer keeps staging the same bytes under the corrected
        // version id. Migrate its quota in this lock so the window before
        // commit neither looks stale (another reserve reclaiming live quota)
        // nor leaks at commit (which releases by the adopted id).
        if let Some((staged_key, size)) = index.reservations.remove(&expected.id) {
            index
                .reservations
                .entry(updated.id)
                .or_insert_with(|| (staged_key, 0))
                .1 += size;
        }
        Some(updated)
    }

    pub fn version_for_patch(&self, key: &str, metadata: &Metadata) -> Option<Arc<CacheVersion>> {
        let mut index = self.index.lock().unwrap();
        if let Some(active) = index.active(key) {
            return (active.metadata == *metadata).then_some(active);
        }
        let last = index.entries.get(key)?.last()?;
        let source = last.record.source.clone();
        (last.record.metadata == *metadata).then(|| index.set_version(&source, metadata.clone()))
    }

    pub fn clear(&self, key: &str) {
        self.index.lock().unwrap().clear_group(key);
        tracing::info!(cache_key = key, "cache cleared");
    }
}

impl Reservation {
    fn commit(
        mut self,
        stage: tempfile::NamedTempFile,
        span: Range<u64>,
        version: Arc<CacheVersion>,
    ) -> Result<()> {
        let name = stage
            .path()
            .file_name()
            .and_then(|value| value.to_str())
            .context("invalid staging name")?;
        let id = name
            .strip_prefix(".rpc-stage-")
            .context("invalid staging prefix")?;
        let sidecar_path = self.cache.dir.join(INTERNAL).join(format!("{id}.json"));
        let path = sidecar_path.with_extension("part");
        let record = Record {
            source: version.source.clone(),
            published: None,
            modified: None,
            playlist: None,
            playlist_digest: None,
            key: version.key.clone(),
            span,
            metadata: version.metadata.clone(),
        };
        ensure!(
            record.span.end - record.span.start == stage.as_file().metadata()?.len(),
            "staged length mismatch"
        );
        stage.as_file().sync_data()?;
        if record.span == (0..version.metadata.size) {
            let mut index = self.cache.index.lock().unwrap();
            if !self.cache.refresh_index(&mut index) || !index.is_current(&version) {
                return Ok(());
            }
            if index.published(&version.key).is_none() {
                published::publish_staged_media(&self.cache.dir, &mut index, &version, stage)
                    .inspect_err(|error| {
                        record_io_failure(error.downcast_ref(), DownloadCacheFailure::Publish)
                    })?;
                index.release_reservation(version.id, self.size);
                self.size = 0;
                index.publish_playlists(&self.cache.dir, self.cache.capacity);
            }
            return Ok(());
        }
        let Some(mut sidecar) = self.cache.stage(&version)? else {
            return Ok(());
        };
        serde_json::to_writer(&mut sidecar, &record)?;
        sidecar.flush()?;
        sidecar.as_file().sync_data()?;
        let mut index = self.cache.index.lock().unwrap();
        if !self.cache.refresh_index(&mut index) || !index.is_current(&version) {
            return Ok(());
        }
        stage.persist_noclobber(&path)?;
        if let Err(error) = sidecar.persist_noclobber(&sidecar_path) {
            remove(&path);
            return Err(error.into());
        }
        index.release_reservation(version.id, self.size);
        self.size = 0;
        tracing::debug!(
            start = record.span.start,
            end_exclusive = record.span.end,
            "cache interval published"
        );
        index.insert(Entry {
            record,
            path,
            sidecar: sidecar_path,
            used: Mutex::new(SystemTime::now()),
            retired: AtomicBool::new(false),
        });
        drop(index);
        if let Err(error) = publish_complete(&self.cache, &version) {
            record_io_failure(error.downcast_ref(), DownloadCacheFailure::Publish);
            cache_warning("publish_media", &format_args!("{error:#}"));
        }
        let mut index = self.cache.index.lock().unwrap();
        if self.cache.refresh_index(&mut index) {
            index.publish_playlists(&self.cache.dir, self.cache.capacity);
        }
        Ok(())
    }
}

impl Index {
    fn reconcile_publications(&mut self) {
        for entries in self.entries.values_mut() {
            let mut paths = HashSet::new();
            entries.sort_by_key(|entry| std::cmp::Reverse(*entry.used.lock().unwrap()));
            entries.retain(|entry| {
                if entry.record.published.is_some() && !paths.insert(entry.path.clone()) {
                    remove(&entry.sidecar);
                    self.size -= entry.size();
                    return false;
                }
                true
            });
            let published: Vec<_> = entries
                .iter()
                .filter(|entry| entry.record.published.is_some() && entry.record.playlist.is_none())
                .cloned()
                .collect();
            entries.retain(|entry| {
                let superseded = entry.record.published.is_none()
                    && published.iter().any(|file| {
                        file.record.metadata == entry.record.metadata
                            && file.record.span.start <= entry.record.span.start
                            && file.record.span.end >= entry.record.span.end
                    });
                if superseded {
                    delete(entry);
                    self.size -= entry.size();
                }
                !superseded
            });
            entries.sort_by_key(|entry| entry.record.published.is_some());
        }
    }

    fn reset(&mut self) {
        *self = Self {
            next_version: self.next_version,
            pins: std::mem::take(&mut self.pins),
            generation: self.generation + 1,
            ..Self::default()
        };
    }

    fn active(&self, key: &str) -> Option<Arc<CacheVersion>> {
        self.versions.get(key)?.upgrade()
    }

    fn is_current(&self, version: &CacheVersion) -> bool {
        self.active(&version.key)
            .is_some_and(|active| std::ptr::eq(active.as_ref(), version))
    }

    fn set_version(&mut self, source: &str, metadata: Metadata) -> Arc<CacheVersion> {
        self.versions
            .retain(|_, version| version.strong_count() > 0);
        let key = key(source);
        let version = Arc::new(CacheVersion {
            source: source.to_owned(),
            key: key.clone(),
            metadata,
            id: self.next_version,
        });
        self.next_version += 1;
        self.versions.insert(key, Arc::downgrade(&version));
        version
    }

    fn clear_group(&mut self, key: &str) {
        let dependencies = self
            .published(key)
            .and_then(|entry| entry.record.playlist.as_ref())
            .map(|playlist| playlist.dependencies.clone())
            .unwrap_or_default();
        self.clear(key);
        for dependency in dependencies {
            let shared = self
                .entries
                .values()
                .flatten()
                .filter_map(|entry| entry.record.playlist.as_ref())
                .any(|playlist| {
                    playlist
                        .dependencies
                        .iter()
                        .any(|item| item.key == dependency.key)
                });
            if !shared && !self.pins.contains_key(&dependency.key) {
                self.clear_group(&dependency.key);
            }
        }
    }

    fn clear_dependents(&mut self, key: &str) {
        let dependents: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entries)| {
                entries.iter().any(|entry| {
                    entry.record.playlist.as_ref().is_some_and(|playlist| {
                        playlist.dependencies.iter().any(|item| item.key == key)
                    })
                })
            })
            .map(|(candidate, _)| candidate.clone())
            .collect();
        for dependent in dependents {
            self.clear(&dependent);
        }
    }

    fn clear(&mut self, key: &str) {
        let entries = self.entries.remove(key).unwrap_or_default();
        self.clear_dependents(key);
        self.versions.remove(key);
        for entry in entries {
            if entry.record.playlist.is_some() {
                self.playlists.remove(&entry.record.source);
            }
            entry.retired.store(true, Ordering::Release);
            remove(&entry.sidecar);
            if entry.record.published.is_some() && entry.matches_published_file() {
                remove(&entry.path);
            }
            self.retired.push((Arc::downgrade(&entry), entry.size()));
        }
    }

    fn published(&self, key: &str) -> Option<&Arc<Entry>> {
        self.entries.get(key)?.iter().find(|entry| {
            entry.record.published.is_some() && entry.record.span == (0..entry.record.metadata.size)
        })
    }

    fn published_at(&self, key: &str, path: &Path) -> Option<&Arc<Entry>> {
        self.entries
            .get(key)?
            .iter()
            .find(|entry| entry.record.published.as_deref() == Some(path))
    }

    fn valid_published_graph(
        &self,
        entry: &Entry,
        visited: &mut HashSet<PublishedDependency>,
    ) -> bool {
        let Some(path) = &entry.record.published else {
            return false;
        };
        if !visited.insert(PublishedDependency {
            key: entry.record.key.clone(),
            path: path.clone(),
        }) {
            return true;
        }
        entry.matches_published_file()
            && entry.record.playlist.as_ref().is_none_or(|playlist| {
                playlist.dependencies.iter().all(|dependency| {
                    self.published_at(&dependency.key, &dependency.path)
                        .is_some_and(|entry| self.valid_published_graph(entry, visited))
                })
            })
    }

    fn publish_playlists(&mut self, root: &Path, capacity: u64) {
        let mut changed = true;
        while changed {
            changed = false;
            let sources: Vec<_> = self.playlists.keys().cloned().collect();
            for source in sources {
                if publish_playlist(root, self, capacity, &source).unwrap_or_else(|error| {
                    if error.downcast_ref::<PublicationCapacity>().is_some() {
                        record_download_failure(DownloadCacheFailure::Capacity);
                    } else {
                        record_io_failure(error.downcast_ref(), DownloadCacheFailure::Publish);
                    }
                    cache_warning("publish_playlist", &format_args!("{error:#}"));
                    false
                }) {
                    if !self
                        .playlists
                        .get(&source)
                        .is_some_and(|(_, playlist)| hls::is_master(playlist))
                    {
                        self.playlists.remove(&source);
                    }
                    changed = true;
                }
            }
        }
    }

    fn remember_playlist(&mut self, source: &str, canonical: String) {
        if self.playlists.len() >= 128
            && !self.playlists.contains_key(source)
            && let Some(oldest) = self
                .playlists
                .iter()
                .filter(|(source, _)| !self.pins.contains_key(&key(source)))
                .min_by_key(|(_, (seen, _))| *seen)
                .map(|(source, _)| source.clone())
        {
            self.playlists.remove(&oldest);
        }
        self.playlists
            .insert(source.to_owned(), (Instant::now(), canonical));
    }

    fn insert(&mut self, entry: Entry) {
        self.size += entry.size();
        self.entries
            .entry(entry.record.key.clone())
            .or_default()
            .push(Arc::new(entry));
    }

    fn reserve(&mut self, capacity: u64, size: u64, version: Option<&CacheVersion>) -> Option<()> {
        self.release_stale_reservations();
        let remaining = capacity.checked_sub(size.checked_add(self.reserved)?)?;
        self.evict(remaining);
        if self.size > remaining {
            return None;
        }
        self.reserved += size;
        if let Some(version) = version {
            let tracked = self
                .reservations
                .entry(version.id)
                .or_insert_with(|| (version.key.clone(), 0));
            tracked.1 += size;
        }
        Some(())
    }

    /// Return quota still held by writers whose representation has been
    /// retired, so a paused or abandoned old writer cannot starve a newer one.
    fn release_stale_reservations(&mut self) {
        let stale: Vec<u64> = self
            .reservations
            .iter()
            .filter(|(id, (key, _))| self.active(key).is_none_or(|active| active.id != **id))
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            if let Some((_, size)) = self.reservations.remove(&id) {
                self.reserved -= size;
            }
        }
    }

    fn release_reservation(&mut self, version: u64, size: u64) {
        if let Some((_, tracked)) = self.reservations.get_mut(&version) {
            let released = size.min(*tracked);
            *tracked -= released;
            self.reserved -= released;
            if *tracked == 0 {
                self.reservations.remove(&version);
            }
        }
    }

    fn reap_retired(&mut self) {
        self.retired.retain(|(entry, size)| {
            if entry.strong_count() > 0 {
                return true;
            }
            self.size -= size;
            false
        });
    }

    fn evict(&mut self, capacity: u64) {
        self.reap_retired();
        if self.size <= capacity {
            return;
        }
        while self.size > capacity {
            let dependencies: HashSet<_> = self
                .entries
                .values()
                .flatten()
                .filter_map(|entry| entry.record.playlist.as_ref())
                .flat_map(|playlist| playlist.dependencies.iter().map(|item| &item.key))
                .collect();
            let Some((key, position)) = self
                .entries
                .iter()
                .flat_map(|(key, entries)| {
                    entries
                        .iter()
                        .enumerate()
                        .map(move |(position, entry)| (key, position, entry))
                })
                .filter(|(key, _, entry)| {
                    Arc::strong_count(entry) == 1
                        && !dependencies.contains(key)
                        && !self.pins.contains_key(*key)
                })
                .min_by_key(|(_, _, entry)| *entry.used.lock().unwrap())
                .map(|(key, position, _)| (key.clone(), position))
            else {
                break;
            };
            if self.entries[&key][position].record.playlist.is_some() {
                self.clear_group(&key);
                self.reap_retired();
                continue;
            }
            let entries = self.entries.get_mut(&key).unwrap();
            let entry = entries.swap_remove(position);
            if entries.is_empty() {
                self.entries.remove(&key);
            }
            self.size -= entry.size();
            delete(&entry);
        }
    }
}

pub fn key(source: &str) -> String {
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

fn ensure_directory(path: &Path) -> Result<()> {
    match fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure!(
                fs::symlink_metadata(path)?.is_dir(),
                "not a cache directory"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn load(root: &Path, sidecar: &Path) -> Result<Entry> {
    let record: Record = serde_json::from_reader(File::open(sidecar)?)?;
    ensure!(key(&record.source) == record.key, "invalid cache key");
    let path = if let Some(relative) = &record.published {
        ensure!(relative.components().count() >= 2, "invalid published path");
        let mut path = root.to_path_buf();
        for component in relative.components() {
            let std::path::Component::Normal(name) = component else {
                anyhow::bail!("invalid published path");
            };
            path.push(name);
            ensure!(
                !fs::symlink_metadata(&path)?.is_symlink(),
                "symlink in published path"
            );
        }
        path
    } else {
        sidecar.with_extension("part")
    };
    let metadata = fs::symlink_metadata(&path)?;
    ensure!(
        record.span.start < record.span.end && record.span.end <= record.metadata.size,
        "invalid span"
    );
    ensure!(
        metadata.is_file()
            && metadata.len() == record.span.end - record.span.start
            && record
                .modified
                .is_none_or(|time| metadata.modified().ok() == Some(time)),
        "invalid cache file"
    );
    if let Some(digest) = &record.playlist_digest {
        let contents = fs::read_to_string(&path)?;
        ensure!(
            key(&contents) == *digest,
            "published playlist content changed"
        );
    }
    let used = if record.published.is_some() {
        fs::metadata(sidecar)?.modified()?
    } else {
        metadata.modified()?
    };
    Ok(Entry {
        record,
        path,
        sidecar: sidecar.to_path_buf(),
        used: Mutex::new(used),
        retired: AtomicBool::new(false),
    })
}

fn delete(entry: &Entry) {
    remove(&entry.sidecar);
    if entry.record.published.is_none() || entry.matches_published_file() {
        remove(&entry.path);
    }
}

fn remove(path: &Path) {
    if let Err(error) = fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        cache_warning("remove_file", &error);
    }
}

pub struct IntervalWriter {
    cache: Arc<Cache>,
    version: Arc<CacheVersion>,
    chunk_size: u64,
    end: u64,
    next: u64,
    chunk_end: u64,
    chunk: Option<StagedInterval>,
}

impl IntervalWriter {
    pub async fn write(&mut self, mut data: &[u8]) {
        while !data.is_empty() {
            if self.next == self.chunk_end {
                self.chunk_end = self.next.saturating_add(self.chunk_size).min(self.end);
                self.chunk =
                    StagedInterval::new(&self.cache, self.next..self.chunk_end, &self.version);
            }
            let size = (self.chunk_end - self.next).min(data.len() as u64) as usize;
            if let Some(chunk) = &mut self.chunk
                && let Err(error) = chunk.file.write_all(&data[..size]).await
            {
                record_io_failure(Some(&error), DownloadCacheFailure::Write);
                cache_warning("write_interval", &error);
                self.chunk = None;
            }
            self.next += size as u64;
            data = &data[size..];
            if self.next == self.chunk_end
                && self.next != self.end
                && let Some(chunk) = self.chunk.take()
            {
                chunk.commit(self.version.clone()).await;
            }
        }
    }

    pub async fn finish(self) {
        if let Some(chunk) = self.chunk {
            chunk.commit(self.version).await;
        }
    }

    pub(crate) async fn finish_interrupted(self) {
        if let Some(mut chunk) = self.chunk
            && chunk.span.start < self.next
            && self.next < self.end
        {
            chunk.span.end = self.next;
            chunk.commit(self.version).await;
        }
    }
}

pub(crate) struct WholeWriter {
    source: String,
    version: Arc<CacheVersion>,
    data: StagedInterval,
}

impl WholeWriter {
    pub(crate) async fn write(&mut self, bytes: &[u8]) -> Option<()> {
        let reservation = &mut self.data.reservation;
        let cache = &reservation.cache;
        {
            let mut index = cache.index.lock().unwrap();
            if !cache.refresh_index(&mut index)
                || reservation.generation != index.generation
                || !index.is_current(&self.version)
            {
                return None;
            }
            index
                .reserve(cache.capacity, bytes.len() as u64, Some(&self.version))
                .or_else(|| {
                    record_download_failure(DownloadCacheFailure::Capacity);
                    None
                })?;
            reservation.size += bytes.len() as u64;
        }
        self.data
            .file
            .write_all(bytes)
            .await
            .inspect_err(|error| {
                record_io_failure(Some(error), DownloadCacheFailure::Write);
                cache_warning("write_media", error);
            })
            .ok()?;
        self.data.span.end = reservation.size;
        Some(())
    }

    pub(crate) async fn finish(mut self) {
        let received = self.data.reservation.size;
        if received == 0 {
            return;
        }
        let cache = self.data.reservation.cache.clone();
        let generation = self.data.reservation.generation;
        let mut version = self.version.clone();
        if version.metadata.size != received {
            // The origin omitted Content-Length; record the received size only
            // while this representation is still the current one. The check and
            // the size adoption share one index lock so a newer version adopted
            // in between cannot be retired by this late finish.
            #[cfg(test)]
            if let Some(race) = cache.whole_finish_race.lock().unwrap().take() {
                race();
            }
            let Some(updated) =
                cache.adopt_received_size(&self.source, &version, received, generation)
            else {
                return;
            };
            version = updated;
            // The quota moved to the corrected id inside adopt_received_size;
            // keep the reservation identity in sync so a failed or skipped
            // commit still releases it on drop.
            self.data.reservation.version = version.id;
            #[cfg(test)]
            if let Some(race) = cache.whole_commit_race.lock().unwrap().take() {
                race();
            }
        }
        if cache.is_current(&version) {
            self.data.commit(version).await;
        }
    }
}

struct StagedInterval {
    reservation: Reservation,
    stage: tempfile::NamedTempFile,
    file: AsyncFile,
    span: Range<u64>,
}

impl StagedInterval {
    fn new(cache: &Arc<Cache>, span: Range<u64>, version: &CacheVersion) -> Option<Self> {
        let reservation = cache.reserve(span.end - span.start, version)?;
        let stage = cache
            .stage(version)
            .inspect_err(|error| {
                record_io_failure(error.downcast_ref(), DownloadCacheFailure::Write);
                cache_warning("stage_interval", error);
            })
            .ok()??;
        Self::open(reservation, stage, span)
            .inspect_err(|error| {
                record_io_failure(error.downcast_ref(), DownloadCacheFailure::Write);
                cache_warning("open_staging_file", error);
            })
            .ok()
    }

    fn open(
        reservation: Reservation,
        stage: tempfile::NamedTempFile,
        span: Range<u64>,
    ) -> Result<Self> {
        let file = AsyncFile::from_std(stage.reopen()?);
        Ok(Self {
            reservation,
            stage,
            file,
            span,
        })
    }

    async fn commit(mut self, version: Arc<CacheVersion>) {
        if let Err(error) = self.file.flush().await {
            record_io_failure(Some(&error), DownloadCacheFailure::Write);
            cache_warning("flush_interval", &error);
            return;
        }
        drop(self.file.into_std().await);
        let span = tracing::Span::current();
        let failure = DOWNLOAD_CACHE_FAILURE.try_with(Clone::clone).ok().flatten();
        let result = tokio::task::spawn_blocking(move || {
            DOWNLOAD_CACHE_FAILURE.sync_scope(failure, || {
                span.in_scope(|| self.reservation.commit(self.stage, self.span, version))
            })
        })
        .await;
        if let Err(error) = result
            .context("cache commit task")
            .and_then(|result| result)
        {
            record_io_failure(error.downcast_ref(), DownloadCacheFailure::Write);
            cache_warning("commit_interval", &format_args!("{error:#}"));
        }
    }
}

struct DirectoryLease {
    lock: File,
    #[cfg(not(unix))]
    marker: tempfile::NamedTempFile,
}

impl DirectoryLease {
    fn acquire(dir: &Path) -> Result<Self> {
        fs::create_dir_all(dir).context("create cache directory")?;
        let lock = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join(".lock"))
            .context("open cache lock")?;
        #[cfg(not(target_os = "android"))]
        lock.try_lock()
            .context("cannot acquire cache directory lock")?;
        #[cfg(target_os = "android")]
        {
            use std::os::fd::AsRawFd;
            // Rust's File::try_lock is unsupported on Android. The descriptor
            // retains this nonblocking flock until DirectoryLease is dropped.
            let status = unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
            if status != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("cannot acquire cache directory lock");
            }
        }
        let lease = Self {
            lock,
            #[cfg(not(unix))]
            marker: tempfile::Builder::new()
                .prefix(".rpc-lease-")
                .tempfile_in(dir)?,
        };
        ensure!(lease.is_current(dir), "cache lock path changed");
        Ok(lease)
    }

    fn is_current(&self, dir: &Path) -> bool {
        let Ok(current) = fs::metadata(dir.join(".lock")) else {
            return false;
        };
        let Ok(held) = self.lock.metadata() else {
            return false;
        };
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            (current.dev(), current.ino()) == (held.dev(), held.ino())
        }
        #[cfg(not(unix))]
        {
            self.marker.path().is_file() && current.created().ok() == held.created().ok()
        }
    }
}

fn cache_warning(operation: &'static str, error: &dyn std::fmt::Display) {
    static LAST: std::sync::LazyLock<Mutex<HashMap<&'static str, Instant>>> =
        std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
    let now = Instant::now();
    let mut last = LAST.lock().unwrap();
    if last
        .get(operation)
        .is_none_or(|previous| now.duration_since(*previous) >= Duration::from_secs(10))
    {
        last.insert(operation, now);
        tracing::warn!(operation, %error, "cache operation failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SOURCE: &str = "https://example.test/video.mp4";

    fn metadata(etag: Option<&str>) -> Metadata {
        Metadata {
            target: key(SOURCE),
            etag: etag.map(str::to_owned),
            size: 100,
            content_type: Some("video/mp4".into()),
        }
    }

    async fn store(cache: &Arc<Cache>, version: &Arc<CacheVersion>, span: Range<u64>) {
        let bytes: Vec<_> = span.clone().map(|offset| offset as u8).collect();
        let mut writer = cache.writer(version.clone(), span).unwrap();
        writer.write(&bytes).await;
        writer.finish().await;
    }

    const MASTER: &str = "https://example.test/master.m3u8";
    const FIRST: &str = "https://example.test/first.m3u8";
    const SECOND: &str = "https://example.test/second.m3u8";

    fn observe_variant(cache: &Cache, source: &str) {
        cache.observe_playlist(
            source,
            format!("#EXTM3U\n#EXTINF:1,\n{SOURCE}\n#EXT-X-ENDLIST\n"),
        );
    }

    #[tokio::test]
    async fn publication_crash_child() {
        let Some(root) = std::env::var_os("RPC_TEST_PUBLICATION_ROOT") else {
            return;
        };
        let cache = Cache::open(root.into(), 4096).unwrap();
        if std::env::var("RPC_TEST_PUBLICATION_TARGET").unwrap() == "master.m3u8" {
            observe_variant(&cache, SECOND);
        } else {
            let version = cache
                .adopt(SOURCE, metadata(Some("\"v1\"")), cache.refresh())
                .unwrap();
            store(&cache, &version, 50..100).await;
        }
    }

    async fn check_publication_recovery(target: &str) {
        for step in ["staged", "recorded", "installed", "indexed", "cleaned"] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path();
            let cache = Cache::open(root.to_owned(), 4096).unwrap();
            let version = cache
                .adopt(SOURCE, metadata(Some("\"v1\"")), cache.refresh())
                .unwrap();
            let playlist = target == "master.m3u8";
            store(&cache, &version, if playlist { 0..100 } else { 0..50 }).await;
            if playlist {
                observe_variant(&cache, FIRST);
                cache.observe_playlist(
                    MASTER,
                    format!("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100\n{FIRST}\n#EXT-X-STREAM-INF:BANDWIDTH=200\n{SECOND}\n"),
                );
                assert!(!cache.published_playlist(MASTER).unwrap().contains(SECOND));
            }
            drop(cache);
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "cache::tests::publication_crash_child"])
                .env("RPC_TEST_PUBLICATION_ROOT", root)
                .env("RPC_TEST_PUBLICATION_TARGET", target)
                .env("RPC_TEST_PUBLICATION_STEP", step)
                .stdout(std::process::Stdio::null())
                .spawn()
                .unwrap();
            let marker = root.join("publication-checkpoint");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !marker.is_file()
                && child.try_wait().unwrap().is_none()
                && Instant::now() < deadline
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let reached = marker.is_file();
            let _ = child.kill();
            child.wait().unwrap();
            assert!(reached, "child did not reach {target}/{step}");
            fs::remove_file(marker).unwrap();

            for _ in 0..2 {
                let cache = Cache::open(root.to_owned(), 4096).unwrap();
                let installed = matches!(step, "installed" | "indexed" | "cleaned");
                if playlist {
                    let master = cache.published_playlist(MASTER).unwrap();
                    assert!(master.contains(FIRST), "{step}");
                    assert_eq!(master.contains(SECOND), installed, "{step}");
                    assert!(cache.published_playlist(FIRST).is_some());
                    assert!(cache.published_playlist(SECOND).is_some());
                    assert_eq!(
                        fs::read(root.join("example.test/video.mp4")).unwrap(),
                        (0..100).collect::<Vec<u8>>()
                    );
                } else {
                    let path = root.join("example.test/video.mp4");
                    assert_eq!(path.exists(), installed, "{step}");
                    if installed {
                        assert_eq!(fs::read(path).unwrap(), (0..100).collect::<Vec<u8>>());
                    }
                    assert_eq!(cache.index.lock().unwrap().size, 100, "{step}");
                }
                let internal = root.join(INTERNAL);
                let entries = fs::read_dir(internal)
                    .unwrap()
                    .collect::<std::io::Result<Vec<_>>>()
                    .unwrap();
                assert!(
                    entries.iter().all(|entry| !entry
                        .file_name()
                        .to_string_lossy()
                        .starts_with(".rpc-stage-")),
                    "{step}"
                );
                let index = cache.index.lock().unwrap();
                assert_eq!(
                    entries.len(),
                    index
                        .entries
                        .values()
                        .flatten()
                        .map(|entry| {
                            if entry.record.published.is_some() {
                                1
                            } else {
                                2
                            }
                        })
                        .sum::<usize>(),
                    "{step}"
                );
                if playlist {
                    let media_bytes = fs::read_dir(root.join("example.test"))
                        .unwrap()
                        .map(|file| file.unwrap().metadata().unwrap().len())
                        .sum::<u64>();
                    assert_eq!(index.size, media_bytes, "{step}");
                }
            }
        }
    }

    #[tokio::test]
    async fn recovers_complete_mp4_after_forced_publication_stop() {
        check_publication_recovery("video.mp4").await;
    }

    #[tokio::test]
    async fn recovers_master_and_shared_media_after_forced_publication_stop() {
        check_publication_recovery("master.m3u8").await;
    }

    #[tokio::test]
    async fn concurrent_publications_respect_shared_capacity_and_held_readers() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 150).unwrap();
        let other_source = "https://example.test/other.mp4";
        let mut metadata = metadata(Some("\"v1\""));
        metadata.size = 50;
        let first = cache
            .adopt(SOURCE, metadata.clone(), cache.refresh())
            .unwrap();
        let second = cache
            .adopt(other_source, metadata.clone(), cache.refresh())
            .unwrap();
        cache.pin(key(SOURCE));
        cache.pin(key(other_source));
        store(&cache, &first, 0..25).await;
        store(&cache, &second, 0..25).await;
        // Readers can hold interval files after a full file has replaced them.
        let first_readers = cache.plan(&key(SOURCE), 0..25, &metadata);
        let second_readers = cache.plan(&key(other_source), 0..25, &metadata);
        tokio::join!(
            store(&cache, &first, 25..50),
            store(&cache, &second, 25..50)
        );

        let first_published = cache.contains(SOURCE, None);
        let second_published = cache.contains(other_source, None);
        assert_ne!(
            first_published, second_published,
            "only one full publication fits while intervals are held"
        );
        assert_eq!(cache.index.lock().unwrap().reserved, 0);
        assert!(cache.index.lock().unwrap().size <= cache.capacity());
        drop((first_readers, second_readers));
        let missing = if first_published {
            other_source
        } else {
            SOURCE
        };
        cache.retry_media_publication(missing).await.unwrap();
        assert!(cache.contains(SOURCE, None));
        assert!(cache.contains(other_source, None));
        assert_eq!(cache.index.lock().unwrap().reserved, 0);
    }

    #[test]
    fn disk_full_takes_precedence_over_cache_quota() {
        let failure = Arc::new(Mutex::new(None));
        DOWNLOAD_CACHE_FAILURE.sync_scope(Some(failure.clone()), || {
            record_download_failure(DownloadCacheFailure::Capacity);
            let error = std::io::Error::from(std::io::ErrorKind::StorageFull);
            record_io_failure(Some(&error), DownloadCacheFailure::Write);
        });
        assert!(matches!(
            *failure.lock().unwrap(),
            Some(DownloadCacheFailure::DiskFull)
        ));
    }

    #[tokio::test]
    async fn plans_only_missing_bytes_around_matching_intervals() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 1_024).unwrap();
        let metadata = metadata(Some("\"v1\""));
        let version = cache
            .adopt(SOURCE, metadata.clone(), cache.refresh())
            .unwrap();
        store(&cache, &version, 10..25).await;
        store(&cache, &version, 35..50).await;

        let plan: Vec<_> = cache
            .plan(&key(SOURCE), 15..55, &metadata)
            .into_iter()
            .map(|piece| match piece {
                Piece::Cached(_, span) => (true, span),
                Piece::Missing(span) => (false, span),
            })
            .collect();
        assert_eq!(
            plan,
            [
                (true, 15..25),
                (false, 25..35),
                (true, 35..50),
                (false, 50..55)
            ]
        );
    }

    #[tokio::test]
    async fn does_not_combine_intervals_without_a_strong_validator() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 1_024).unwrap();
        let metadata = metadata(None);
        let version = cache
            .adopt(SOURCE, metadata.clone(), cache.refresh())
            .unwrap();
        store(&cache, &version, 0..10).await;
        store(&cache, &version, 10..20).await;

        assert!(
            matches!(cache.plan(&key(SOURCE), 5..15, &metadata).as_slice(),
                         [Piece::Missing(span)] if *span == (5..15))
        );
        assert!(
            matches!(cache.plan(&key(SOURCE), 5..10, &metadata).as_slice(),
                         [Piece::Cached(_, span)] if *span == (5..10))
        );
    }

    #[tokio::test]
    async fn publishes_next_to_a_directory_with_the_preferred_file_name() {
        let directory = tempfile::tempdir().unwrap();
        let host = directory.path().join("example.test");
        let preferred = host.join("video.mp4");
        fs::create_dir_all(&preferred).unwrap();

        let cache = Cache::open(directory.path().to_owned(), 1_024).unwrap();
        let version = cache
            .adopt(SOURCE, metadata(Some("\"v1\"")), cache.refresh())
            .unwrap();
        store(&cache, &version, 0..100).await;

        assert!(preferred.is_dir());
        let files: Vec<_> = fs::read_dir(host)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.is_file())
            .collect();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].extension().unwrap(), "mp4");
        assert_eq!(fs::read(&files[0]).unwrap(), (0..100).collect::<Vec<u8>>());
    }

    #[tokio::test]
    async fn adopting_a_replacement_rejects_old_pending_writes() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 1_024).unwrap();
        let old = metadata(Some("\"v1\""));
        let new = metadata(Some("\"v2\""));
        let version = cache.adopt(SOURCE, old.clone(), cache.refresh()).unwrap();
        store(&cache, &version, 0..10).await;
        let mut pending = cache.writer(version.clone(), 10..20).unwrap();
        pending.write(&[1; 10]).await;

        let replacement = cache.adopt(SOURCE, new.clone(), cache.refresh()).unwrap();
        pending.finish().await;
        assert!(!cache.is_current(&version));
        assert!(matches!(cache.plan(&key(SOURCE), 0..20, &old).as_slice(),
                         [Piece::Missing(span)] if *span == (0..20)));
        store(&cache, &replacement, 0..20).await;
        assert!(matches!(cache.plan(&key(SOURCE), 0..20, &new).as_slice(),
                         [Piece::Cached(_, span)] if *span == (0..20)));
    }

    fn store_sync(cache: &Arc<Cache>, version: &Arc<CacheVersion>, span: Range<u64>) {
        let bytes: Vec<u8> = span.clone().map(|offset| offset as u8).collect();
        let reservation = cache.reserve(span.end - span.start, version).unwrap();
        let stage = cache.stage_file().unwrap();
        stage.as_file().write_all(&bytes).unwrap();
        reservation.commit(stage, span, version.clone()).unwrap();
    }

    #[tokio::test]
    async fn reserving_reclaims_quota_held_by_a_retired_whole_writer() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 100).unwrap();
        let generation = cache.refresh();
        let mut old = metadata(Some("\"v1\""));
        old.size = 100;
        let old_version = cache.adopt(SOURCE, old.clone(), generation).unwrap();
        let mut whole = cache
            .whole_writer(SOURCE.to_owned(), old.clone(), Some(100), generation)
            .unwrap();
        whole.write(&[1; 100]).await.unwrap();
        assert_eq!(cache.index.lock().unwrap().reserved, 100);

        // A newer representation retires the old writer, which stays paused and
        // keeps holding its staged quota. A new reserve must reclaim it.
        let new = metadata(Some("\"v2\""));
        let new_version = cache.adopt(SOURCE, new.clone(), generation).unwrap();
        let reservation = cache.reserve(100, &new_version);
        assert!(
            reservation.is_some(),
            "stale writer must not starve the new version"
        );
        assert_eq!(cache.index.lock().unwrap().reserved, 100);

        // Dropping the retired writer must not release the quota twice.
        drop(whole);
        assert_eq!(cache.index.lock().unwrap().reserved, 100);
        drop(reservation);
        assert_eq!(cache.index.lock().unwrap().reserved, 0);
        let _ = old_version;
    }

    #[tokio::test]
    async fn unknown_length_finish_does_not_retire_a_newer_published_version() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 4096).unwrap();
        let generation = cache.refresh();
        let mut old = metadata(Some("\"v1\""));
        old.size = 0; // origin omitted Content-Length
        let old_version = cache.adopt(SOURCE, old.clone(), generation).unwrap();
        let mut whole = cache
            .whole_writer(SOURCE.to_owned(), old.clone(), None, generation)
            .unwrap();
        whole.write(&[7; 100]).await.unwrap();

        // Inject a newer representation that is adopted and published at the
        // exact point where the old finish corrects its recorded size.
        let racer = cache.clone();
        let new = metadata(Some("\"v2\""));
        *cache.whole_finish_race.lock().unwrap() = Some(Box::new(move || {
            let version = racer.adopt(SOURCE, new.clone(), generation).unwrap();
            store_sync(&racer, &version, 0..100);
        }));
        whole.finish().await;

        assert!(cache.contains(SOURCE, None));
        assert_eq!(
            cache.metadata(&key(SOURCE)).unwrap().etag.as_deref(),
            Some("\"v2\"")
        );
        let _ = old_version;
        let published = directory.path().join("example.test/video.mp4");
        assert_eq!(fs::read(published).unwrap(), (0..100).collect::<Vec<u8>>());
    }

    #[tokio::test]
    async fn unknown_length_publish_releases_its_staged_quota() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 100).unwrap();
        let generation = cache.refresh();
        let mut unknown = metadata(Some("\"v1\""));
        unknown.size = 0; // origin omitted Content-Length
        let mut whole = cache
            .whole_writer(SOURCE.to_owned(), unknown.clone(), None, generation)
            .unwrap();
        whole.write(&[7; 100]).await.unwrap();
        whole.finish().await;

        assert!(cache.contains(SOURCE, None));
        let index = cache.index.lock().unwrap();
        assert_eq!(
            index.reserved, 0,
            "publishing must release the staged quota without waiting for the next reserve"
        );
        assert!(index.reservations.is_empty());
    }

    #[tokio::test]
    async fn reserving_between_size_correction_and_commit_keeps_staged_quota() {
        let directory = tempfile::tempdir().unwrap();
        let cache = Cache::open(directory.path().to_owned(), 100).unwrap();
        let generation = cache.refresh();
        let mut unknown = metadata(Some("\"v1\""));
        unknown.size = 0; // origin omitted Content-Length
        let mut whole = cache
            .whole_writer(SOURCE.to_owned(), unknown.clone(), None, generation)
            .unwrap();
        whole.write(&[7; 100]).await.unwrap();

        // A second resource tries to reserve the full capacity in the window
        // between the size correction (which adopts a new version id) and the
        // commit. The staged bytes are still in use and must not be reclaimed
        // as stale, or the cache would exceed its configured capacity.
        let other_source = "https://example.test/other.mp4";
        let mut other_metadata = metadata(Some("\"o1\""));
        other_metadata.target = key(other_source);
        let other_version = cache
            .adopt(other_source, other_metadata, generation)
            .unwrap();
        let racer = cache.clone();
        let outcome = Arc::new(Mutex::new(None));
        let raced = outcome.clone();
        *cache.whole_commit_race.lock().unwrap() = Some(Box::new(move || {
            *raced.lock().unwrap() = Some(racer.reserve(100, &other_version).is_some());
        }));
        whole.finish().await;

        assert_eq!(
            *outcome.lock().unwrap(),
            Some(false),
            "in-flight staged quota must not be reclaimed as stale"
        );
        assert!(cache.contains(SOURCE, None));
        let index = cache.index.lock().unwrap();
        assert_eq!(index.reserved, 0);
        assert!(index.size <= cache.capacity());
    }

    #[tokio::test]
    async fn whole_finish_hooks_are_isolated_per_cache_instance() {
        let dir_a = tempfile::tempdir().unwrap();
        let a = Cache::open(dir_a.path().to_owned(), 100).unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let b = Cache::open(dir_b.path().to_owned(), 100).unwrap();
        let gen_a = a.refresh();
        let gen_b = b.refresh();
        let mut unknown = metadata(Some("\"v1\""));
        unknown.size = 0; // origin omitted Content-Length

        // Arm both hooks for b's writer only.
        let finish_fired = Arc::new(Mutex::new(false));
        let commit_fired = Arc::new(Mutex::new(false));
        let flag = finish_fired.clone();
        *b.whole_finish_race.lock().unwrap() = Some(Box::new(move || {
            *flag.lock().unwrap() = true;
        }));
        let flag = commit_fired.clone();
        *b.whole_commit_race.lock().unwrap() = Some(Box::new(move || {
            *flag.lock().unwrap() = true;
        }));

        // An unrelated cache instance finishing its own unknown-length whole
        // file must not consume b's hooks.
        let mut whole_a = a
            .whole_writer(SOURCE.to_owned(), unknown.clone(), None, gen_a)
            .unwrap();
        whole_a.write(&[1; 10]).await.unwrap();
        whole_a.finish().await;
        assert!(
            b.whole_finish_race.lock().unwrap().is_some(),
            "a finished on another instance must not steal the finish hook"
        );
        assert!(
            b.whole_commit_race.lock().unwrap().is_some(),
            "a finished on another instance must not steal the commit hook"
        );
        assert!(!*finish_fired.lock().unwrap());
        assert!(!*commit_fired.lock().unwrap());

        // b's own finish consumes exactly its hooks.
        let mut whole_b = b
            .whole_writer(SOURCE.to_owned(), unknown.clone(), None, gen_b)
            .unwrap();
        whole_b.write(&[2; 10]).await.unwrap();
        whole_b.finish().await;
        assert!(*finish_fired.lock().unwrap());
        assert!(*commit_fired.lock().unwrap());
    }
}
