use std::io::{Read, Seek, SeekFrom};

use super::*;
use crate::hls::Resource;

fn path_component(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() {
        if character.is_control()
            || matches!(character, '<' | '>' | ':' | '"' | '\\' | '|' | '?' | '*')
        {
            for byte in character.to_string().bytes() {
                escaped.push_str(&format!("%{byte:02X}"));
            }
        } else {
            escaped.push(character);
        }
    }
    if escaped.is_empty() || matches!(escaped.as_str(), "." | ".." | INTERNAL | ".lock") {
        return format!("_{}", &key(value)[..12]);
    }
    if escaped.len() > 220 {
        let extension = Path::new(&escaped).extension().and_then(|ext| ext.to_str());
        return match extension.filter(|ext| ext.len() <= 32) {
            Some(ext) => format!("{}.{ext}", &key(value)[..24]),
            None => key(value)[..24].to_owned(),
        };
    }
    escaped
}

fn media_path(root: &Path, version: &CacheVersion) -> Result<PathBuf> {
    let url = url::Url::parse(&version.source)?;
    let mut parts = url
        .path_segments()
        .context("source has no path")?
        .collect::<Vec<_>>();
    let last = parts
        .pop()
        .filter(|part| !part.is_empty())
        .unwrap_or("index");
    let mut directory = root.to_path_buf();
    for part in std::iter::once(url.host_str().context("source has no host")?).chain(parts) {
        let name = path_component(part);
        let child = directory.join(&name);
        if fs::symlink_metadata(&child).is_ok_and(|metadata| !metadata.is_dir()) {
            directory.push(format!("{name}~{}", &key(part)[..12]));
        } else {
            directory = child;
        }
        ensure_directory(&directory)?;
    }
    let mut name = path_component(last);
    if Path::new(&name).extension().is_none() {
        match version
            .metadata
            .content_type
            .as_deref()
            .map(|value| value.split(';').next().unwrap_or("").trim())
        {
            Some("video/mp4") => name.push_str(".mp4"),
            Some("video/mp2t") => name.push_str(".ts"),
            Some("audio/mp4") => name.push_str(".m4a"),
            Some("audio/aac") => name.push_str(".aac"),
            Some("text/vtt") => name.push_str(".vtt"),
            Some("application/vnd.apple.mpegurl") => name.push_str(".m3u8"),
            _ => {}
        }
    }
    let preferred = directory.join(&name);
    if fs::symlink_metadata(&preferred).is_ok() {
        let path = Path::new(&name);
        let stem = path
            .file_stem()
            .and_then(|part| part.to_str())
            .context("missing file name")?;
        let suffix = &version.key[..12];
        name = match path.extension().and_then(|part| part.to_str()) {
            Some(extension) => format!("{stem}~{suffix}.{extension}"),
            None => format!("{stem}~{suffix}"),
        };
    }
    Ok(directory.join(name))
}

pub(super) fn publish_complete(cache: &Arc<Cache>, version: &CacheVersion) -> Result<()> {
    let wanted = 0..version.metadata.size;
    let (covering, stage, reservation) = {
        let mut index = cache.index.lock().unwrap();
        if !cache.refresh_index(&mut index)
            || !index.is_current(version)
            || index.published(&version.key).is_some()
            || index.publishing.contains(&version.id)
        {
            return Ok(());
        }
        let entries = &index.entries[&version.key];
        if entries.iter().map(|entry| entry.size()).sum::<u64>() < wanted.end {
            return Ok(());
        }
        let Some(covering) = covering_entries(entries, &version.metadata, &wanted) else {
            return Ok(());
        };
        let stage = cache.stage_file()?;
        if index
            .reserve(cache.capacity, wanted.end, Some(version))
            .is_none()
        {
            record_download_failure(DownloadCacheFailure::Capacity);
            return Ok(());
        }
        index.publishing.insert(version.id);
        let reservation = Reservation {
            cache: cache.clone(),
            size: wanted.end,
            generation: index.generation,
            version: version.id,
        };
        (covering, stage, reservation)
    };
    let mut stage = stage;
    let copied =
        copy_ranges(&mut stage, &covering).and_then(|()| Ok(stage.as_file().sync_data()?));
    drop(covering);
    let mut index = cache.index.lock().unwrap();
    index.publishing.remove(&version.id);
    copied?;
    if !cache.refresh_index(&mut index)
        || !index.is_current(version)
        || index.published(&version.key).is_some()
    {
        return Ok(());
    }
    let result = publish_staged_media(&cache.dir, &mut index, version, stage);
    drop(index);
    drop(reservation);
    result
}

pub(super) fn publish_staged_media(
    root: &Path,
    index: &mut Index,
    version: &CacheVersion,
    stage: tempfile::NamedTempFile,
) -> Result<()> {
    let path = media_path(root, version)?;
    let record = Record {
        source: version.source.clone(),
        key: version.key.clone(),
        metadata: version.metadata.clone(),
        span: 0..version.metadata.size,
        published: Some(path.strip_prefix(root)?.to_path_buf()),
        modified: Some(stage.as_file().metadata()?.modified()?),
        playlist: None,
        playlist_digest: None,
    };
    persist_published(root, index, stage, record, path)
}

pub(super) fn covering_entries(
    entries: &[Arc<Entry>],
    metadata: &Metadata,
    wanted: &Range<u64>,
) -> Option<Vec<(Arc<Entry>, Range<u64>)>> {
    if metadata.etag.is_none() {
        return entries
            .iter()
            .find(|entry| {
                entry.record.metadata == *metadata
                    && entry.record.span.start <= wanted.start
                    && entry.record.span.end >= wanted.end
            })
            .map(|entry| vec![(entry.clone(), wanted.clone())]);
    }
    let mut candidates: Vec<_> = entries
        .iter()
        .filter(|entry| {
            entry.record.metadata == *metadata
                && entry.record.span.end > wanted.start
                && entry.record.span.start < wanted.end
        })
        .collect();
    candidates.sort_unstable_by_key(|entry| {
        (
            entry.record.span.start,
            std::cmp::Reverse(entry.record.span.end),
        )
    });
    let mut cursor = wanted.start;
    let mut covering = Vec::new();
    for entry in candidates {
        let span = &entry.record.span;
        if span.start > cursor {
            break;
        }
        if span.end > cursor {
            let end = span.end.min(wanted.end);
            covering.push((entry.clone(), cursor..end));
            cursor = end;
        }
    }
    (cursor == wanted.end).then_some(covering)
}

fn copy_ranges(
    stage: &mut tempfile::NamedTempFile,
    covering: &[(Arc<Entry>, Range<u64>)],
) -> Result<()> {
    for (entry, span) in covering {
        let (mut file, offset) = entry.open(span)?;
        file.seek(SeekFrom::Start(offset))?;
        let length = span.end - span.start;
        ensure!(
            std::io::copy(&mut file.take(length), stage)? == length,
            "cached range ended early"
        );
    }
    Ok(())
}

fn persist_published(
    root: &Path,
    index: &mut Index,
    stage: tempfile::NamedTempFile,
    record: Record,
    path: PathBuf,
) -> Result<()> {
    #[cfg(test)]
    publication_checkpoint(root, &path, "staged");
    let internal = root.join(INTERNAL);
    let mut sidecar = tempfile::Builder::new()
        .prefix(".rpc-stage-")
        .tempfile_in(&internal)?;
    serde_json::to_writer(&mut sidecar, &record)?;
    sidecar.as_file().sync_data()?;
    let id = sidecar
        .path()
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix(".rpc-stage-"))
        .context("invalid staging name")?;
    let sidecar_path = internal.join(format!("{id}.json"));
    sidecar.persist_noclobber(&sidecar_path)?;
    #[cfg(test)]
    publication_checkpoint(root, &path, "recorded");
    let replaces_playlist = record.playlist.is_some()
        && index.published(&record.key).is_some_and(|entry| {
            entry.path == path && entry.record.playlist.is_some() && entry.matches_published_file()
        });
    let persisted = if replaces_playlist {
        stage.persist(&path)
    } else {
        stage.persist_noclobber(&path)
    };
    if let Err(error) = persisted {
        remove(&sidecar_path);
        return Err(error.into());
    }
    #[cfg(test)]
    publication_checkpoint(root, &path, "installed");
    let replace_intervals = record.span == (0..record.metadata.size);
    let entries = index.entries.remove(&record.key).unwrap_or_default();
    let key = record.key.clone();
    index.insert(Entry {
        record,
        path: path.clone(),
        sidecar: sidecar_path,
        used: Mutex::new(SystemTime::now()),
        retired: AtomicBool::new(false),
    });
    #[cfg(test)]
    publication_checkpoint(root, &path, "indexed");
    for entry in entries {
        if replaces_playlist && entry.path == path {
            remove(&entry.sidecar);
            index.retired.push((Arc::downgrade(&entry), entry.size()));
            continue;
        }
        if entry.record.published.is_some() || !replace_intervals {
            index.entries.get_mut(&key).unwrap().push(entry);
            continue;
        }
        entry.retired.store(true, Ordering::Release);
        remove(&entry.sidecar);
        index.retired.push((Arc::downgrade(&entry), entry.size()));
    }
    index.entries.get_mut(&key).unwrap().rotate_left(1);
    #[cfg(test)]
    publication_checkpoint(root, &path, "cleaned");
    Ok(())
}

#[cfg(test)]
fn publication_checkpoint(root: &Path, path: &Path, step: &str) {
    let Ok(target) = std::env::var("RPC_TEST_PUBLICATION_TARGET") else {
        return;
    };
    if path.file_name().and_then(|name| name.to_str()) == Some(&target)
        && std::env::var("RPC_TEST_PUBLICATION_STEP").as_deref() == Ok(step)
    {
        fs::write(root.join("publication-checkpoint"), step).unwrap();
        loop {
            std::thread::park();
        }
    }
}

fn relative_uri(from: &Path, to: &Path) -> Result<String> {
    let from = from.parent().context("playlist has no parent")?;
    let common = from
        .components()
        .zip(to.components())
        .take_while(|(a, b)| a == b)
        .count();
    let mut uri = "../".repeat(from.components().count() - common);
    for (index, component) in to.components().skip(common).enumerate() {
        if index != 0 {
            uri.push('/');
        }
        let text = component
            .as_os_str()
            .to_str()
            .context("invalid media path")?;
        for byte in text.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
                uri.push(char::from(byte));
            } else {
                uri.push_str(&format!("%{byte:02X}"));
            }
        }
    }
    Ok(uri)
}

pub(super) fn publish_playlist(
    root: &Path,
    index: &mut Index,
    capacity: u64,
    source: &str,
) -> Result<bool> {
    let Some((_, canonical)) = index.playlists.get(source).cloned() else {
        return Ok(false);
    };
    let key = super::key(source);
    if hls::uses_encryption(&canonical) {
        return Ok(false);
    }
    let original = canonical.clone();
    let master = hls::is_master(&canonical);
    let canonical = if master {
        let Some(available) = hls::available_playlist(&canonical, |resource, source| {
            resource == Resource::Playlist
                && index.published(&super::key(source)).is_some_and(|entry| {
                    entry.record.playlist.is_some()
                        && index.valid_published_graph(entry, &mut HashSet::new())
                })
        })?
        else {
            return Ok(false);
        };
        available
    } else {
        let mut ready = true;
        let mut has_media = false;
        hls::media_references(&canonical, |source, span| {
            has_media = true;
            let key = super::key(source);
            ready &= index.published(&key).is_some_and(|entry| {
                entry.record.playlist.is_none()
                    && entry.matches_published_file()
                    && span
                        .as_ref()
                        .is_none_or(|span| span.end <= entry.record.span.end)
            }) || span.as_ref().is_some_and(|span| {
                index
                    .entries
                    .get(&key)
                    .and_then(|entries| {
                        covering_entries(entries, &entries.last()?.record.metadata, span)
                    })
                    .is_some()
            });
            Ok((source.to_owned(), false))
        })?;
        if !ready || !has_media {
            return Ok(false);
        }
        canonical
    };
    let existing = index.published(&key).cloned();
    if existing.as_ref().is_some_and(|entry| {
        entry
            .record
            .playlist
            .as_ref()
            .is_some_and(|playlist| playlist.canonical == canonical)
            && index.valid_published_graph(entry, &mut HashSet::new())
    }) {
        return Ok(false);
    }
    let mut metadata = Metadata {
        target: String::new(),
        size: 0,
        etag: None,
        content_type: Some("application/vnd.apple.mpegurl".to_owned()),
    };
    let path = if let Some(entry) = existing
        .as_ref()
        .filter(|entry| entry.record.playlist.is_some() && entry.matches_published_file())
    {
        entry.path.clone()
    } else {
        media_path(
            root,
            &CacheVersion {
                source: source.to_owned(),
                key: key.clone(),
                metadata: metadata.clone(),
                id: 0,
            },
        )?
    };
    let mut dependencies = Vec::new();
    let local = if master {
        hls::rewrite(&canonical, |_, source| {
            let entry = index
                .published(&super::key(source))
                .context("missing HLS dependency")?
                .clone();
            let uri = relative_uri(&path, &entry.path)?;
            dependencies.push(entry);
            Ok(uri)
        })?
    } else {
        hls::media_references(&canonical, |source, span| {
            let key = super::key(source);
            let (entry, sliced) = if let Some(entry) = index.published(&key) {
                (entry.clone(), false)
            } else {
                let span = span.context("missing complete media")?;
                (publish_slice(root, index, capacity, source, span)?, true)
            };
            let uri = relative_uri(&path, &entry.path)?;
            dependencies.push(entry);
            Ok((uri, sliced))
        })?
    };
    metadata.size = local.len() as u64;
    if index.reserve(capacity, metadata.size, None).is_none() {
        record_download_failure(DownloadCacheFailure::Capacity);
        return Ok(false);
    }
    // The index remains locked through the write and commit.
    index.reserved -= metadata.size;
    let mut stage = tempfile::Builder::new()
        .prefix(".rpc-stage-")
        .tempfile_in(root.join(INTERNAL))?;
    stage.write_all(local.as_bytes())?;
    stage.as_file().sync_data()?;
    let record = Record {
        source: source.to_owned(),
        key,
        span: 0..metadata.size,
        metadata,
        published: Some(path.strip_prefix(root)?.to_path_buf()),
        modified: Some(stage.as_file().metadata()?.modified()?),
        playlist_digest: Some(super::key(&local)),
        playlist: Some(PublishedPlaylist {
            canonical,
            original,
            dependencies: dependencies
                .iter()
                .map(|entry| PublishedDependency {
                    key: entry.record.key.clone(),
                    path: entry.record.published.clone().unwrap(),
                })
                .collect(),
        }),
    };
    persist_published(root, index, stage, record, path)?;
    Ok(true)
}

fn publish_slice(
    root: &Path,
    index: &mut Index,
    capacity: u64,
    source: &str,
    span: Range<u64>,
) -> Result<Arc<Entry>> {
    let key = key(source);
    let entries = index.entries.get(&key).context("missing range resource")?;
    let metadata = entries
        .last()
        .context("missing range metadata")?
        .record
        .metadata
        .clone();
    if let Some(entry) = entries.iter().find(|entry| {
        entry.record.span == span
            && entry.record.published.is_some()
            && entry.matches_published_file()
    }) {
        return Ok(entry.clone());
    }
    let covering = covering_entries(entries, &metadata, &span).context("missing media range")?;
    let base = media_path(
        root,
        &CacheVersion {
            source: source.to_owned(),
            key: key.clone(),
            metadata: metadata.clone(),
            id: 0,
        },
    )?;
    let stem = base
        .file_stem()
        .and_then(|part| part.to_str())
        .context("missing range file name")?;
    let compact_stem = super::key(stem);
    let stem = if stem.len() > 175 {
        &compact_stem[..24]
    } else {
        stem
    };
    let mut name = format!("{stem}~{}-{}-{}", &key[..12], span.start, span.end);
    if let Some(extension) = base.extension().and_then(|part| part.to_str()) {
        name.push('.');
        name.push_str(extension);
    }
    let path = base.with_file_name(name);
    let size = span.end - span.start;
    index
        .reserve(capacity, size, None)
        .context(PublicationCapacity)?;
    // The index remains locked through the write and commit.
    index.reserved -= size;
    let mut stage = tempfile::Builder::new()
        .prefix(".rpc-stage-")
        .tempfile_in(root.join(INTERNAL))?;
    copy_ranges(&mut stage, &covering)?;
    stage.as_file().sync_data()?;
    drop(covering);
    let replaced: Vec<_> = index.entries[&key]
        .iter()
        .filter(|entry| {
            Arc::strong_count(entry) == 1
                && entry.record.published.is_none()
                && entry.record.span.start >= span.start
                && entry.record.span.end <= span.end
        })
        .cloned()
        .collect();
    let replaced_size: u64 = replaced.iter().map(|entry| entry.size()).sum();
    let record = Record {
        source: source.to_owned(),
        key: key.clone(),
        span,
        metadata,
        published: Some(path.strip_prefix(root)?.to_path_buf()),
        modified: Some(stage.as_file().metadata()?.modified()?),
        playlist: None,
        playlist_digest: None,
    };
    persist_published(root, index, stage, record, path)?;
    index.entries.get_mut(&key).unwrap().retain(|entry| {
        let replaced = replaced.iter().any(|old| Arc::ptr_eq(entry, old));
        if replaced {
            delete(entry);
        }
        !replaced
    });
    index.size -= replaced_size;
    Ok(index.entries[&key].last().unwrap().clone())
}
