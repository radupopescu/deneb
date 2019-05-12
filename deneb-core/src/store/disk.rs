mod pack;

use {
    self::pack::{pack_chunk, unpack_chunk},
    super::{Chunk, DiskChunk, Store},
    crate::{
        cas::Digest,
        crypt::EncryptionKey,
        errors::{DenebResult, StoreError},
        inode::ChunkDescriptor,
        util::atomic_write,
    },
    log::trace,
    lru::LruCache,
    nix::sys::stat::stat,
    std::{
        cell::RefCell,
        fs::{create_dir_all, File, OpenOptions},
        io::{Read, Write},
        path::{Path, PathBuf},
        sync::Arc,
    },
};

const OBJECT_PATH: &str = "data";
const SCRATCH_PATH: &str = "scratch";
const CACHE_MAX_OBJECTS: usize = 100;
const MIN_COMPRESSION_THRESHOLD: usize = 1024 * 1024;

/// A disk-based implementation of the `Store` trait.
///
/// Files are stored in subdirectories of `root_dir`/data, using a content-addressed
/// naming scheme: the first two letters of the hex representation of the file digest
/// is used as a subdirectory in which to store the file.
///
/// For example:
/// The full path at which a file with the digest "abcdefg123456" is stored is:
/// "`root_dir`/data/ab/cdefg123456"
pub(super) struct DiskStore {
    encryption_key: Option<EncryptionKey>,
    chunk_size: usize,
    root_dir: PathBuf,
    object_dir: PathBuf,
    scratch_dir: PathBuf,
    cache: RefCell<LruCache<Digest, Arc<dyn Chunk>>>,
}

impl DiskStore {
    pub(super) fn try_new(
        dir: &Path,
        encryption_key: Option<EncryptionKey>,
        chunk_size: usize,
    ) -> DenebResult<DiskStore> {
        let root_dir = dir;
        let object_dir = root_dir.join(OBJECT_PATH);
        let scratch_dir = root_dir.join(SCRATCH_PATH);

        // Create object dir
        create_dir_all(&object_dir)?;
        create_dir_all(&scratch_dir)?;

        Ok(DiskStore {
            encryption_key,
            chunk_size,
            root_dir: root_dir.to_owned(),
            object_dir,
            scratch_dir,
            cache: RefCell::new(LruCache::new(CACHE_MAX_OBJECTS)),
        })
    }
}

impl Store for DiskStore {
    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn chunk(&self, digest: &Digest) -> DenebResult<Arc<dyn Chunk>> {
        let mut cache = self.cache.borrow_mut();
        if cache.contains(digest) {
            cache
                .get(digest)
                .map(Arc::clone)
                .ok_or_else(|| StoreError::ChunkGet(digest.to_string()).into())
        } else {
            let full_path = unpack_chunk(
                digest,
                &self.object_dir,
                &self.scratch_dir,
                self.encryption_key.as_ref(),
            )?;
            let file_stats = stat(full_path.as_path())?;
            let chunk = DiskChunk::try_new(file_stats.st_size as usize, full_path)?;
            cache.put(*digest, Arc::new(chunk));
            cache
                .get(digest)
                .map(Arc::clone)
                .ok_or_else(|| StoreError::ChunkGet(digest.to_string()).into())
        }
    }

    fn put_chunk(&mut self, contents: &[u8]) -> DenebResult<ChunkDescriptor> {
        let compressed = contents.len() > MIN_COMPRESSION_THRESHOLD;
        let digest = pack_chunk(
            contents,
            &self.object_dir,
            compressed,
            self.encryption_key.as_ref(),
        )?;
        Ok(ChunkDescriptor {
            digest,
            size: contents.len(),
        })
    }

    fn read_special_file(&self, file_name: &Path) -> DenebResult<Vec<u8>> {
        let mut body = Vec::new();
        let full_path = self.root_dir.join(file_name);
        let mut f = File::open(full_path.to_owned())?;
        f.read_to_end(&mut body)?;
        trace!("Special file read: {:?}", full_path);
        Ok(body)
    }

    fn write_special_file(
        &mut self,
        file_name: &Path,
        data: &mut dyn Read,
        append: bool,
    ) -> DenebResult<()> {
        let mut body = Vec::new();
        data.read_to_end(&mut body)?;
        let full_path = self.root_dir.join(file_name);
        if append {
            let mut f = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(&full_path)?;
            f.write_all(&body)?;
        } else {
            atomic_write(full_path.as_path(), body.as_slice())?;
        }
        trace!("Special file written: {:?}", full_path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn diskstore_create_put_get() -> DenebResult<()> {
        const BYTES: &[u8] = b"alabalaportocala";
        let temp_dir = TempDir::new("/tmp/deneb_test_diskstore")?;
        let mut store = DiskStore::try_new(temp_dir.path(), None, 10000)?;
        let mut v1: &[u8] = BYTES;
        let descriptors = store.put_file_chunked(&mut v1)?;
        let v2 = store.chunk(&descriptors[0].digest)?;
        let mut buf = vec![0; v2.size()];
        v2.read_at(&mut buf, 0)?;
        assert_eq!(BYTES, buf.as_slice());
        Ok(())
    }
}
