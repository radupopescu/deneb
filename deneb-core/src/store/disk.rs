use failure::ResultExt;
use lru::LruCache;
use nix::sys::stat::stat;

use std::cell::RefCell;
use std::fs::{create_dir_all, File};
use std::io::Read;
use std::sync::Arc;

use std::path::{Path, PathBuf};

use cas::Digest;
use errors::{DenebError, DenebResult, StoreError};
use util::atomic_write;

use super::{Store, StoreBuilder};

const OBJECT_PATH: &str = "data";
const PREFIX_SIZE: usize = 2;
//const NUM_PREFIX: usize = 2;

const CACHE_MAX_OBJECTS: usize = 100;

pub struct DiskStoreBuilder;

impl StoreBuilder for DiskStoreBuilder {
    type Store = DiskStore;

    fn at_dir<P: AsRef<Path>>(&self, dir: P, chunk_size: usize) -> DenebResult<Self::Store> {
        let root_dir = dir.as_ref().to_owned();
        let object_dir = root_dir.join(OBJECT_PATH);

        // Create object dir
        create_dir_all(&object_dir)?;

        Ok(Self::Store { chunk_size, _root_dir: root_dir, object_dir,
                         cache: RefCell::new(LruCache::new(CACHE_MAX_OBJECTS)) })
    }
}

/// A disk-based implementation of the `Store` trait.
///
/// Files are stored in subdirectories of `root_dir`/data, using a content-addressed
/// naming scheme: the first two letters of the hex representation of the file digest
/// is used as a subdirectory in which to store the file.
///
/// For example:
/// The full path at which a file with the digest "abcdefg123456" is stored is:
/// "`root_dir`/data/ab/cdefg123456"
pub struct DiskStore {
    chunk_size: usize,
    _root_dir: PathBuf,
    object_dir: PathBuf,
    cache: RefCell<LruCache<Digest, Arc<Vec<u8>>>>,
}

impl DiskStore {
    /// Given a Digest, returns the absolute file path and the directory path
    /// corresponding to the object in the store
    fn digest_to_path(&self, digest: &Digest) -> (PathBuf, PathBuf) {
        let mut prefix1 = digest.to_string();
        let mut prefix2 = prefix1.split_off(PREFIX_SIZE);
        let file_name = prefix2.split_off(PREFIX_SIZE);
        let directory = self.object_dir.join(prefix1).join(prefix2);
        let file_path = directory.join(file_name);
        (file_path, directory)
    }
}

impl Store for DiskStore {
    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn get_chunk(&self, digest: &Digest) -> DenebResult<Arc<Vec<u8>>> {
        let mut cache = self.cache.borrow_mut();
        if cache.contains(digest) {
            cache
                .get(digest)
                .map(Arc::clone)
                .ok_or_else(|| StoreError::ChunkGet(digest.to_string()).into())
        } else {
            let (full_path, _) = self.digest_to_path(digest);
            let file_stats = stat(full_path.as_path())?;
            let mut buffer = Vec::new();
            let mut f = File::open(&full_path).context(DenebError::DiskIO)?;
            let bytes_read = f.read_to_end(&mut buffer).context(DenebError::DiskIO)?;
            if bytes_read as i64 == file_stats.st_size {
                trace!("Chunk read: {:?}", full_path);
                cache.put(*digest, Arc::new(buffer));
                cache
                    .get(digest)
                    .map(Arc::clone)
                    .ok_or_else(|| StoreError::ChunkGet(digest.to_string()).into())
            } else {
                Err(StoreError::ChunkGet(digest.to_string()).into())
            }
        }
    }

    fn put_chunk(&mut self, digest: &Digest, contents: Vec<u8>) -> DenebResult<()> {
        let (full_path, directory) = self.digest_to_path(&digest);
        create_dir_all(&directory)?;
        atomic_write(full_path.as_path(), contents.as_slice())?;
        trace!("Chunk written: {:?}", full_path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    use util::run;

    #[test]
    fn diskstore_create_put_get() {
        run(|| {
            let temp_dir = TempDir::new("/tmp/deneb_test_diskstore")?;
            let sb = DiskStoreBuilder;
            let mut store = sb.at_dir(temp_dir.path(), 10000)?;
            let v1: Vec<u8> = vec![0 as u8; 1000];
            let descriptors = store.put_file_chunked(v1.as_slice())?;
            let v2 = store.get_chunk(&descriptors[0].digest)?;
            assert_eq!(v1, *v2);
            Ok(())
        });
    }
}
