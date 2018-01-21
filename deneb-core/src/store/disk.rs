use failure::ResultExt;
use nix::sys::stat::stat;

use std::fs::{create_dir_all, File};
use std::io::Read;

use std::path::{Path, PathBuf};

use cas::Digest;
use deneb_common::errors::{DenebError, DenebResult, StoreError};
use deneb_common::util::atomic_write;

use super::{Store, StoreBuilder};

const OBJECT_PATH: &str = "data";
const PREFIX_SIZE: usize = 2;
//const NUM_PREFIX: usize = 2;

pub struct DiskStoreBuilder;

impl StoreBuilder for DiskStoreBuilder {
    type Store = DiskStore;

    fn at_dir<P: AsRef<Path>>(&self, dir: P) -> DenebResult<Self::Store> {
        let root_dir = dir.as_ref().to_owned();
        let object_dir = root_dir.join(OBJECT_PATH);

        // Create object dir
        create_dir_all(&object_dir)?;

        Ok(Self::Store {
            _root_dir: root_dir,
            object_dir: object_dir,
        })
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
    _root_dir: PathBuf,
    object_dir: PathBuf,
}

impl Store for DiskStore {
    fn get_chunk(&self, digest: &Digest) -> DenebResult<Vec<u8>> {
        let mut prefix1 = digest.to_string();
        let mut prefix2 = prefix1.split_off(PREFIX_SIZE);
        let file_name = prefix2.split_off(PREFIX_SIZE);
        let full_path = self.object_dir.join(prefix1).join(prefix2).join(file_name);
        let file_stats = stat(full_path.as_path())?;
        let mut buffer = Vec::new();
        let mut f = File::open(&full_path).context(DenebError::DiskIO)?;
        let bytes_read = f.read_to_end(&mut buffer).context(DenebError::DiskIO)?;
        if bytes_read as i64 == file_stats.st_size {
            trace!("Chunk read: {:?}", full_path);
            Ok(buffer)
        } else {
            Err(StoreError::ChunkGet(digest.to_string()).into())
        }
    }

    fn put_chunk(&mut self, digest: Digest, contents: &[u8]) -> DenebResult<()> {
        let hex_digest = digest.to_string();
        let mut prefix1 = hex_digest.clone();
        let mut prefix2 = prefix1.split_off(PREFIX_SIZE);
        let file_name = prefix2.split_off(PREFIX_SIZE);
        let full_dir = self.object_dir.join(prefix1).join(prefix2);
        create_dir_all(&full_dir)?;
        let full_path = full_dir.join(file_name);
        atomic_write(full_path.as_path(), contents)?;
        trace!("Chunk written: {:?}", full_path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use cas::hash;

    #[test]
    fn diskstore_create_put_get() {
        let temp_dir = TempDir::new("/tmp/deneb_test_diskstore");
        assert!(temp_dir.is_ok());
        let sb = DiskStoreBuilder;
        if let Ok(temp_dir) = temp_dir {
            let store = sb.at_dir(temp_dir.path());
            assert!(store.is_ok());
            if let Ok(mut store) = store {
                let k1 = "some_key".as_ref();
                let v1: Vec<u8> = vec![0 as u8; 1000];
                let ret = store.put_chunk(hash(k1), v1.as_slice());
                assert!(ret.is_ok());
                if ret.is_ok() {
                    let v2 = store.get_chunk(&hash(k1));
                    assert!(v2.is_ok());
                    if let Ok(v2) = v2 {
                        assert_eq!(v1, v2);
                    }
                }
            }
        }
    }
}
