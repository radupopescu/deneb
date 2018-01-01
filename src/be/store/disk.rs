use nix::sys::stat::stat;

use std::char::from_digit;
use std::fs::{File, create_dir_all};
use std::io::Read;

use std::path::{Path, PathBuf};

use be::cas::Digest;
use common::util::file::atomic_write;
use common::errors::*;

use super::{Store, StoreBuilder};

const OBJECT_PATH: &str = "data";
const PREFIX_SIZE: usize = 2;

pub struct DiskStoreBuilder;

impl StoreBuilder for DiskStoreBuilder {
    type Store = DiskStore;

    fn at_dir<P: AsRef<Path>>(&self, dir: P) -> Result<Self::Store> {
        let root_dir = PathBuf::from(dir.as_ref());
        let object_dir = root_dir.join(OBJECT_PATH);

        // Create object dir and its subdirectories (00, 01, 4e, 3f etc.)
        for i in 0..16 {
            for j in 0..16 {
                if let (Some(i), Some(j)) = (from_digit(i, 16), from_digit(j, 16)) {
                    let mut prefix = i.to_string();
                    prefix.push(j);
                    create_dir_all(object_dir.join(prefix))?;
                }
            }
        }

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
    fn get_chunk(&self, digest: &Digest) -> Result<Vec<u8>> {
        let mut prefix = digest.to_string();
        let file_name = prefix.split_off(PREFIX_SIZE);
        let full_path = self.object_dir.join(prefix).join(file_name);
        let file_stats = stat(full_path.as_path())?;
        let mut buffer = Vec::new();
        let mut f = File::open(&full_path)?;
        let bytes_read = f.read_to_end(&mut buffer)?;
        if bytes_read as i64 == file_stats.st_size {
            debug!("File read: {:?}", full_path);
            Ok(buffer)
        } else {
            bail!("Could not retrive chunk from disk store.")
        }
    }

    fn put_chunk(&mut self, digest: Digest, contents: &[u8]) -> Result<()> {
        let hex_digest = digest.to_string();
        let mut prefix = hex_digest.clone();
        let file_name = prefix.split_off(PREFIX_SIZE);
        let full_path = self.object_dir.join(prefix).join(file_name);
        if let Ok(()) = atomic_write(full_path.as_path(), contents) {
            debug!("File written: {:?}", full_path);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;
    use be::cas::hash;

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
