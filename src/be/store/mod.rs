use be::cas::Digest;
use common::errors::*;

mod mem;
pub use self::mem::MemStore;

mod disk;
pub use self::disk::DiskStore;

pub trait Store {
    fn get_chunk(&self, digest: &Digest) -> Result<Vec<u8>>;

    fn put_chunk(&mut self, digest: Digest, contents: &[u8]) -> Result<()>;
}

