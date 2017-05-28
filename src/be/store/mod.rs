use be::cas::Digest;
use common::errors::*;

mod mem;
pub use self::mem::MemStore;

mod disk;
pub use self::disk::DiskStore;

pub mod util;

pub trait Store {
    fn get(&self, digest: &Digest) -> Result<Option<Vec<u8>>>;

    fn put(&mut self, digest: Digest, contents: &[u8]) -> Result<()>;
}

