use std::path::Path;

use be::cas::Digest;
use common::errors::*;

mod mem;
pub use self::mem::{MemStore, MemStoreBuilder};

mod disk;
pub use self::disk::{DiskStore, DiskStoreBuilder};

pub trait StoreBuilder {
    type Store: self::Store;

    fn at_dir<P: AsRef<Path>>(&self, dir: P) -> Result<Self::Store>;
}

pub trait Store {
    fn get_chunk(&self, digest: &Digest) -> Result<Vec<u8>>;

    fn put_chunk(&mut self, digest: Digest, contents: &[u8]) -> Result<()>;
}
