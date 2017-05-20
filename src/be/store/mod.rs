use be::cas::Digest;

pub trait Store {
    fn get(&self, hash: &Digest) -> Option<&[u8]>;

    fn put(&mut self, hash: Digest, contents: &[u8]);
}

mod mem;
pub use self::mem::MemStore;

