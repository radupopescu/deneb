use std::collections::HashMap;
use std::hash::Hash;

use errors::*;

pub trait Store<K> {
    fn get(&self, hash: &K) -> Option<&[u8]>;

    fn put(&mut self, hash: &K, contents: &[u8]) -> Result<()>;
}

pub struct HashMapStore<K> {
    objects: HashMap<K, Vec<u8>>,
}

impl<K> HashMapStore<K> where K: Hash + Eq + PartialEq {
    pub fn new() -> HashMapStore<K> {
        HashMapStore { objects: HashMap::new() }
    }
}

impl<K> Store<K> for HashMapStore<K> where K: Hash + Eq + PartialEq {
    fn get(&self, hash: &K) -> Option<&[u8]> {
        self.objects.get(hash).map(|v| v.as_slice())
    }

    fn put(&mut self, _hash: &K, _contents: &[u8]) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_put_get() {

    }
}
