use std::collections::HashMap;
use std::path::Path;

use be::cas::Digest;
use common::errors::{StoreError, DenebResult};

use super::{Store, StoreBuilder};

pub struct MemStoreBuilder;

impl StoreBuilder for MemStoreBuilder {
    type Store = MemStore;

    fn at_dir<P: AsRef<Path>>(&self, _dir: P) -> DenebResult<Self::Store> {
        Ok(MemStore::new())
    }
}

#[derive(Default)]
pub struct MemStore {
    objects: HashMap<Digest, Vec<u8>>,
}

impl MemStore {
    pub fn new() -> MemStore {
        Self::default()
    }

    pub fn show_stats(&self) {
        info!("MemStore: number of objects: {}", self.objects.len());
    }
}

impl Store for MemStore {
    fn get_chunk(&self, digest: &Digest) -> DenebResult<Vec<u8>> {
        self.objects
            .get(digest)
            .cloned()
            .ok_or_else(|| StoreError::ChunkGet(digest.to_string()).into())
    }

    fn put_chunk(&mut self, digest: Digest, contents: &[u8]) -> DenebResult<()> {
        self.objects
            .entry(digest)
            .or_insert_with(|| contents.to_vec());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use be::cas::hash;

    #[test]
    fn memstore_create_put_get() {
        let mut store: MemStore = MemStore::new();
        let k1 = "some_key".as_ref();
        let v1: Vec<u8> = vec![1, 2, 3];
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
