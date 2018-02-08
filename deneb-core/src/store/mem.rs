use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use cas::Digest;
use errors::{DenebResult, StoreError};

use super::{Store, StoreBuilder};

pub struct MemStoreBuilder;

impl StoreBuilder for MemStoreBuilder {
    type Store = MemStore;

    fn at_dir<P: AsRef<Path>>(&self, _dir: P, chunk_size: usize) -> DenebResult<Self::Store> {
        Ok(Self::Store::new(chunk_size))
    }
}

#[derive(Default)]
pub struct MemStore {
    chunk_size: usize,
    objects: HashMap<Digest, Arc<Vec<u8>>>,
}

impl MemStore {
    pub fn new(chunk_size: usize) -> MemStore {
        MemStore {
            chunk_size,
            objects: HashMap::new(),
        }
    }

    pub fn show_stats(&self) {
        info!("MemStore: number of objects: {}", self.objects.len());
    }
}

impl Store for MemStore {
    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn get_chunk(&self, digest: &Digest) -> DenebResult<Arc<Vec<u8>>> {
        self.objects
            .get(digest)
            .map(Arc::clone)
            .ok_or_else(|| StoreError::ChunkGet(digest.to_string()).into())
    }

    // Note: can this be improved by inserting chunks as the become available from
    //       read_chunks?
    fn put_chunk(&mut self, digest: &Digest, contents: Vec<u8>) -> DenebResult<()> {
        self.objects.entry(*digest).or_insert(Arc::new(contents));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use util::run;

    #[test]
    fn memstore_create_put_get() {
        run(|| {
            let mut store: MemStore = MemStore::new(10000);
            let v1: Vec<u8> = vec![1, 2, 3];
            let descriptors = store.put_file_chunked(v1.as_slice())?;
            let v2 = store.get_chunk(&descriptors[0].digest)?;
            assert_eq!(v1, *v2);
            Ok(())
        })
    }
}
