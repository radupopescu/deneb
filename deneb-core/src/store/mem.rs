use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use cas::Digest;
use errors::{DenebResult, StoreError};

use super::{Chunk, MemChunk, Store, StoreBuilder};

pub struct MemStoreBuilder;

impl StoreBuilder for MemStoreBuilder {
    fn at_dir(&self, _dir: &Path, chunk_size: usize) -> DenebResult<Box<dyn Store>> {
        Ok(Box::new(MemStore::new(chunk_size)))
    }
}

#[derive(Default)]
pub struct MemStore {
    chunk_size: usize,
    objects: HashMap<Digest, Arc<dyn Chunk>>,
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

    fn get_chunk(&self, digest: &Digest) -> DenebResult<Arc<dyn Chunk>> {
        self.objects
            .get(digest)
            .map(Arc::clone)
            .ok_or_else(|| StoreError::ChunkGet(digest.to_string()).into())
    }

    // Note: can this be improved by inserting chunks as the become available from
    //       read_chunks?
    fn put_chunk(&mut self, digest: &Digest, contents: Vec<u8>) -> DenebResult<()> {
        self.objects
            .entry(*digest)
            .or_insert_with(|| Arc::new(MemChunk::new(*digest, contents)));
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
            const BYTES: &[u8] = b"alabalaportocala";
            let mut store: MemStore = MemStore::new(10000);
            let mut v1: &[u8] = BYTES;
            let descriptors = store.put_file_chunked(&mut v1)?;
            let v2 = store.get_chunk(&descriptors[0].digest)?;
            assert_eq!(BYTES, v2.get_slice());
            Ok(())
        })
    }
}
