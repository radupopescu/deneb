use {
    super::{Chunk, MemChunk, Store},
    crate::{
        cas::Digest,
        errors::{DenebResult, StoreError},
    },
    std::{
        collections::HashMap,
        io::Read,
        path::{Path, PathBuf},
        sync::Arc,
    },
};

#[derive(Default)]
pub(super) struct MemStore {
    chunk_size: usize,
    objects: HashMap<Digest, Arc<dyn Chunk>>,
    special: HashMap<PathBuf, Vec<u8>>,
}

impl MemStore {
    pub(super) fn new(chunk_size: usize) -> MemStore {
        MemStore {
            chunk_size,
            objects: HashMap::new(),
            special: HashMap::new(),
        }
    }
}

impl Store for MemStore {
    fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    fn chunk(&self, digest: &Digest) -> DenebResult<Arc<dyn Chunk>> {
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

    fn read_special_file(&self, file_name: &Path) -> DenebResult<Vec<u8>> {
        self.special
            .get(&file_name.to_owned())
            .cloned()
            .ok_or_else(|| StoreError::FileGet(file_name.to_owned()).into())
    }

    fn write_special_file(
        &mut self,
        file_name: &Path,
        data: &mut dyn Read,
        append: bool,
    ) -> DenebResult<()> {
        let name = file_name.to_owned();
        let mut body = Vec::new();
        data.read_to_end(&mut body)?;
        if append {
            if let Some(mut existing) = self.special.get(&name).cloned() {
                existing.append(&mut body);
                self.special.insert(name, body);
            }
        } else {
            self.special.insert(name, body);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memstore_create_put_get() -> DenebResult<()> {
        const BYTES: &[u8] = b"alabalaportocala";
        let mut store: MemStore = MemStore::new(10000);
        let mut v1: &[u8] = BYTES;
        let descriptors = store.put_file_chunked(&mut v1)?;
        let v2 = store.chunk(&descriptors[0].digest)?;
        assert_eq!(BYTES, v2.slice());
        Ok(())
    }
}
