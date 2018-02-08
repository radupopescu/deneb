use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use cas::{hash, read_chunks, Digest};
use inode::ChunkDescriptor;
use errors::DenebResult;

mod mem;
pub use self::mem::{MemStore, MemStoreBuilder};

mod disk;
pub use self::disk::{DiskStore, DiskStoreBuilder};

pub trait StoreBuilder {
    type Store: self::Store;

    fn at_dir<P: AsRef<Path>>(&self, dir: P, chunk_size: usize) -> DenebResult<Self::Store>;
}

pub trait Store {
    fn chunk_size(&self) -> usize;

    fn get_chunk(&self, digest: &Digest) -> DenebResult<Arc<Vec<u8>>>;

    fn put_chunk(&mut self, digest: &Digest, contents: Vec<u8>) -> DenebResult<()>;

    fn put_file<R: Read>(&mut self, mut data: R) -> DenebResult<Vec<ChunkDescriptor>> {
        let mut descriptors = vec![];
        let mut buf = vec![];
        let n = data.read_to_end(&mut buf)?;
        let digest = hash(buf.as_slice());
        descriptors.push(ChunkDescriptor { digest, size: n });
        self.put_chunk(&digest, buf)?;
        Ok(descriptors)
    }

    fn put_file_chunked<R: Read>(&mut self, data: R) -> DenebResult<Vec<ChunkDescriptor>> {
        let mut descriptors = vec![];
        let mut buf = vec![0 as u8; self.chunk_size()];
        for (digest, obj) in read_chunks(data, buf.as_mut_slice())? {
            descriptors.push(ChunkDescriptor {
                digest,
                size: obj.len(),
            });
            self.put_chunk(&digest, obj)?;
        }
        Ok(descriptors)
    }
}
