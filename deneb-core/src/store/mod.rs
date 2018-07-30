use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use cas::{hash, read_chunks, Digest};
use errors::DenebResult;
use inode::ChunkDescriptor;

mod chunk;
pub(crate) use self::chunk::{Chunk, MemChunk, MmapChunk};

mod disk;
mod mem;

#[derive(Clone, Copy)]
pub enum StoreType {
    InMemory,
    OnDisk,
}

pub struct Builder;

impl Builder {
    pub fn build<P: AsRef<Path>>(
        store_type: StoreType,
        dir: P,
        chunk_size: usize,
    ) -> DenebResult<Box<dyn Store>> {
        match store_type {
            StoreType::InMemory => Ok(Box::new(mem::MemStore::new(chunk_size))),
            StoreType::OnDisk => Ok(Box::new(disk::DiskStore::new(dir.as_ref(), chunk_size)?)),
        }
    }
}

/// Types which can perform IO into repository storage
///
pub trait Store: Send {
    /// Returns the chunk size used by the store
    fn chunk_size(&self) -> usize;

    /// Returns a buffer with the contents of the requested chunk
    ///
    /// The method returns the "unpacked" chunks wrapped in an `Arc`,
    /// allowing implementations to cache the results.
    fn get_chunk(&self, digest: &Digest) -> DenebResult<Arc<dyn Chunk>>;

    /// Write a single chunk into the repository
    ///
    fn put_chunk(&mut self, digest: &Digest, contents: Vec<u8>) -> DenebResult<()>;

    /// Write a file into the repository without chunking
    ///
    fn put_file(&mut self, data: &mut dyn Read) -> DenebResult<ChunkDescriptor> {
        let mut buf = vec![];
        let n = data.read_to_end(&mut buf)?;
        let digest = hash(buf.as_slice());
        let descriptor = ChunkDescriptor { digest, size: n };
        self.put_chunk(&digest, buf)?;
        Ok(descriptor)
    }

    /// Write a file into the repository with chunking
    ///
    fn put_file_chunked(&mut self, data: &mut dyn Read) -> DenebResult<Vec<ChunkDescriptor>> {
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
