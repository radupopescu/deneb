use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use cas::{hash, read_chunks, Digest};
use inode::ChunkDescriptor;
use errors::DenebResult;

mod chunk;
pub(crate) use self::chunk::{Chunk, MemChunk, MmapChunk};

mod mem;
pub use self::mem::{MemStore, MemStoreBuilder};

mod disk;
pub use self::disk::{DiskStore, DiskStoreBuilder};

/// Builder types for `Store` objects
pub trait StoreBuilder {
    type Store: self::Store;

    /// Construct the new store at the specified directory
    ///
    /// It is assumed that the newly constructed store will keep any
    /// objects (chunks) already present at the specified directory
    fn at_dir<P: AsRef<Path>>(&self, dir: P, chunk_size: usize) -> DenebResult<Self::Store>;
}

/// Types which can perform IO into repository storage
///
pub trait Store {
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
    fn put_file<R: Read>(&mut self, mut data: R) -> DenebResult<ChunkDescriptor> {
        let mut buf = vec![];
        let n = data.read_to_end(&mut buf)?;
        let digest = hash(buf.as_slice());
        let descriptor = ChunkDescriptor { digest, size: n };
        self.put_chunk(&digest, buf)?;
        Ok(descriptor)
    }

    /// Write a file into the repository with chunking
    ///
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
