use {
    crate::{
        cas::{read_chunked, Digest},
        crypt::EncryptionKey,
        errors::DenebResult,
        inode::ChunkDescriptor,
    },
    std::{io::Read, path::Path, sync::Arc},
};

pub(crate) use self::chunk::{Chunk, DiskChunk, MemChunk};

mod chunk;
mod disk;
mod mem;

#[derive(Clone, Copy)]
pub enum StoreType {
    InMemory,
    OnDisk,
}

pub fn open_store<P: AsRef<Path>>(
    store_type: StoreType,
    dir: P,
    encryption_key: Option<EncryptionKey>,
    chunk_size: usize,
) -> DenebResult<Box<dyn Store>> {
    Ok(match store_type {
        StoreType::InMemory => Box::new(mem::MemStore::new(encryption_key, chunk_size)),
        StoreType::OnDisk => Box::new(disk::DiskStore::try_new(
            dir.as_ref(),
            encryption_key,
            chunk_size,
        )?),
    })
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
    fn chunk(&self, digest: &Digest) -> DenebResult<Arc<dyn Chunk>>;

    /// Write a single chunk into the repository
    ///
    fn put_chunk(&mut self, contents: &[u8]) -> DenebResult<ChunkDescriptor>;

    /// Write a file into the repository without chunking
    ///
    fn put_file(&mut self, data: &mut dyn Read) -> DenebResult<ChunkDescriptor> {
        let mut buf = vec![];
        data.read_to_end(&mut buf)?;
        Ok(self.put_chunk(buf.as_slice())?)
    }

    /// Write a file into the repository with chunking
    ///
    fn put_file_chunked(&mut self, data: &mut dyn Read) -> DenebResult<Vec<ChunkDescriptor>> {
        let mut descriptors = vec![];
        let mut buf = vec![0 as u8; self.chunk_size()];
        read_chunked(data, buf.as_mut_slice(), |s| {
            descriptors.push(self.put_chunk(s)?);
            Ok(())
        })?;
        Ok(descriptors)
    }

    /// Read a special file from outside of the content-addressed area of the store
    fn read_special_file(&self, file_name: &Path) -> DenebResult<Vec<u8>>;

    /// Write a special file outside of the content-addresed area of the store
    ///
    /// Special files are the repository manifest, reflog etc.
    fn write_special_file(
        &mut self,
        file_name: &Path,
        data: &mut dyn Read,
        append: bool,
    ) -> DenebResult<()>;
}
