use {
    crate::errors::DenebResult,
    log::trace,
    std::{
        fs::{remove_file, File},
        os::unix::fs::FileExt,
        path::PathBuf,
    },
};

/// An trait for accessing the contents of chunks stored in a repository
///
pub trait Chunk: Send + Sync {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> DenebResult<usize>;

    fn size(&self) -> usize;
}

pub(crate) struct DiskChunk {
    size: usize,
    disk_path: PathBuf,
    file_handle: File,
}

impl DiskChunk {
    pub(crate) fn try_new(size: usize, disk_path: PathBuf) -> DenebResult<DiskChunk> {
        let file_handle = File::open(&disk_path)?;
        Ok(DiskChunk {
            size,
            disk_path,
            file_handle,
        })
    }
}

impl Drop for DiskChunk {
    fn drop(&mut self) {
        trace!("Removing chunk file: {:?}", &self.disk_path);
        let _ = remove_file(&self.disk_path);
    }
}

impl Chunk for DiskChunk {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> DenebResult<usize> {
        self.file_handle
            .read_at(buf, offset)
            .map_err(std::convert::Into::into)
    }

    fn size(&self) -> usize {
        self.size
    }
}

pub(crate) struct MemChunk {
    data: Vec<u8>,
}

impl MemChunk {
    pub(crate) fn new(data: Vec<u8>) -> MemChunk {
        MemChunk { data }
    }
}

impl Chunk for MemChunk {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> DenebResult<usize> {
        buf.copy_from_slice(&self.data.as_slice()[offset as usize..offset as usize + buf.len()]);
        Ok(buf.len())
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write};
    use tempdir::TempDir;

    use super::{Chunk, DiskChunk, MemChunk};

    use crate::errors::DenebResult;

    #[test]
    fn disk_chunk() -> DenebResult<()> {
        const MSG: &[u8] = b"alabalaportocala";

        let tmp = TempDir::new("chunks")?;
        let fname = tmp.path().join("c1");
        let mut f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&fname)?;
        f.write(MSG)?;
        let cnk = DiskChunk::try_new(MSG.len(), fname.clone())?;
        let cnk = Box::new(cnk);
        let mut buf = vec![0; cnk.size()];
        cnk.read_at(&mut buf, 0)?;
        assert_eq!(MSG, buf.as_slice());
        Ok(())
    }

    #[test]
    fn mem_chunk() -> DenebResult<()> {
        const MSG: &[u8] = b"alabalaportocala";

        let cnk = MemChunk::new(MSG.to_owned());
        let mut buf = vec![0; cnk.size()];
        cnk.read_at(&mut buf, 0)?;
        assert_eq!(MSG, buf.as_slice());
        Ok(())
    }
}
