use {
    crate::{cas::Digest, errors::DenebResult},
    log::trace,
    memmap::Mmap,
    std::{fs::File, path::PathBuf},
};

/// An trait for accessing the contents of chunks stored in a repository
///
pub trait Chunk: Send + Sync {
    /// Return the content of the chunk in a slice
    ///
    fn slice(&self) -> &[u8];

    fn size(&self) -> usize;
}

pub(crate) struct MmapChunk {
    digest: Digest,
    pub size: usize,
    disk_path: PathBuf,
    map: Mmap,
    own_file: bool,
}

impl MmapChunk {
    pub(crate) fn try_new(
        digest: Digest,
        size: usize,
        disk_path: PathBuf,
        own_file: bool,
    ) -> DenebResult<MmapChunk> {
        let f = File::open(&disk_path)?;
        let map = unsafe { Mmap::map(&f) }?;
        Ok(MmapChunk {
            digest,
            size,
            disk_path,
            map,
            own_file,
        })
    }
}

impl Drop for MmapChunk {
    fn drop(&mut self) {
        if self.own_file {
            if ::std::fs::remove_file(&self.disk_path).is_ok() {
                trace!("Removing chunk file: {:?}", &self.disk_path);
            } else {
                panic!("Could not remove chunk file {:?}", &self.disk_path);
            }
        }
    }
}

impl Chunk for MmapChunk {
    fn slice(&self) -> &[u8] {
        trace!(
            "Loaded contents of chunk {} -  size: {}",
            self.digest,
            self.size
        );
        self.map.as_ref()
    }

    fn size(&self) -> usize {
        self.size
    }
}

pub(crate) struct MemChunk {
    digest: Digest,
    data: Vec<u8>,
}

impl MemChunk {
    pub(crate) fn new(digest: Digest, data: Vec<u8>) -> MemChunk {
        MemChunk { digest, data }
    }
}

impl Chunk for MemChunk {
    fn slice(&self) -> &[u8] {
        trace!(
            "Loaded contents of chunk {} -  size: {}",
            self.digest,
            self.data.len(),
        );
        self.data.as_slice()
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions, io::Write};
    use tempdir::TempDir;

    use super::{Chunk, MemChunk, MmapChunk};

    use crate::cas::hash;

    #[test]
    fn mmap_chunk() {
        const MSG: &[u8] = b"alabalaportocala";

        let tmp = TempDir::new("chunks");
        if let Ok(tmp) = tmp {
            let fname = tmp.path().join("c1");
            let f = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(&fname);
            if let Ok(mut f) = f {
                let _ = f.write(MSG);
                let cnk = MmapChunk::try_new(hash(MSG), MSG.len(), fname.clone(), true);
                if let Ok(cnk) = cnk {
                    let cnk = Box::new(cnk);
                    assert_eq!(MSG, cnk.slice());
                }
            }
        }
    }

    #[test]
    fn mem_chunk() {
        const MSG: &[u8] = b"alabalaportocala";

        let cnk = MemChunk::new(hash(MSG), MSG.to_owned());
        assert_eq!(MSG, cnk.slice());
    }
}
