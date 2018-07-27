use memmap::Mmap;

use std::{fs::File, path::PathBuf};

use {cas::Digest, errors::DenebResult};

/// An trait for accessing the contents of chunks stored in a repository
///
pub trait Chunk : Send + Sync {
    /// Return the content of the chunk in a slice
    ///
    fn get_slice(&self) -> &[u8];

    fn digest(&self) -> Digest;

    fn size(&self) -> usize;
}

pub(crate) struct MmapChunk {
    digest: Digest,
    pub size: usize,
    disk_path: PathBuf,
    map: Mmap,
    own_file: bool,
}

impl MmapChunk
{
    pub(crate) fn new(digest: Digest, size: usize, disk_path: PathBuf, own_file: bool) -> DenebResult<MmapChunk> {
        let f = File::open(&disk_path)?;
        let mm = unsafe { Mmap::map(&f) }?;

        Ok(MmapChunk {
            digest,
            size,
            disk_path,
            map: mm,
            own_file
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
    fn get_slice(&self) -> &[u8] {
        trace!(
            "Loaded contents of chunk {} -  size: {}",
            self.digest,
            self.size
        );
        self.map.as_ref()
    }

    fn digest(&self) -> Digest {
        self.digest
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
    fn get_slice(&self) -> &[u8] {
        self.data.as_slice()
    }

    fn digest(&self) -> Digest {
        self.digest
    }

    fn size(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use std::{fs::OpenOptions, io::Write};

    use super::{Chunk, MemChunk, MmapChunk};

    use cas::hash;

    #[test]
    fn mmap_chunk() {
        const MSG: &[u8] = b"alabalaportocala";

        let tmp = TempDir::new("chunks");
        if let Ok(tmp) = tmp {
            let fname = tmp.path().join("c1");
            let mut f = OpenOptions::new().write(true).read(true).create(true).open(&fname);
            if let Ok(mut f) = f {
                let _ = f.write(MSG);
                let cnk = MmapChunk::new(hash(MSG), MSG.len(), fname.clone(), true);
                if let Ok(cnk) = cnk {
                    let cnk = Box::new(cnk);
                    assert_eq!(MSG, cnk.get_slice());
                }
            }
        }
    }

    #[test]
    fn mem_chunk() {
        const MSG: &[u8] = b"alabalaportocala";

        let cnk = MemChunk::new(hash(MSG), MSG.to_owned());
        assert_eq!(MSG, cnk.get_slice());
    }
}