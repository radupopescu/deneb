use memmap::Mmap;

use std::{fs::File, path::PathBuf};

use {cas::Digest, errors::DenebResult};

/// An trait for accessing the contents of chunks stored in a repository
///
pub(crate) trait Chunk {
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
}

impl MmapChunk
{
    pub(crate) fn new(digest: Digest, size: usize, disk_path: PathBuf) -> DenebResult<MmapChunk> {
        let f = File::open(&disk_path)?;
        let mm = unsafe { Mmap::map(&f) }?;

        Ok(MmapChunk {
            digest,
            size,
            disk_path,
            map: mm,
        })
    }
}

impl Drop for MmapChunk {
    fn drop(&mut self) {
        if ::std::fs::remove_file(&self.disk_path).is_ok() {
            trace!("Removing chunk file: {:?}", &self.disk_path);
        } else {
            panic!("Could not remove chunk file {:?}", &self.disk_path);
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

#[cfg(test)]
mod tests {
    use tempdir::TempDir;
    use std::{fs::{OpenOptions, remove_file}, io::Write};

    use super::{Chunk, MmapChunk};

    use cas::hash;

    #[test]
    fn basic() {
        const msg: &[u8] = b"alabalaportocala";

        let tmp = TempDir::new("chunks");
        if let Ok(tmp) = tmp {
            let fname = tmp.path().join("c1");
            let mut f = OpenOptions::new().write(true).read(true).create(true).open(&fname);
            if let Ok(mut f) = f {
                f.write(msg);
                let cnk = MmapChunk::new(hash(msg), msg.len(), fname.clone());
                if let Ok(cnk) = cnk {
                    assert_eq!(msg, cnk.get_slice());
                }
            }
        }
    }
}