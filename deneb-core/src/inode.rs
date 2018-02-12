use nix::libc::mode_t;
use nix::sys::stat::{lstat, S_IFBLK, S_IFCHR, S_IFDIR, S_IFIFO, S_IFLNK, S_IFMT, S_IFREG};
use time::Timespec;

use std::cmp::{max, min};
use std::i32;
use std::u16;
use std::path::Path;

use errors::UnixError;
use cas::Digest;

#[derive(Copy, Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum FileType {
    NamedPipe,
    CharDevice,
    BlockDevice,
    Directory,
    RegularFile,
    Symlink,
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
pub struct FileAttributes {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    #[serde(with = "TimespecDef")]
    pub atime: Timespec,
    #[serde(with = "TimespecDef")]
    pub mtime: Timespec,
    #[serde(with = "TimespecDef")]
    pub ctime: Timespec,
    #[serde(with = "TimespecDef")]
    pub crtime: Timespec,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub flags: u32,
}

impl Default for FileAttributes {
    fn default() -> FileAttributes {
        FileAttributes {
            ino: 0,
            size: 0,
            blocks: 0,
            atime: Timespec::new(0, 0),
            mtime: Timespec::new(0, 0),
            ctime: Timespec::new(0, 0),
            crtime: Timespec::new(0, 0),
            kind: FileType::RegularFile,
            perm: 0,
            nlink: 0,
            uid: 0,
            gid: 0,
            rdev: 0,
            flags: 0,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChunkDescriptor {
    pub digest: Digest,
    pub size: usize,
}

/// Data structure returned by the `lookup_chunks` function
///
/// The digest identifying a chunk and the indices which define an exclusive
/// range of that should be read from the chunk data.
#[derive(Debug, PartialEq)]
pub struct ChunkPart<'a>(pub &'a Digest, pub usize, pub usize);

#[derive(Clone, Deserialize, Serialize)]
pub struct INode {
    pub attributes: FileAttributes,
    pub chunks: Vec<ChunkDescriptor>,
}

impl INode {
    pub fn new(index: u64, path: &Path, chunks: Vec<ChunkDescriptor>) -> Result<INode, UnixError> {
        let stats = lstat(path)?;
        // Note: we prefix `attributes` with an underscore to avoid triggering an
        //       "unused_mut" warning on Linux.
        let mut _attributes = FileAttributes {
            ino: index,
            size: max::<i64>(stats.st_size, 0) as u64,
            blocks: max::<i64>(stats.st_blocks, 0) as u64,
            atime: Timespec {
                sec: stats.st_atime,
                nsec: min::<i64>(stats.st_atime_nsec, i64::from(i32::MAX)) as i32,
            },
            mtime: Timespec {
                sec: stats.st_mtime,
                nsec: min::<i64>(stats.st_mtime_nsec, i64::from(i32::MAX)) as i32,
            },
            ctime: Timespec {
                sec: stats.st_ctime,
                nsec: min::<i64>(stats.st_ctime_nsec, i64::from(i32::MAX)) as i32,
            },
            crtime: Timespec { sec: 0, nsec: 0 },
            kind: mode_to_file_type(stats.st_mode),
            perm: mode_to_permissions(stats.st_mode),
            nlink: 0,
            uid: stats.st_uid,
            gid: stats.st_gid,
            rdev: 0,
            flags: 0,
        };
        #[cfg(target_os = "macos")]
        {
            _attributes.crtime = Timespec {
                sec: stats.st_birthtime,
                nsec: min::<i64>(stats.st_birthtime_nsec, i64::from(i32::MAX)) as i32,
            };
        }

        Ok(INode {
            attributes: _attributes,
            chunks: chunks,
        })
    }
}

/// Lookup a subset of consecutive chunks corresponding to a memory slice
///
/// Given a list of `ChunkDescriptor`, representing consecutive chunks of a file and a segment identified by
/// `offset` - the offset from the beginning of the file - and `size` - the size of the segment,
/// this function returns a vector of `ChunkPart`
pub fn lookup_chunks(offset: usize, size: usize, chunks: &[ChunkDescriptor]) -> Vec<ChunkPart> {
    let (first_chunk, mut offset_in_chunk) = chunk_idx_for_offset(offset, chunks);
    let mut output = Vec::new();
    let mut bytes_left = size;
    for c in chunks[first_chunk..].iter() {
        let read_bytes = min(bytes_left, c.size - offset_in_chunk);
        output.push(ChunkPart(
            &c.digest,
            offset_in_chunk,
            offset_in_chunk + read_bytes,
        ));
        offset_in_chunk = 0;
        bytes_left -= read_bytes;
        if bytes_left == 0 {
            break;
        }
    }
    output
}

fn mode_to_file_type(mode: mode_t) -> FileType {
    let ft = mode & S_IFMT.bits();
    if ft == S_IFDIR.bits() {
        FileType::Directory
    } else if ft == S_IFCHR.bits() {
        FileType::CharDevice
    } else if ft == S_IFBLK.bits() {
        FileType::BlockDevice
    } else if ft == S_IFREG.bits() {
        FileType::RegularFile
    } else if ft == S_IFLNK.bits() {
        FileType::Symlink
    } else if ft == S_IFIFO.bits() {
        FileType::NamedPipe
    } else {
        // S_IFSOCK???
        panic!("Unknown file mode: {}. Could not identify file type.", mode);
    }
}

fn mode_to_permissions(mode: mode_t) -> u16 {
    #[cfg(target_os = "linux")]
    debug_assert!(mode <= u16::MAX as u32);
    (mode & !S_IFMT.bits()) as u16
}

/// Lookup the index in a list of chunks corresponding to an offset
///
/// Returns a pair of `usize` representing the index of the chunk inside the list (slice)
/// and the offset inside the chunk which correspond to the give offset
fn chunk_idx_for_offset(offset: usize, chunks: &[ChunkDescriptor]) -> (usize, usize) {
    let mut acc = 0;
    let mut idx = 0;
    let mut offset_in_chunk = 0;
    for (i, c) in chunks.iter().enumerate() {
        acc += c.size;
        idx = i;
        if acc > offset {
            offset_in_chunk = offset + c.size - acc;
            break;
        }
    }
    (idx, offset_in_chunk)
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "Timespec")]
struct TimespecDef {
    pub sec: i64,
    pub nsec: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_to_file_type_test() {
        let stats = lstat("/usr").unwrap();
        assert_eq!(mode_to_file_type(stats.st_mode), FileType::Directory);

        let stats = lstat("/etc/hosts").unwrap();
        assert_eq!(mode_to_file_type(stats.st_mode), FileType::RegularFile);
    }

    #[test]
    fn mode_to_permissions_test() {
        let stats = lstat("/etc/hosts").unwrap();
        assert_eq!(mode_to_permissions(stats.st_mode), 0o644);
    }

}
