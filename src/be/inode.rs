use nix::libc::mode_t;
use nix::sys::stat::{S_IFMT, S_IFDIR, S_IFCHR, S_IFBLK, S_IFREG, S_IFLNK, S_IFIFO, lstat};
use time::Timespec;

use std::cmp::{min, max};
use std::fmt;
use std::i32::MAX;
use std::path::Path;

use common::errors::*;
use be::cas::Digest;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum FileType {
    NamedPipe,
    CharDevice,
    BlockDevice,
    Directory,
    RegularFile,
    Symlink,
}

#[derive(Copy, Clone, Debug)]
pub struct FileAttributes {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: Timespec,
    pub mtime: Timespec,
    pub ctime: Timespec,
    pub crtime: Timespec,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub flags: u32,
}

pub struct INode {
    pub attributes: FileAttributes,
    pub digests: Vec<Digest>,
}

impl INode {
    pub fn new(index: u64, path: &Path, digests: &[Digest]) -> Result<INode> {
        let stats = lstat(path)?;
        let mut attributes = FileAttributes {
            ino: index,
            size: max::<i64>(stats.st_size, 0) as u64,
            blocks: max::<i64>(stats.st_blocks, 0) as u64,
            atime: Timespec {
                sec: stats.st_atime,
                nsec: min::<i64>(stats.st_atime_nsec, MAX as i64) as i32,
            },
            mtime: Timespec {
                sec: stats.st_mtime,
                nsec: min::<i64>(stats.st_mtime_nsec, MAX as i64) as i32,
            },
            ctime: Timespec {
                sec: stats.st_ctime,
                nsec: min::<i64>(stats.st_ctime_nsec, MAX as i64) as i32,
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
        #[cfg(target_os="macos")]
        {
            attributes.crtime = Timespec {
                sec: stats.st_birthtime,
                nsec: min::<i64>(stats.st_birthtime_nsec, MAX as i64) as i32,
            };
        }

        Ok(INode {
               attributes: attributes,
               digests: digests.to_vec(),
           })
    }
}

impl fmt::Display for INode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Attributes: {:?}, digests: {:?}",
               self.attributes,
               self.digests)
    }
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
    mode & !S_IFMT.bits()
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
