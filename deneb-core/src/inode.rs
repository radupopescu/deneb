use {
    crate::cas::Digest,
    nix::{
        libc::mode_t,
        sys::stat::{FileStat, SFlag},
    },
    serde::{Deserialize, Serialize},
    std::{
        cmp::{max, min},
        i32, u16,
    },
    time::Timespec,
};

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
    pub index: u64,
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

impl FileAttributes {
    pub fn with_stats(stats: FileStat, index: u64) -> FileAttributes {
        let mut attrs = FileAttributes::from(stats);
        attrs.index = index;
        attrs
    }

    pub fn update(&mut self, changes: &FileAttributeChanges) {
        if let Some(mode) = changes.mode {
            self.kind = mode_to_file_type(mode as mode_t);
            self.perm = mode_to_permissions(mode as mode_t);
        }
        if let Some(uid) = changes.uid {
            self.uid = uid;
        }
        if let Some(gid) = changes.gid {
            self.gid = gid;
        }
        if let Some(size) = changes.size {
            self.size = size;
        }
        if let Some(atime) = changes.atime {
            self.atime = atime;
        }
        if let Some(mtime) = changes.mtime {
            self.mtime = mtime;
        }
        if let Some(crtime) = changes.crtime {
            self.crtime = crtime;
        }
        if let Some(chgtime) = changes.chgtime {
            self.ctime = chgtime;
        }
    }
}

impl Default for FileAttributes {
    fn default() -> FileAttributes {
        FileAttributes {
            index: 0,
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

impl From<FileStat> for FileAttributes {
    fn from(stats: FileStat) -> FileAttributes {
        // Note: we prefix `attributes` with an underscore to avoid triggering an
        //       "unused_mut" warning on Linux.
        let mut _attributes = FileAttributes {
            index: stats.st_ino,
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
        _attributes
    }
}

pub struct FileAttributeChanges {
    mode: Option<u32>,
    uid: Option<u32>,
    gid: Option<u32>,
    pub size: Option<u64>,
    atime: Option<Timespec>,
    mtime: Option<Timespec>,
    crtime: Option<Timespec>,
    chgtime: Option<Timespec>,
    #[allow(dead_code)]
    flags: Option<u32>,
}

impl FileAttributeChanges {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<Timespec>,
        mtime: Option<Timespec>,
        crtime: Option<Timespec>,
        chgtime: Option<Timespec>,
        flags: Option<u32>,
    ) -> FileAttributeChanges {
        FileAttributeChanges {
            mode,
            uid,
            gid,
            size,
            atime,
            mtime,
            crtime,
            chgtime,
            flags,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChunkDescriptor {
    pub digest: Digest,
    pub size: usize,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct INode {
    pub attributes: FileAttributes,
    pub chunks: Vec<ChunkDescriptor>,
}

impl INode {
    pub fn new(attributes: FileAttributes, chunks: Vec<ChunkDescriptor>) -> INode {
        INode { attributes, chunks }
    }
}

pub(crate) fn mode_to_file_type(mode: mode_t) -> FileType {
    let ft = mode & SFlag::S_IFMT.bits();
    if ft == SFlag::S_IFDIR.bits() {
        FileType::Directory
    } else if ft == SFlag::S_IFCHR.bits() {
        FileType::CharDevice
    } else if ft == SFlag::S_IFBLK.bits() {
        FileType::BlockDevice
    } else if ft == SFlag::S_IFREG.bits() {
        FileType::RegularFile
    } else if ft == SFlag::S_IFLNK.bits() {
        FileType::Symlink
    } else if ft == SFlag::S_IFIFO.bits() {
        FileType::NamedPipe
    } else {
        // S_IFSOCK???
        panic!("Unknown file mode: {}. Could not identify file type.", mode);
    }
}

pub(crate) fn mode_to_permissions(mode: mode_t) -> u16 {
    #[cfg(target_os = "linux")]
    debug_assert!(mode <= u16::MAX as u32);
    (mode & !SFlag::S_IFMT.bits()) as u16
}

#[derive(Deserialize, Serialize)]
#[serde(remote = "Timespec")]
struct TimespecDef {
    pub sec: i64,
    pub nsec: i32,
}

#[cfg(test)]
mod tests {
    use nix::sys::stat::lstat;

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
