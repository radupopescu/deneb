use fuse::{FileAttr, FileType};
use nix::libc::mode_t;
use nix::sys::stat::{S_IFMT, S_IFDIR, S_IFCHR, S_IFBLK, S_IFREG, S_IFLNK, S_IFIFO, stat};

use std::cmp::{min, max};
use std::collections::HashMap;
use std::fmt;
use std::fs::read_dir;
use std::i32::MAX;
use std::path::{Path, PathBuf};
use time::Timespec;

use errors::*;
use hash::ContentHash;

struct INode {
    attributes: FileAttr,
    content_hash: ContentHash,
}

impl INode {
    fn new(index: u64, path: &Path, hash: ContentHash) -> Result<INode> {
        let stats = stat(path)?;
        let mut attributes = FileAttr {
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
            content_hash: hash,
        })
    }
}

impl fmt::Display for INode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "Attributes: {:?}, content_hash: {:?}",
               self.attributes,
               self.content_hash)
    }
}

struct IndexGenerator {
    current_index: u64,
}
impl Default for IndexGenerator {
    fn default() -> IndexGenerator {
        IndexGenerator { current_index: 1 }
    }
}
impl IndexGenerator {
    fn get_next(&mut self) -> u64 {
        self.current_index += 1;
        self.current_index
    }
}

pub struct Catalog {
    inodes: HashMap<u64, INode>,
    dir_entries: HashMap<u64, HashMap<PathBuf, u64>>,
    index_generator: IndexGenerator,
}

impl Catalog {
    pub fn from_dir(dir: &Path) -> Result<Catalog> {
        let mut catalog = Catalog {
            inodes: HashMap::new(),
            dir_entries: HashMap::new(),
            index_generator: IndexGenerator::default(),
        };
        catalog.add_root(dir)?;
        catalog.add_dir_entry(1, Path::new("/"), 1);
        catalog.visit_dirs(dir, 1)?;
        Ok(catalog)
    }

    pub fn add_root(&mut self, root: &Path) -> Result<()> {
        let inode = INode::new(1, root, ContentHash::new())?;
        self.inodes.insert(1, inode);
        Ok(())
    }

    pub fn add_inode(&mut self, entry: &Path, content_hash: ContentHash) -> Result<u64> {
        let index = self.index_generator.get_next();
        let inode = INode::new(index, entry, content_hash)?;
        self.inodes.insert(index, inode);
        Ok(index)
    }

    pub fn show_stats(&self) {
        debug!("Catalog stats: number of inodes: {}", self.inodes.len());
        debug!("Directory entries:");
        for (k1,v1) in self.dir_entries.iter() {
            for (k2, v2) in v1.iter() {
                debug!("  parent: {}, path: {:?}, inode: {}", k1, k2, v2);
            }
        }
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) {
        let dir = self.dir_entries.entry(parent);
        let mut dir_entry = dir.or_insert_with(|| {
            let mut dir_entry = HashMap::new();
            dir_entry.insert(name.to_owned(), index);
            dir_entry
        });
        dir_entry.entry(name.to_owned()).or_insert_with(|| index);
    }

    fn visit_dirs(&mut self, dir: &Path, parent: u64) -> Result<()> {
        for entry in read_dir(dir)? {
            let path = (entry?).path();
            let fpath = &path.as_path();
            let fname = Path::new(fpath
                                  .file_name()
                                  .ok_or_else(|| "Could not get file name from path")?);
            let index = self.add_inode(fpath, ContentHash::new())?;
            self.add_dir_entry(parent, fname, index);
            if path.is_dir() {
                self.visit_dirs(&path, index)?;
            }
        }
        Ok(())
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
        let stats = stat("/etc").unwrap();
        assert_eq!(mode_to_file_type(stats.st_mode), FileType::Directory);

        let stats = stat("/etc/hosts").unwrap();
        assert_eq!(mode_to_file_type(stats.st_mode), FileType::RegularFile);
    }

    #[test]
    fn mode_to_permissions_test() {
        let stats = stat("/etc/hosts").unwrap();
        assert_eq!(mode_to_permissions(stats.st_mode), 0o644);
    }
}
