use fuse::{FileAttr, FileType};
use nix::libc::mode_t;
use nix::sys::stat::{S_IFMT, S_IFDIR, S_IFCHR, S_IFBLK, S_IFREG, S_IFLNK, S_IFIFO, lstat};
use rust_sodium::crypto::hash::sha512::Digest;
use rust_sodium::crypto::hash::hash;
use time::Timespec;

use std::cmp::{min, max};
use std::collections::HashMap;
use std::fmt;
use std::fs::read_dir;
use std::i32::MAX;
use std::path::{Path, PathBuf};

use std::fs::File;
use std::io::{Read, BufReader};

use errors::*;
use store::Store;

pub struct INode {
    pub attributes: FileAttr,
    pub digests: Vec<Digest>,
}

/// Describes the interface of metadata catalogs
///
pub trait Catalog {
    fn get_inode(&self, index: &u64) -> Option<&INode>;

    fn get_dir_entries(&self, parent: &u64) -> Option<&HashMap<PathBuf, u64>>;
}

impl INode {
    fn new(index: u64, path: &Path, hashes: &[Digest]) -> Result<INode> {
        let stats = lstat(path)?;
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
               digests: hashes.to_vec(),
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

pub struct HashMapCatalog {
    inodes: HashMap<u64, INode>,
    dir_entries: HashMap<u64, HashMap<PathBuf, u64>>,
    index_generator: IndexGenerator,
}

impl HashMapCatalog {
    pub fn with_dir<S: Store>(dir: &Path, store: &mut S) -> Result<HashMapCatalog> {
        let mut catalog = HashMapCatalog {
            inodes: HashMap::new(),
            dir_entries: HashMap::new(),
            index_generator: IndexGenerator::default(),
        };
        catalog
            .add_root(dir)
            .chain_err(|| ErrorKind::DirVisitError(dir.to_path_buf()))?;
        catalog
            .visit_dirs(store, dir, 1, 1)
            .chain_err(|| ErrorKind::DirVisitError(dir.to_path_buf()))?;
        Ok(catalog)
    }

    pub fn show_stats(&self) {
        info!("Catalog stats: number of inodes: {}", self.inodes.len());
        info!("Directory entries:");
        for (k1, v1) in &self.dir_entries {
            for (k2, v2) in v1.iter() {
                info!("  parent: {}, path: {:?}, inode: {}", k1, k2, v2);
            }
        }
    }

    fn add_root(&mut self, root: &Path) -> Result<()> {
        let inode = INode::new(1, root, &[])
            .chain_err(|| "Could not construct root inode")?;
        self.inodes.insert(1, inode);
        Ok(())
    }

    fn add_inode(&mut self, entry: &Path, digests: &[Digest]) -> Result<u64> {
        let index = self.index_generator.get_next();
        let inode = INode::new(index, entry, digests)?;
        self.inodes.insert(index, inode);
        Ok(index)
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) {
        let dir = self.dir_entries.entry(parent);
        let mut dir_entry = dir.or_insert_with(|| {
                                                   let mut dir_entry = HashMap::new();
                                                   dir_entry.insert(name.to_owned(), index);
                                                   dir_entry
                                               });
        dir_entry.entry(name.to_owned()).or_insert_with(|| index);
        if let Some(inode) = self.inodes.get_mut(&index) {
            inode.attributes.nlink += 1;
        }
    }

    fn visit_dirs<S>(&mut self,
                     store: &mut S,
                     dir: &Path,
                     dir_index: u64,
                     parent_index: u64)
                     -> Result<()>
        where S: Store
    {
        self.add_dir_entry(dir_index, Path::new("."), dir_index);
        self.add_dir_entry(dir_index, Path::new(".."), parent_index);

        for entry in read_dir(dir)? {
            let path = (entry?).path();
            let fpath = &path.as_path();
            let fname = Path::new(fpath
                                      .file_name()
                                      .ok_or_else(|| "Could not get file name from path")?);

            // TODO: This has to be rewritten with buffered reads + chunking
            let mut digests = Vec::new();
            if path.is_file() {
                let mut abs_path = dir.to_path_buf();
                abs_path.push(fname);
                let f = File::open(abs_path)?;
                let mut buffer = Vec::new();
                let _ = BufReader::new(f).read_to_end(&mut buffer)?;
                let digest = hash(buffer.as_ref());
                store.put(digest, buffer.as_ref());
                digests.push(digest);
            }

            let index = self.add_inode(fpath, &digests)?;
            self.add_dir_entry(dir_index, fname, index);

            if path.is_dir() {
                self.visit_dirs(store, &path, index, dir_index)?;
            }
        }
        Ok(())
    }
}

impl Catalog for HashMapCatalog {
    fn get_inode(&self, index: &u64) -> Option<&INode> {
        self.inodes.get(index)
    }

    fn get_dir_entries(&self, parent: &u64) -> Option<&HashMap<PathBuf, u64>> {
        self.dir_entries.get(parent)
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
