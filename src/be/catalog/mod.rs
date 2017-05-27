use std::cell::Cell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use common::errors::*;
use be::inode::{Chunk, INode};

/// Describes the interface of metadata catalogs
///
pub trait Catalog {
    fn get_next_index(&self) -> u64;
    fn get_inode(&self, index: &u64) -> Option<&INode>;
    fn get_dir_entries(&self, parent: &u64) -> Option<&HashMap<PathBuf, u64>>;
    fn add_inode(&mut self, entry: &Path, index: u64, digests: Vec<Chunk>) -> Result<()>;
    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> Result<()>;
}

struct IndexGenerator {
    current_index: Cell<u64>,
}

impl Default for IndexGenerator {
    fn default() -> IndexGenerator {
        IndexGenerator { current_index: Cell::new(1) }
    }
}

impl IndexGenerator {
    fn get_next(&self) -> u64 {
        let idx = self.current_index.get() + 1;
        self.current_index.replace(idx);
        idx
    }
}

mod mem;
pub use self::mem::MemCatalog;

mod lmdb;
pub use self::lmdb::LmdbCatalog;
