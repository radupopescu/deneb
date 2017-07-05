use std::cell::Cell;
use std::path::{Path, PathBuf};

use common::errors::*;
use be::inode::{Chunk, INode};

mod mem;
pub use self::mem::{MemCatalog, MemCatalogBuilder};

mod lmdb;
pub use self::lmdb::{LmdbCatalog, LmdbCatalogBuilder};

/// Describes the interface of catalog builders
pub trait CatalogBuilder {
    type Catalog: self::Catalog;

    fn create<P: AsRef<Path>>(&self, path: P) -> Result<Self::Catalog>;

    fn open<P: AsRef<Path>>(&self, path: P) -> Result<Self::Catalog>;
}

/// Describes the interface of metadata catalogs
///
pub trait Catalog {
    fn show_stats(&self) {}

    fn get_next_index(&self) -> u64;

    fn get_inode(&self, index: u64) -> Result<INode>;

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> Result<u64>;

    fn get_dir_entry_inode(&self, parent: u64, name: &Path) -> Result<INode> {
        let index = self.get_dir_entry_index(parent, name)?;
        self.get_inode(index)
    }

    fn get_dir_entries(&self, parent: u64) -> Result<Vec<(PathBuf, u64)>>;

    fn add_inode(&mut self, entry: &Path, index: u64, chunks: Vec<Chunk>) -> Result<()>;

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
    fn starting_at(i0: u64) -> Result<IndexGenerator> {
        if i0 > 0 {
            Ok(IndexGenerator { current_index: Cell::new(i0) })
        } else {
            bail!("Invalid starting index for IndexGenerator");
        }
    }

    fn get_next(&self) -> u64 {
        let idx = self.current_index.get() + 1;
        self.current_index.replace(idx);
        idx
    }
}
