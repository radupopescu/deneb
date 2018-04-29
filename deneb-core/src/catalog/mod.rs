use std::path::{Path, PathBuf};

use errors::{DenebError, DenebResult};
use inode::INode;

mod mem;
pub use self::mem::{MemCatalog, MemCatalogBuilder};

mod lmdb;
pub use self::lmdb::{LmdbCatalog, LmdbCatalogBuilder};

/// Describes the interface of catalog builders
pub trait CatalogBuilder {
    type Catalog: self::Catalog;

    fn create<P: AsRef<Path>>(&self, path: P) -> DenebResult<Self::Catalog>;

    fn open<P: AsRef<Path>>(&self, path: P) -> DenebResult<Self::Catalog>;
}

/// Describes the interface of metadata catalogs
///
pub trait Catalog {
    fn show_stats(&self) {}

    fn get_max_index(&self) -> u64;

    fn get_inode(&self, index: u64) -> DenebResult<INode>;

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> DenebResult<Option<u64>>;

    fn get_dir_entries(&self, parent: u64) -> DenebResult<Vec<(PathBuf, u64)>>;

    fn add_inode(
        &mut self,
        inode: INode
    ) -> DenebResult<()>;

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> DenebResult<()>;
}

#[derive(Copy, Clone)]
pub struct IndexGenerator {
    current_index: u64,
}

impl Default for IndexGenerator {
    fn default() -> IndexGenerator {
        IndexGenerator {
            current_index: 1,
        }
    }
}

impl IndexGenerator {
    pub fn starting_at(i0: u64) -> Result<IndexGenerator, DenebError> {
        if i0 > 0 {
            Ok(IndexGenerator {
                current_index: i0,
            })
        } else {
            Err(DenebError::IndexGenerator)
        }
    }

    pub fn get_next(&mut self) -> u64 {
        self.current_index += 1;
        self.current_index
    }
}
