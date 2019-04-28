use {
    crate::{errors::DenebResult, inode::INode},
    std::path::{Path, PathBuf},
};

mod lmdb;
mod mem;

#[derive(Clone, Copy)]
pub enum CatalogType {
    InMemory,
    Lmdb,
}

pub fn open_catalog<P: AsRef<Path>>(
    catalog_type: CatalogType,
    path: P,
    create: bool,
) -> DenebResult<Box<dyn Catalog>> {
    Ok(match catalog_type {
        CatalogType::InMemory => Box::new(mem::MemCatalog::new()),
        CatalogType::Lmdb => Box::new(lmdb::LmdbCatalog::open(path.as_ref(), create)?),
    })
}

/// Describes the interface of metadata catalogs
///
pub trait Catalog: Send {
    fn show_stats(&self) {}

    fn max_index(&self) -> u64;

    fn inode(&self, index: u64) -> DenebResult<INode>;

    fn dir_entry_index(&self, parent: u64, name: &Path) -> DenebResult<Option<u64>>;

    fn dir_entries(&self, parent: u64) -> DenebResult<Vec<(PathBuf, u64)>>;

    fn add_inode(&mut self, inode: &INode) -> DenebResult<()>;

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> DenebResult<()>;

    fn remove_inode(&mut self, index: u64) -> DenebResult<()>;
}

#[derive(Copy, Clone)]
pub(crate) struct IndexGenerator {
    current_index: u64,
}

impl Default for IndexGenerator {
    fn default() -> IndexGenerator {
        IndexGenerator { current_index: 1 }
    }
}

impl IndexGenerator {
    pub fn starting_at(i0: u64) -> IndexGenerator {
        IndexGenerator { current_index: i0 }
    }

    pub fn next(&mut self) -> u64 {
        if self.current_index == std::u64::MAX {
            panic!("Index generator: max index reached.")
        }
        self.current_index += 1;
        self.current_index
    }
}
