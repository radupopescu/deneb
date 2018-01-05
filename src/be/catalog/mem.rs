use std::collections::HashMap;
use std::path::{Path, PathBuf};

use be::inode::{ChunkDescriptor, INode};

use common::errors::CatalogError;
use super::*;

pub struct MemCatalogBuilder;

impl CatalogBuilder for MemCatalogBuilder {
    type Catalog = MemCatalog;

    fn create<P: AsRef<Path>>(&self, _path: P) -> DenebResult<Self::Catalog> {
        Ok(MemCatalog::new())
    }

    fn open<P: AsRef<Path>>(&self, _path: P) -> DenebResult<Self::Catalog> {
        Ok(MemCatalog::new())
    }
}

#[derive(Default)]
pub struct MemCatalog {
    inodes: HashMap<u64, INode>,
    dir_entries: HashMap<u64, HashMap<PathBuf, u64>>,
    index_generator: IndexGenerator,
}

impl MemCatalog {
    pub fn new() -> MemCatalog {
        Self::default()
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
}

impl Catalog for MemCatalog {
    fn get_next_index(&self) -> u64 {
        self.index_generator.get_next()
    }

    fn get_inode(&self, index: u64) -> DenebResult<INode> {
        self.inodes
            .get(&index)
            .cloned()
            .ok_or_else(|| CatalogError::INodeRead(index).into())
    }

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> DenebResult<u64> {
        self.dir_entries
            .get(&parent)
            .and_then(|entries| entries.get(name))
            .cloned()
            .ok_or_else(|| CatalogError::DEntryNotFound(name.into(), parent).into())
    }

    fn get_dir_entries(&self, parent: u64) -> DenebResult<Vec<(PathBuf, u64)>> {
        self.dir_entries
            .get(&parent)
            .map(|entries| {
                entries
                    .iter()
                    .map(|(name, index)| (name.to_owned(), *index))
                    .collect::<Vec<(PathBuf, u64)>>()
            })
            .ok_or_else(|| CatalogError::DEntryRead(parent).into())
    }

    fn add_inode(
        &mut self,
        entry: &Path,
        index: u64,
        chunks: Vec<ChunkDescriptor>,
    ) -> DenebResult<()> {
        let inode = INode::new(index, entry, chunks)?;
        self.inodes.insert(index, inode);
        Ok(())
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> DenebResult<()> {
        let dir_entry = self.dir_entries.entry(parent).or_insert_with(|| {
            let mut dir_entry = HashMap::new();
            dir_entry.insert(name.to_owned(), index);
            dir_entry
        });
        dir_entry.entry(name.to_owned()).or_insert(index);

        let inode = self.inodes
            .get_mut(&index)
            .ok_or_else(|| CatalogError::INodeRead(index))?;

        inode.attributes.nlink += 1;

        Ok(())
    }
}
