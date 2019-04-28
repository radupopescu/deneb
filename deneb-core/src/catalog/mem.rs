use {
    super::*,
    crate::{errors::CatalogError, inode::INode},
    log::info,
    std::{
        collections::HashMap,
        path::{Path, PathBuf},
    },
};

#[derive(Default)]
pub(super) struct MemCatalog {
    inodes: HashMap<u64, INode>,
    dir_entries: HashMap<u64, HashMap<PathBuf, u64>>,
    max_index: u64,
}

impl MemCatalog {
    pub(super) fn new() -> MemCatalog {
        Self::default()
    }
}

impl Catalog for MemCatalog {
    fn show_stats(&self) {
        info!("Catalog stats: number of inodes: {}", self.inodes.len());
        info!("Directory entries:");
        for (k1, v1) in &self.dir_entries {
            for (k2, v2) in v1.iter() {
                info!("  parent: {}, path: {:?}, inode: {}", k1, k2, v2);
            }
        }
    }

    fn max_index(&self) -> u64 {
        self.max_index
    }

    fn inode(&self, index: u64) -> DenebResult<INode> {
        self.inodes
            .get(&index)
            .cloned()
            .ok_or_else(|| CatalogError::INodeRead(index).into())
    }

    fn dir_entry_index(&self, parent: u64, name: &Path) -> DenebResult<Option<u64>> {
        Ok(self
            .dir_entries
            .get(&parent)
            .and_then(|entries| entries.get(name))
            .cloned())
    }

    fn dir_entries(&self, parent: u64) -> DenebResult<Vec<(PathBuf, u64)>> {
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

    fn add_inode(&mut self, inode: &INode) -> DenebResult<()> {
        let index = inode.attributes.index;
        self.inodes.insert(index, inode.clone());
        if index > self.max_index {
            self.max_index = index;
        }
        Ok(())
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> DenebResult<()> {
        let dir_entry = self.dir_entries.entry(parent).or_insert_with(|| {
            let mut dir_entry = HashMap::new();
            dir_entry.insert(name.to_owned(), index);
            dir_entry
        });
        dir_entry.entry(name.to_owned()).or_insert(index);

        let inode = self
            .inodes
            .get_mut(&index)
            .ok_or_else(|| CatalogError::INodeRead(index))?;

        inode.attributes.nlink += 1;

        Ok(())
    }

    fn remove_inode(&mut self, index: u64) -> DenebResult<()> {
        self.inodes.remove(&index);
        self.dir_entries.remove(&index);
        Ok(())
    }
}
