use std::collections::HashMap;
use std::path::{Path, PathBuf};

use be::inode::{Chunk, INode};

use super::*;

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

    fn get_inode(&self, index: &u64) -> Option<&INode> {
        self.inodes.get(index)
    }

    fn get_dir_entries(&self, parent: &u64) -> Option<&HashMap<PathBuf, u64>> {
        self.dir_entries.get(parent)
    }

    fn add_inode(&mut self, entry: &Path, index: u64, chunks: Vec<Chunk>) -> Result<()> {
        let inode = INode::new(index, entry, chunks)
            .chain_err(|| format!("Could not construct inode {} for path: {:?}", index, entry))?;
        self.inodes.insert(index, inode);
        Ok(())
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> Result<()> {
        let dir = self.dir_entries.entry(parent);
        let mut dir_entry = dir.or_insert_with(|| {
                                                   let mut dir_entry = HashMap::new();
                                                   dir_entry.insert(name.to_owned(), index);
                                                   dir_entry
                                               });
        dir_entry.entry(name.to_owned()).or_insert_with(|| index);

        let inode = self.inodes
            .get_mut(&index)
            .ok_or_else(|| format!("Could not read inode: {}", index))?;

        inode.attributes.nlink += 1;

        Ok(())
    }
}

