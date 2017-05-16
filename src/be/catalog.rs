use std::cell::Cell;
use std::collections::HashMap;
use std::fs::{File, read_dir};
use std::io::BufReader;
use std::path::{Path, PathBuf};

use common::errors::*;
use be::cas::read_chunks;
use be::inode::{Chunk, INode};
use be::store::Store;

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

#[derive(Default)]
pub struct HashMapCatalog {
    inodes: HashMap<u64, INode>,
    dir_entries: HashMap<u64, HashMap<PathBuf, u64>>,
    index_generator: IndexGenerator,
}

impl HashMapCatalog {
    pub fn new() -> HashMapCatalog {
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

impl Catalog for HashMapCatalog {
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

pub fn populate_with_dir<C, S>(catalog: &mut C,
                               store: &mut S,
                               dir: &Path,
                               chunk_size: u64)
                               -> Result<()>
    where C: Catalog,
          S: Store
{
    catalog
        .add_inode(dir, 1, vec![])
        .chain_err(|| ErrorKind::DirVisitError(dir.to_path_buf()))?;

    visit_dirs(catalog, store, dir, chunk_size, 1, 1)
        .chain_err(|| ErrorKind::DirVisitError(dir.to_path_buf()))?;

    Ok(())
}

fn visit_dirs<C, S>(catalog: &mut C,
                    store: &mut S,
                    dir: &Path,
                    chunk_size: u64,
                    dir_index: u64,
                    parent_index: u64)
                    -> Result<()>
    where C: Catalog,
          S: Store
{
    catalog
        .add_dir_entry(dir_index, Path::new("."), dir_index)?;
    catalog
        .add_dir_entry(dir_index, Path::new(".."), parent_index)?;

    for entry in read_dir(dir)? {
        let path = (entry?).path();
        let fpath = &path.as_path();
        let fname = Path::new(fpath
                                  .file_name()
                                  .ok_or_else(|| "Could not get file name from path")?);

        let mut chunks = Vec::new();
        if path.is_file() {
            let mut abs_path = dir.to_path_buf();
            abs_path.push(fname);
            let f = File::open(abs_path)?;
            let mut reader = BufReader::new(f);
            for (ref digest, ref data) in read_chunks(&mut reader, chunk_size)? {
                store.put(digest.clone(), data.as_ref());
                chunks.push(Chunk {
                                digest: digest.clone(),
                                size: data.len(),
                            });
            }
        }

        let index = catalog.get_next_index();
        catalog.add_inode(fpath, index, chunks)?;
        catalog.add_dir_entry(dir_index, fname, index)?;

        if path.is_dir() {
            visit_dirs(catalog, store, &path, chunk_size, index, dir_index)?;
        }
    }
    Ok(())
}
