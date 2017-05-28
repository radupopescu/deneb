use bincode::{serialize, deserialize, Infinite};
use lmdb_rs::{EnvBuilder, Environment, DbHandle};
use lmdb_rs::core::{EnvCreateNoSubDir, DbCreate};

use std::collections::BTreeMap;
use std::cmp::max;

use super::*;

const MAX_CATALOG_SIZE: u64 = 100 * 1024 * 1024; // 100MB
const MAX_CATALOG_READERS: usize = 100;
const MAX_CATALOG_DBS: usize = 3;

const CATALOG_VERSION: u64 = 1;

// Note: Could be enhanced with an in-memory LRU cache
/// A filesystem metadata catalog backed by an LMDB database
pub struct LmdbCatalog {
    env: Environment,
    inodes: DbHandle,
    dir_entries: DbHandle,
    _meta: DbHandle,
    version: u64,
    index_generator: IndexGenerator,
}

impl LmdbCatalog {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<LmdbCatalog> {
        let env = open_environment(path.as_ref())?;

        {
            // Create databases
            let inodes = try_create_db(&env, "inodes")?;
            let dir_entries = try_create_db(&env, "dir_entries")?;
            let meta = try_create_db(&env, "meta")?;

            {
                let writer = env.new_transaction()?;
                {
                    // Write catalog format version
                    let db = writer.bind(&meta);
                    db.set(&"catalog_version", &CATALOG_VERSION)?;
                }
                writer.commit()?;
            }

            info!("Created LMDB catalog {:?}.", path.as_ref());

            Ok(LmdbCatalog {
                   env: env,
                   inodes: inodes,
                   dir_entries: dir_entries,
                   _meta: meta,
                   version: CATALOG_VERSION,
                   index_generator: IndexGenerator::default(),
               })
        }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<LmdbCatalog> {
        let env = open_environment(path.as_ref())?;

        {
            // Create databases
            let inodes = try_create_db(&env, "inodes")?;
            let dir_entries = try_create_db(&env, "dir_entries")?;
            let meta = try_create_db(&env, "meta")?;

            let ver = {
                let reader = env.get_reader()?;
                let db = reader.bind(&meta);
                db.get::<u64>(&"catalog_version")?
            };

            if ver > CATALOG_VERSION {
                bail!(ErrorKind::LmdbCatalogError("Invalid catalog version".to_owned()));
            }

            // Retrieve the largest inode index in the catalog
            let starting_index = {
                let reader = env.get_reader()?;
                let mut max_index = 1;
                let db = reader.bind(&inodes);
                for it in db.iter()? {
                    let idx = it.get_key::<u64>();
                    max_index = max(idx, max_index);
                }
                max_index
            };

            info!("Opened LMDB catalog {:?}.", path.as_ref());

            Ok(LmdbCatalog {
                   env: env,
                   inodes: inodes,
                   dir_entries: dir_entries,
                   _meta: meta,
                   version: ver,
                   index_generator: IndexGenerator::starting_at(starting_index)?,
               })
        }
    }

    pub fn show_stats(&self) {
        if let Ok(env_info) = self.env.info() {
            info!("Environment information:");
            info!("  Map size: {}", env_info.me_mapsize);
            info!("  Last used page: {}", env_info.me_last_pgno);
            info!("  Last committed transaction id: {}",
                  env_info.me_last_txnid);
            info!("  Maximum number of readers: {}", env_info.me_maxreaders);
            info!("  Current number of readers: {}", env_info.me_numreaders);
        }
        if let Ok(stats) = self.env.stat() {
            info!("Environment stats:");
            info!("  Size of database page: {}", stats.ms_psize);
            info!("  Depth of B-tree: {}", stats.ms_depth);
            info!("  Number of internal pages: {}", stats.ms_branch_pages);
            info!("  Number of leaf pages: {}", stats.ms_leaf_pages);
            info!("  Number of overflow pages: {}", stats.ms_overflow_pages);
            info!("  Number of entries: {}", stats.ms_entries);
        }
        info!("Catalog version: {}", self.version);
    }
}

impl Catalog for LmdbCatalog {
    fn get_next_index(&self) -> u64 {
        self.index_generator.get_next()
    }

    fn get_inode(&self, index: u64) -> Option<INode> {
        if let Ok(reader) = self.env.get_reader() {
            let db = reader.bind(&self.inodes);
            if let Ok(buffer) = db.get::<&[u8]>(&index) {
                return deserialize::<INode>(buffer).ok();
            }
        }
        None
    }

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> Option<u64> {
        if let Ok(reader) = self.env.get_reader() {
            let db = reader.bind(&self.dir_entries);
            if let Ok(buffer) = db.get::<&[u8]>(&parent) {
                if let Ok(entries) = deserialize::<BTreeMap<PathBuf, u64>>(buffer) {
                    return entries.get(name).map(|e| *e);
                }
            }
        }
        None
    }

    fn get_dir_entries(&self, parent: u64) -> Option<Vec<(PathBuf, u64)>> {
        if let Ok(reader) = self.env.get_reader() {
            let db = reader.bind(&self.dir_entries);
            if let Ok(buffer) = db.get::<&[u8]>(&parent) {
                if let Ok(entries) = deserialize::<BTreeMap<PathBuf, u64>>(buffer) {
                    return Some(entries
                                    .iter()
                                    .map(|(name, index)| (name.to_owned(), *index))
                                    .collect::<Vec<(PathBuf, u64)>>());
                }
            }
        }
        None
    }

    fn add_inode(&mut self, entry: &Path, index: u64, chunks: Vec<Chunk>) -> Result<()> {
        let inode = INode::new(index, entry, chunks)
            .chain_err(|| format!("Could not construct inode {} for path: {:?}", index, entry))?;

        let buffer = serialize(&inode, Infinite)
            .chain_err(|| format!("Could not serialize inode {}", index))?;

        let writer = self.env.new_transaction()?;
        {
            let db = writer.bind(&self.inodes);
            db.set(&index, &buffer)
                .chain_err(|| format!("Could not write inode {} to database", index))?;
        }
        writer.commit()?;

        Ok(())
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> Result<()> {
        let writer = self.env.new_transaction()?;
        {
            // Retrieve and update dir entries for parent
            let dir_entry_db = writer.bind(&self.dir_entries);
            let mut entries = BTreeMap::new();
            if let Ok(buffer) = dir_entry_db.get::<&[u8]>(&parent) {
                // Dir entries exist for parent
                entries =
                    deserialize::<BTreeMap<PathBuf, u64>>(buffer)
                        .chain_err(|| format!("Could no retrieve dir entries for {}", parent))?;
                entries.insert(name.to_owned(), index);
            } else {
                // No dir entries exist for parent
                entries.insert(name.to_owned(), index);
            }

            // Write updated dir entries to database
            let buffer =
                serialize(&entries, Infinite)
                    .chain_err(|| format!("Could not serialize dir entries for {}", parent))?;
            dir_entry_db
                .set(&parent, &buffer)
                .chain_err(|| format!("Could not write dir entries for {} to database", parent))?;

            // Retrieve inode of index
            let inode_db = writer.bind(&self.inodes);
            let buffer = inode_db
                .get::<&[u8]>(&index)
                .chain_err(|| format!("Could not retrieve inode {}", index))?;
            let mut inode = deserialize::<INode>(buffer)
                .chain_err(|| format!("Could not deserialize inode {}", index))?;

            // Update number of hardlink in inode
            inode.attributes.nlink += 1;

            // Write inode back to database
            let buffer = serialize(&inode, Infinite)
                .chain_err(|| format!("Could not serialize inode of {}", index))?;
            inode_db
                .set(&index, &buffer)
                .chain_err(|| format!("Could not write inode of {} to database", index))?;
        }
        writer.commit()?;
        Ok(())
    }
}

fn open_environment<P: AsRef<Path>>(path: P) -> Result<Environment> {
    EnvBuilder::new()
        .flags(EnvCreateNoSubDir)
        .max_dbs(MAX_CATALOG_DBS)
        .max_readers(MAX_CATALOG_READERS)
        .map_size(MAX_CATALOG_SIZE)
        .open(path, 0o600)
        .chain_err(|| "Could not open LMDB environment")
}

fn try_create_db(env: &Environment, name: &str) -> Result<DbHandle> {
    env.create_db(name, DbCreate)
        .chain_err(|| format!("Could not get a handle to the {} database", name))
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn lmdb_catalog_create_then_reopen() {
        let tmp = TempDir::new("/tmp/deneb_lmdb_test");
        assert!(tmp.is_ok());
        if let Ok(prefix) = tmp {
            let catalog_path = prefix.path().to_owned().join("test-lmdb-catalog");
            {
                let catalog = LmdbCatalog::create(&catalog_path);
                assert!(catalog.is_ok());
                if let Ok(mut catalog) = catalog {
                    catalog.show_stats();
                    assert!(catalog.add_inode(Path::new("/tmp/"), 2, vec![]).is_ok());
                    assert!(catalog.add_inode(Path::new("/usr/"), 3, vec![]).is_ok());
                }
            }
            {
                let catalog = LmdbCatalog::open(&catalog_path);
                assert!(catalog.is_ok());
                if let Ok(catalog) = catalog {
                    assert_eq!(catalog.index_generator.get_next(), 4);
                }
            }
        }
    }
}
