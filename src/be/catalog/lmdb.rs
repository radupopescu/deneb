use bincode::{serialize, deserialize, Infinite};
use lmdb_rs::{EnvBuilder, Environment, DbHandle};
use lmdb_rs::core::{EnvCreateNoSubDir, DbCreate};

use super::*;

const MAX_CATALOG_SIZE: u64 = 100 * 1024 * 1024; // 100MB
const MAX_CATALOG_READERS: usize = 100;
const MAX_CATALOG_DBS: usize = 3;

const CATALOG_VERSION: u64 = 1;

pub struct LmdbCatalog {
    env: Environment,
    inodes: DbHandle,
    dir_entries: DbHandle,
    meta: DbHandle,
    version: u64,
    index_generator: IndexGenerator,
}

impl LmdbCatalog {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<LmdbCatalog> {
        let env = open_environment(path)?;

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

            Ok(LmdbCatalog {
                   env: env,
                   inodes: inodes,
                   dir_entries: dir_entries,
                   meta: meta,
                   version: CATALOG_VERSION,
                   index_generator: IndexGenerator::default(),
               })
        }
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<LmdbCatalog> {
        let env = open_environment(path)?;

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

            Ok(LmdbCatalog {
                   env: env,
                   inodes: inodes,
                   dir_entries: dir_entries,
                   meta: meta,
                   version: ver,
                   index_generator: IndexGenerator::default(),
               })
        }
    }

    pub fn show_stats(&self) {
        if let Ok(env_info) = self.env.info() {
            info!("Environment information:");
            info!("  Map size: {}", env_info.me_mapsize);
            info!("  Last used page: {}", env_info.me_last_pgno);
            info!("  Last committed transaction id: {}", env_info.me_last_txnid);
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
                return deserialize::<INode>(buffer).ok()
            }
        }
        None
    }

    fn get_dir_entry_index(&self, _parent: u64, _name: &Path) -> Option<u64> {
        None
    }

    fn get_dir_entries(&self, _parent: u64) -> Option<Vec<(PathBuf, u64)>> {
        None
    }

    fn add_inode(&mut self, _entry: &Path, _index: u64, _digests: Vec<Chunk>) -> Result<()> {
        bail!("Not implemented");
    }

    fn add_dir_entry(&mut self, _parent: u64, _name: &Path, _index: u64) -> Result<()> {
        bail!("Not implemented");
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
    use std::fs::remove_dir_all;

    use super::*;

    #[test]
    fn lmdb_catalog_create_then_reopen() {
        let _ = remove_dir_all("/tmp/test-lmdb-catalog");
        let _ = remove_dir_all("/tmp/test-lmdb-catalog-lock");
        {
            let catalog = LmdbCatalog::create(Path::new("/tmp/test-lmdb-catalog"));
            assert!(catalog.is_ok());
            if let Ok(catalog) = catalog {
                catalog.show_stats();
            }
        }
        {
            assert!(LmdbCatalog::open(Path::new("/tmp/test-lmdb-catalog")).is_ok());
        }
    }
}
