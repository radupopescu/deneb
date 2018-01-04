use failure::ResultExt;

use bincode::{serialize, deserialize, Infinite};
use lmdb::{Cursor, Database, DatabaseFlags, Environment, Transaction, WriteFlags, NO_SUB_DIR};
use lmdb_sys::{MDB_envinfo, MDB_stat, mdb_env_info, mdb_env_stat};

use std::collections::BTreeMap;
use std::cmp::max;
use std::str::from_utf8;

use common::errors::{CatalogError, LMDBError};
use super::*;

const MAX_CATALOG_SIZE: usize = 100 * 1024 * 1024; // 100MB
const MAX_CATALOG_READERS: u32 = 100;
const MAX_CATALOG_DBS: u32 = 3;

const CATALOG_VERSION: u32 = 1;

// Note: Could be enhanced with an in-memory LRU cache
/// A filesystem metadata catalog backed by an LMDB database
pub struct LmdbCatalog {
    env: Environment,
    inodes: Database,
    dir_entries: Database,
    _meta: Database,
    version: u32,
    index_generator: IndexGenerator,
}

pub struct LmdbCatalogBuilder;

impl CatalogBuilder for LmdbCatalogBuilder {
    type Catalog = LmdbCatalog;

    fn create<P: AsRef<Path>>(&self, path: P) -> DenebResult<Self::Catalog> {
        let (env, inodes, dir_entries, meta) = init_db(&path)?;

        {
            let mut writer = env.begin_rw_txn()?;
            // Write catalog format version
            writer.put(meta, &"catalog_version",
                       &format!("{}", CATALOG_VERSION),
                       WriteFlags::empty())?;
            writer.commit()?;
        }

        info!("Created LMDB catalog {:?}.", path.as_ref());

        Ok(Self::Catalog {
               env: env,
               inodes: inodes,
               dir_entries: dir_entries,
               _meta: meta,
               version: CATALOG_VERSION,
               index_generator: IndexGenerator::default(),
           })
    }

    fn open<P: AsRef<Path>>(&self, path: P) -> DenebResult<Self::Catalog> {
        let (env, inodes, dir_entries, meta) = init_db(&path)?;

        let ver = {
            let reader = env.begin_ro_txn()?;
            let v = reader.get(meta, &"catalog_version")?;
            from_utf8(v)?.parse::<u32>()
        }?;

        if ver > CATALOG_VERSION {
            return Err(CatalogError::Version(ver).into());
        }

        // Retrieve the largest inode index in the catalog
        let starting_index = {
            let reader = env.begin_ro_txn()?;
            let mut max_index = 1;
            for (k, _v) in reader.open_ro_cursor(inodes)?.iter() {
                let idx = from_utf8(k)?.parse::<u64>()?;
                max_index = max(idx, max_index);
            }
            max_index
        };

        info!("Opened LMDB catalog {:?}.", path.as_ref());

        Ok(Self::Catalog {
               env: env,
               inodes: inodes,
               dir_entries: dir_entries,
               _meta: meta,
               version: ver,
               index_generator: IndexGenerator::starting_at(starting_index)?,
        })
    }
}

impl Catalog for LmdbCatalog {
    fn show_stats(&self) {
        let env_info = get_env_info(&self.env);
        info!("Environment information:");
        info!("  Map size: {}", env_info.me_mapsize);
        info!("  Last used page: {}", env_info.me_last_pgno);
        info!("  Last committed transaction id: {}",
              env_info.me_last_txnid);
        info!("  Maximum number of readers: {}", env_info.me_maxreaders);
        info!("  Current number of readers: {}", env_info.me_numreaders);

        let stats = get_env_stat(&self.env);
        info!("Environment stats:");
        info!("  Size of database page: {}", stats.ms_psize);
        info!("  Depth of B-tree: {}", stats.ms_depth);
        info!("  Number of internal pages: {}", stats.ms_branch_pages);
        info!("  Number of leaf pages: {}", stats.ms_leaf_pages);
        info!("  Number of overflow pages: {}", stats.ms_overflow_pages);
        info!("  Number of entries: {}", stats.ms_entries);

        info!("Catalog version: {}", self.version);
    }

    fn get_next_index(&self) -> u64 {
        self.index_generator.get_next()
    }

    fn get_inode(&self, index: u64) -> DenebResult<INode> {
        let reader = self.env.begin_ro_txn()?;
        let buffer = reader.get(self.inodes, &format!("{}", index))
            .context(CatalogError::INodeRead(index))?;
        deserialize::<INode>(buffer)
            .context(CatalogError::INodeDeserialization(index))
            .map_err(|e| e.into())
    }

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> DenebResult<u64> {
        let reader = self.env.begin_ro_txn()?;
        let buffer = reader.get(self.dir_entries, &format!("{}", parent))
            .context(CatalogError::DEntryRead(parent))?;
        let entries = deserialize::<BTreeMap<PathBuf, u64>>(buffer)
            .context(CatalogError::DEntryDeserialization(parent))?;
        let idx = entries
            .get(name)
            .cloned()
            .ok_or_else(|| CatalogError::DEntryNotFound(name.into(), parent))?;
        Ok(idx)
    }

    fn get_dir_entries(&self, parent: u64) -> DenebResult<Vec<(PathBuf, u64)>> {
        let reader = self.env.begin_ro_txn()?;
        let buffer = reader.get(self.dir_entries, &format!("{}", parent))
            .context(CatalogError::DEntryRead(parent))?;
        let entries = deserialize::<BTreeMap<PathBuf, u64>>(buffer)
            .context(CatalogError::DEntryDeserialization(parent))?;
        Ok(entries
               .iter()
               .map(|(name, index)| (name.to_owned(), *index))
               .collect::<Vec<(PathBuf, u64)>>())
    }

    fn add_inode(&mut self, entry: &Path, index: u64, chunks: Vec<ChunkDescriptor>) -> DenebResult<()> {
        let inode = INode::new(index, entry, chunks)?;

        let buffer = serialize(&inode, Infinite)
            .context(CatalogError::INodeSerialization(index))?;

        let mut writer = self.env.begin_rw_txn()?;

        writer
            .put(self.inodes, &format!("{}", index), &buffer, WriteFlags::empty())
            .context(CatalogError::INodeWrite(index))?;

        writer.commit()?;

        Ok(())
    }

    fn add_dir_entry(&mut self, parent: u64, name: &Path, index: u64) -> DenebResult<()> {
        let mut writer = self.env.begin_rw_txn()?;
        {
            // Retrieve and update dir entries for parent
            let mut entries = BTreeMap::new();
            if let Ok(buffer) = writer.get(self.dir_entries, &format!("{}", parent)) {
                // Dir entries exist for parent
                entries = deserialize::<BTreeMap<PathBuf, u64>>(buffer)
                    .context(CatalogError::DEntryDeserialization(parent))?;
                entries.insert(name.to_owned(), index);
            } else {
                // No dir entries exist for parent
                entries.insert(name.to_owned(), index);
            }

            // Write updated dir entries to database
            let buffer = serialize(&entries, Infinite)
                .context(CatalogError::DEntrySerialization(parent))?;
            writer
                .put(self.dir_entries, &format!("{}", parent), &buffer,
                     WriteFlags::empty())
                .context(CatalogError::DEntryWrite(parent))?;

            // Retrieve inode of index
            let buffer =
                {
                    let buffer = writer.get(self.inodes, &format!("{}", index))
                        .context(CatalogError::INodeRead(index))?;
                    let mut inode = deserialize::<INode>(buffer)
                        .context(CatalogError::INodeDeserialization(index))?;

                    // Update number of hardlink in inode
                    inode.attributes.nlink += 1;

                    // Write inode back to database
                    serialize(&inode, Infinite)
                        .context(CatalogError::INodeSerialization(index))
                }?;
            writer
                .put(self.inodes, &format!("{}", index), &buffer,
                     WriteFlags::empty())
                .context(CatalogError::INodeWrite(index))?;
        }
        writer.commit()?;
        Ok(())
    }
}

fn init_db<P: AsRef<Path>>(path: P) -> Result<(Environment, Database, Database, Database),
                                              LMDBError> {
    let env = open_environment(path.as_ref())?;

    // Create databases
    let inodes = try_create_db(&env, "inodes")?;
    let dir_entries = try_create_db(&env, "dir_entries")?;
    let meta = try_create_db(&env, "meta")?;

    Ok((env, inodes, dir_entries, meta))
}

fn open_environment(path: &Path) -> Result<Environment, LMDBError> {
    Environment::new()
        .set_flags(NO_SUB_DIR)
        .set_max_dbs(MAX_CATALOG_DBS)
        .set_max_readers(MAX_CATALOG_READERS)
        .set_map_size(MAX_CATALOG_SIZE)
        .open_with_permissions(path, 0o600)
        .map_err(|e| e.into())
}

fn try_create_db(env: &Environment, name: &str) -> Result<Database, LMDBError> {
    env.create_db(Some(name), DatabaseFlags::empty())
        .map_err(|e| e.into())
}

fn get_env_info(env: &Environment) -> MDB_envinfo {
    let mut env_info;
    unsafe {
        env_info = ::std::mem::zeroed::<MDB_envinfo>();
        mdb_env_info(env.env(), &mut env_info as *mut MDB_envinfo);
    }
    env_info
}

fn get_env_stat(env: &Environment) -> MDB_stat {
    let mut env_stat;
    unsafe {
        env_stat = ::std::mem::zeroed::<MDB_stat>();
        mdb_env_stat(env.env(), &mut env_stat as *mut MDB_stat);
    }
    env_stat
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
            let cb = LmdbCatalogBuilder;
            let catalog_path = prefix.path().to_owned().join("test-lmdb-catalog");
            {
                let catalog = cb.create(&catalog_path);
                assert!(catalog.is_ok());
                if let Ok(mut catalog) = catalog {
                    catalog.show_stats();
                    assert!(catalog.add_inode(Path::new("/tmp/"), 2, vec![]).is_ok());
                    assert!(catalog.add_inode(Path::new("/usr/"), 3, vec![]).is_ok());
                }
            }
            {
                let catalog = cb.open(&catalog_path);
                assert!(catalog.is_ok());
                if let Ok(catalog) = catalog {
                    assert_eq!(catalog.index_generator.get_next(), 4);
                }
            }
        }
    }
}
