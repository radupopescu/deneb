use bincode::{deserialize, serialize};
use failure::ResultExt;
use lmdb::{
    Database, DatabaseFlags, Environment, EnvironmentFlags, Error as LmdbError, Transaction,
    WriteFlags,
};
use lmdb_sys::{mdb_env_info, mdb_env_stat, MDB_envinfo, MDB_stat};

use std::collections::BTreeMap;
use std::str::from_utf8;

use super::*;
use errors::CatalogError;

const MAX_CATALOG_SIZE: usize = 100 * 1024 * 1024; // 100MB
const MAX_CATALOG_READERS: u32 = 100;
const MAX_CATALOG_DBS: u32 = 3;

const CATALOG_VERSION: u32 = 1;

// Note: Could be enhanced with an in-memory LRU cache
/// A filesystem metadata catalog backed by an LMDB database
pub(super) struct LmdbCatalog {
    env: Environment,
    inodes: Database,
    dir_entries: Database,
    max_index: u64,
    meta: Database,
    version: u32,
}

impl LmdbCatalog {
    pub(super) fn open(path: &Path, create: bool) -> DenebResult<LmdbCatalog> {
        let (env, inodes, dir_entries, meta) = init_db(&path)?;

        if create {
            let mut writer = env.begin_rw_txn()?;
            // Write catalog format version
            writer.put(
                meta,
                &"catalog_version",
                &format!("{}", CATALOG_VERSION),
                WriteFlags::empty(),
            )?;
            writer.put(meta, &"max_index", &"1", WriteFlags::empty())?;
            writer.commit()?;

            info!("Created LMDB catalog {:?}.", path);
        }

        let ver = {
            let reader = env.begin_ro_txn()?;
            let v = reader.get(meta, &"catalog_version")?;
            from_utf8(v)?.parse::<u32>()
        }?;

        if ver > CATALOG_VERSION {
            return Err(CatalogError::Version(ver).into());
        }

        // Retrieve the largest inode index in the catalog
        let max_index = {
            let reader = env.begin_ro_txn()?;
            let v = reader.get(meta, &"max_index")?;
            from_utf8(v)?.parse::<u64>()
        }?;

        info!("Opened LMDB catalog {:?}.", path);

        Ok(LmdbCatalog {
            env,
            inodes,
            dir_entries,
            max_index,
            meta,
            version: ver,
        })
    }
}

impl Catalog for LmdbCatalog {
    fn show_stats(&self) {
        let env_info = get_env_info(&self.env);
        info!("Environment information:");
        info!("  Map size: {}", env_info.me_mapsize);
        info!("  Last used page: {}", env_info.me_last_pgno);
        info!(
            "  Last committed transaction id: {}",
            env_info.me_last_txnid
        );
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

    fn get_max_index(&self) -> u64 {
        self.max_index
    }

    fn get_inode(&self, index: u64) -> DenebResult<INode> {
        let reader = self.env.begin_ro_txn()?;
        let buffer = reader
            .get(self.inodes, &format!("{}", index))
            .context(CatalogError::INodeRead(index))?;
        deserialize::<INode>(buffer)
            .context(CatalogError::INodeDeserialization(index))
            .map_err(|e| e.into())
    }

    fn get_dir_entry_index(&self, parent: u64, name: &Path) -> DenebResult<Option<u64>> {
        let reader = self.env.begin_ro_txn()?;
        let buffer = reader
            .get(self.dir_entries, &format!("{}", parent))
            .context(CatalogError::DEntryRead(parent))?;
        let entries = deserialize::<BTreeMap<PathBuf, u64>>(buffer)
            .context(CatalogError::DEntryDeserialization(parent))?;
        Ok(entries.get(name).cloned())
    }

    fn get_dir_entries(&self, parent: u64) -> DenebResult<Vec<(PathBuf, u64)>> {
        let reader = self.env.begin_ro_txn()?;
        let buffer = reader
            .get(self.dir_entries, &format!("{}", parent))
            .context(CatalogError::DEntryRead(parent))?;
        let entries = deserialize::<BTreeMap<PathBuf, u64>>(buffer)
            .context(CatalogError::DEntryDeserialization(parent))?;
        Ok(entries
            .iter()
            .map(|(name, index)| (name.to_owned(), *index))
            .collect::<Vec<(PathBuf, u64)>>())
    }

    fn add_inode(&mut self, inode: INode) -> DenebResult<()> {
        let index = inode.attributes.index;
        let buffer = serialize(&inode).context(CatalogError::INodeSerialization(index))?;

        let max_index = {
            let reader = self.env.begin_ro_txn()?;
            let v = reader.get(self.meta, &"max_index")?;
            from_utf8(v)?.parse::<u64>()
        }?;

        let mut writer = self.env.begin_rw_txn()?;

        writer
            .put(
                self.inodes,
                &format!("{}", index),
                &buffer,
                WriteFlags::empty(),
            ).context(CatalogError::INodeWrite(index))?;

        if index > max_index {
            writer.put(
                self.meta,
                &"max_index",
                &format!("{}", index),
                WriteFlags::empty(),
            )?;
            self.max_index = index;
        }

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
            let buffer = serialize(&entries).context(CatalogError::DEntrySerialization(parent))?;
            writer
                .put(
                    self.dir_entries,
                    &format!("{}", parent),
                    &buffer,
                    WriteFlags::empty(),
                ).context(CatalogError::DEntryWrite(parent))?;

            // Retrieve inode of index
            let buffer = {
                let buffer = writer
                    .get(self.inodes, &format!("{}", index))
                    .context(CatalogError::INodeRead(index))?;
                let mut inode = deserialize::<INode>(buffer)
                    .context(CatalogError::INodeDeserialization(index))?;

                // Update number of hardlink in inode
                inode.attributes.nlink += 1;

                // Write inode back to database
                serialize(&inode).context(CatalogError::INodeSerialization(index))
            }?;
            writer
                .put(
                    self.inodes,
                    &format!("{}", index),
                    &buffer,
                    WriteFlags::empty(),
                ).context(CatalogError::INodeWrite(index))?;
        }
        writer.commit()?;
        Ok(())
    }
}

fn init_db<P: AsRef<Path>>(
    path: P,
) -> Result<(Environment, Database, Database, Database), LmdbError> {
    let env = open_environment(path.as_ref())?;

    // Create databases
    let inodes = try_create_db(&env, "inodes")?;
    let dir_entries = try_create_db(&env, "dir_entries")?;
    let meta = try_create_db(&env, "meta")?;

    Ok((env, inodes, dir_entries, meta))
}

fn open_environment(path: &Path) -> Result<Environment, LmdbError> {
    Environment::new()
        .set_flags(EnvironmentFlags::NO_SUB_DIR)
        .set_max_dbs(MAX_CATALOG_DBS)
        .set_max_readers(MAX_CATALOG_READERS)
        .set_map_size(MAX_CATALOG_SIZE)
        .open_with_permissions(path, 0o600)
}

fn try_create_db(env: &Environment, name: &str) -> Result<Database, LmdbError> {
    env.create_db(Some(name), DatabaseFlags::empty())
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
    use nix::sys::stat::lstat;
    use tempdir::TempDir;

    use super::*;

    use inode::FileAttributes;

    #[test]
    fn lmdb_catalog_create_then_reopen() {
        let tmp = TempDir::new("/tmp/deneb_lmdb_test");
        assert!(tmp.is_ok());
        if let Ok(prefix) = tmp {
            let catalog_path = prefix.path().to_owned().join("test-lmdb-catalog");
            {
                let catalog = open_catalog(CatalogType::Lmdb, &catalog_path, true);
                assert!(catalog.is_ok());
                if let Ok(mut catalog) = catalog {
                    catalog.show_stats();

                    let stats1 = lstat(Path::new("/tmp/"));
                    let stats2 = lstat(Path::new("/usr/"));
                    assert!(stats1.is_ok());
                    assert!(stats2.is_ok());
                    if let (Ok(stats1), Ok(stats2)) = (stats1, stats2) {
                        let attrs1 = FileAttributes::with_stats(stats1, 2);
                        let attrs2 = FileAttributes::with_stats(stats2, 3);
                        let inode1 = INode::new(attrs1, vec![]);
                        let inode2 = INode::new(attrs2, vec![]);
                        assert!(catalog.add_inode(inode1).is_ok());
                        assert!(catalog.add_inode(inode2).is_ok());
                    }
                }
            }
            {
                let catalog = open_catalog(CatalogType::Lmdb, &catalog_path, false);
                assert!(catalog.is_ok());
                if let Ok(catalog) = catalog {
                    assert_eq!(catalog.get_max_index(), 3);
                }
            }
        }
    }
}
