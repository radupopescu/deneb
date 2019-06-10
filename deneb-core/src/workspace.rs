mod commit;
mod dir;
mod file;
mod inode;

pub(in crate) use commit::Summary as CommitSummary;

use {
    self::{
        commit::commit_workspace,
        dir::{DirEntry, Workspace as DirWorkspace},
        file::Workspace as FileWorkspace,
        inode::Workspace as INodeWorkspace,
    },
    crate::{
        catalog::{open_catalog, Catalog, CatalogType, IndexGenerator},
        crypt::EncryptionKey,
        errors::{DenebResult, DirWorkspaceEntryLookupError, EngineError, WorkspaceError},
        inode::{mode_to_permissions, FileAttributeChanges, FileAttributes, FileType, INode},
        manifest::Manifest,
        populate_with_dir,
        store::{open_store, Store, StoreType},
        util::{atomic_write, get_egid, get_euid},
    },
    failure::ResultExt,
    log::{error, info},
    nix::libc::mode_t,
    std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        ffi::OsStr,
        fs::{create_dir_all, remove_dir_all, File},
        path::{Path, PathBuf},
        rc::Rc,
    },
    time::now_utc,
};

const MANIFEST_PATH: &str = "data/manifest";
const REFLOG_PATH: &str = "data/reflog";

pub(in crate) struct Workspace {
    catalog: Box<dyn Catalog>,
    store: Rc<RefCell<Box<dyn Store>>>,
    manifest: Manifest,
    index_generator: IndexGenerator,
    dirs: HashMap<u64, DirWorkspace>,
    files: HashMap<u64, FileWorkspace>,
    inodes: HashMap<u64, INodeWorkspace>,
    deleted_inodes: HashSet<u64>,
    work_dir: PathBuf,
    dirty: bool,
}

impl Workspace {
    pub(in crate) fn new(
        catalog_type: CatalogType,
        store_type: StoreType,
        work_dir: PathBuf,
        encryption_key: Option<EncryptionKey>,
        sync_dir: Option<PathBuf>,
        chunk_size: usize,
    ) -> DenebResult<Workspace> {
        // Create an object store
        let mut store = open_store(store_type, &work_dir, encryption_key, chunk_size)?;

        let catalog_root = work_dir.join("scratch");
        create_dir_all(catalog_root.as_path())?;
        let catalog_path = catalog_root.join("current_catalog");
        info!("Catalog path: {:?}", catalog_path);

        let manifest_path = work_dir.to_path_buf().join(MANIFEST_PATH);
        info!("Manifest path: {:?}", manifest_path);

        // Create the file metadata catalog and populate it with the contents of "sync_dir"
        if let Some(sync_dir) = sync_dir {
            init(
                &mut *store,
                catalog_type,
                catalog_path.as_path(),
                manifest_path.as_path(),
                sync_dir.as_path(),
                chunk_size,
            )?;
        }

        // If there is no work dir yet (first start, no sync_dir) create and initialize the repository
        if !manifest_path.exists() {
            let empty_dir = work_dir.join("empty_dir");
            create_dir_all(&empty_dir)?;
            init(
                &mut *store,
                catalog_type,
                catalog_path.as_path(),
                manifest_path.as_path(),
                empty_dir.as_path(),
                chunk_size,
            )?;
            remove_dir_all(&empty_dir)?;
        }

        // Load the repository manifest
        let buf = store.read_special_file(&manifest_path)?;
        let manifest = Manifest::deserialize(&buf)?;

        // Get the catalog out of storage and open it
        {
            let root_hash = manifest.root_hash;
            let chunk = store.chunk(&root_hash)?;
            let mut buf = vec![0; chunk.size()];
            chunk.read_at(&mut buf, 0)?;
            atomic_write(catalog_path.as_path(), buf.as_slice())?;
        }

        let catalog = open_catalog(catalog_type, catalog_path.as_path(), false)?;
        catalog.show_stats();

        let index_generator = IndexGenerator::starting_at(catalog.max_index());

        let ws = Workspace {
            catalog,
            store: Rc::new(RefCell::new(store)),
            manifest,
            index_generator,
            dirs: HashMap::new(),
            files: HashMap::new(),
            inodes: HashMap::new(),
            deleted_inodes: HashSet::new(),
            work_dir,
            dirty: false,
        };

        Ok(ws)
    }

    pub(in crate) fn get_attr(&mut self, index: u64) -> DenebResult<FileAttributes> {
        let ws = self.inode_ws(index)?;
        Ok(ws.inode().attributes)
    }

    pub(in crate) fn set_attr(
        &mut self,
        index: u64,
        changes: &FileAttributeChanges,
    ) -> DenebResult<FileAttributes> {
        let ws = self.inode_ws_mut(index)?;
        ws.update_attributes(changes);
        let attrs = ws.inode().attributes;

        if let Some(new_size) = changes.size {
            if let Some(ref mut ws) = self.files.get_mut(&index) {
                ws.truncate(new_size);
            } else {
                return Err(WorkspaceError::FileLookup(index).into());
            }
        }

        self.dirty = true;

        Ok(attrs)
    }

    pub(in crate) fn lookup(
        &mut self,
        parent: u64,
        name: &OsStr,
    ) -> DenebResult<Option<FileAttributes>> {
        let index = if let Some(ws) = self.dirs.get(&parent) {
            ws.entries()
                .iter()
                .find(|DirEntry { name: ref n, .. }| n == &PathBuf::from(name))
                .map(|&DirEntry { index, .. }| index)
        } else {
            self.catalog
                .dir_entry_index(parent, PathBuf::from(name).as_path())?
        };
        if let Some(index) = index {
            self.get_attr(index).map(Some)
        } else {
            Ok(None)
        }
    }

    // Note: We perform inefficient double lookups since Catalog::dir_entries returns
    //       a Result and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    pub(in crate) fn open_dir(&mut self, index: u64) -> DenebResult<()> {
        if !self.dirs.contains_key(&index) {
            let entries = self
                .catalog
                .dir_entries(index)?
                .iter()
                .map(|&(ref name, idx)| {
                    if let Ok(ws) = self.inode_ws(idx) {
                        DirEntry::new(idx, name.clone(), ws.inode().attributes.kind)
                    } else {
                        panic!("Fatal engine error. Could not retrieve inode {}", idx)
                    }
                })
                .collect::<Vec<_>>();
            self.dirs.insert(index, DirWorkspace::new(&entries));
        }
        Ok(())
    }

    pub(in crate) fn release_dir(&mut self, _index: u64) -> DenebResult<()> {
        // Nothing needs to be done here.
        Ok(())
    }

    pub(in crate) fn read_dir(&self, index: u64) -> DenebResult<Vec<(PathBuf, u64, FileType)>> {
        self.dirs
            .get(&index)
            .map(DirWorkspace::entries_tuple)
            .ok_or_else(|| WorkspaceError::DirLookup(index).into())
    }

    // Note: We perform inefficient double lookups since Catalog::inode returns
    //       a Result and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    pub(in crate) fn open_file(&mut self, index: u64, _flags: u32) -> DenebResult<()> {
        if !self.files.contains_key(&index) {
            let store = Rc::clone(&self.store);
            let iws = self.inode_ws(index)?;
            let fws = FileWorkspace::try_new(iws.inode(), store, false)?;
            self.files.insert(index, fws);
        }
        Ok(())
    }

    pub(in crate) fn read_data(&self, index: u64, offset: i64, size: u32) -> DenebResult<Vec<u8>> {
        let offset = ::std::cmp::max(offset, 0) as usize;
        let ws = self
            .files
            .get(&index)
            .ok_or_else(|| WorkspaceError::FileLookup(index))?;
        ws.read_at(offset, size as usize)
    }

    pub(in crate) fn write_data(
        &mut self,
        index: u64,
        offset: i64,
        data: &[u8],
    ) -> DenebResult<u32> {
        let offset = ::std::cmp::max(offset, 0) as usize;
        let (written, new_size) = {
            let ws = self
                .files
                .get_mut(&index)
                .ok_or_else(|| WorkspaceError::FileLookup(index))?;
            ws.write_at(offset, data)
        };
        let ws = self.inode_ws_mut(index)?;
        ws.update_size(new_size);

        self.dirty = true;

        Ok(written)
    }

    pub(in crate) fn release_file(&mut self, index: u64) -> DenebResult<()> {
        let ws = self
            .files
            .get_mut(&index)
            .ok_or_else(|| WorkspaceError::FileLookup(index))?;
        ws.unload();
        Ok(())
    }

    pub(in crate) fn create_file(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _flags: u32,
    ) -> DenebResult<(u64, FileAttributes)> {
        let index = self.index_generator.next();

        // Create new inode
        let mut attributes = FileAttributes::default();
        attributes.index = index;
        let ts = now_utc().to_timespec();
        attributes.atime = ts;
        attributes.mtime = ts;
        attributes.ctime = ts;
        attributes.crtime = ts;
        attributes.perm = mode_to_permissions(mode as mode_t);
        attributes.nlink = 1;
        attributes.uid = get_euid();
        attributes.gid = get_egid();
        let kind = attributes.kind;
        let inode = INode::new(attributes, vec![]);
        let ws = FileWorkspace::try_new(&inode, Rc::clone(&self.store), true)?;
        self.inodes.insert(index, INodeWorkspace::new(inode, true));

        // Create new file workspace
        self.files.insert(index, ws);

        // Update the parent directory workspace
        self.open_dir(parent)?;

        if let Some(ws) = self.dirs.get_mut(&parent) {
            ws.add_entry(index, PathBuf::from(name), kind);
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }

        self.dirty = true;

        Ok((index, attributes))
    }

    pub(in crate) fn create_dir(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
    ) -> DenebResult<FileAttributes> {
        let index = self.index_generator.next();

        // Create new inode
        let mut attributes = FileAttributes::default();
        attributes.index = index;
        let ts = now_utc().to_timespec();
        attributes.atime = ts;
        attributes.mtime = ts;
        attributes.ctime = ts;
        attributes.crtime = ts;
        attributes.kind = FileType::Directory;
        attributes.perm = mode_to_permissions(mode as mode_t);
        attributes.nlink = 1;
        attributes.uid = get_euid();
        attributes.gid = get_egid();
        let inode = INode::new(attributes, vec![]);
        self.inodes
            .insert(index, INodeWorkspace::new(inode.clone(), true));

        // Create new dir workspace
        let mut ws = DirWorkspace::new(&[]);
        ws.add_entry(index, PathBuf::from("."), FileType::Directory);
        ws.add_entry(parent, PathBuf::from(".."), FileType::Directory);
        self.dirs.insert(index, ws);

        // Update the parent directory workspace
        self.open_dir(parent)?;

        if let Some(ws) = self.dirs.get_mut(&parent) {
            ws.add_entry(index, PathBuf::from(name), inode.attributes.kind);
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }

        self.dirty = true;

        Ok(attributes)
    }

    pub(in crate) fn remove(&mut self, parent: u64, name: &OsStr) -> DenebResult<()> {
        self.open_dir(parent)?;
        if let Some(ws) = self.dirs.get_mut(&parent) {
            let pname = PathBuf::from(name);
            let index = ws
                .entry_index(&pname)
                .ok_or_else(|| DirWorkspaceEntryLookupError {
                    parent,
                    name: name.to_owned(),
                })?;
            self.deleted_inodes.insert(index);
            ws.remove_entry(&PathBuf::from(name));
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }

        self.dirty = true;

        Ok(())
    }

    // Note: this implementation isn't atomic
    pub(in crate) fn rename(
        &mut self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> DenebResult<()> {
        self.open_dir(parent).context(EngineError::Rename(
            parent,
            name.to_owned(),
            new_parent,
            new_name.to_owned(),
        ))?;
        self.open_dir(new_parent).context(EngineError::Rename(
            parent,
            name.to_owned(),
            new_parent,
            new_name.to_owned(),
        ))?;

        let src_entry = if let Some(ws) = self.dirs.get_mut(&parent) {
            let pname = PathBuf::from(name);
            let entry = ws
                .entry(&pname)
                .cloned()
                .ok_or_else(|| DirWorkspaceEntryLookupError {
                    parent,
                    name: name.to_owned(),
                })?;
            ws.remove_entry(&pname);
            entry
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        };

        let new_name = PathBuf::from(new_name);

        let old_entry_type = self
            .dirs
            .get(&new_parent)
            .and_then(|ws| ws.entry(&new_name))
            .map(|&DirEntry { entry_type, .. }| entry_type);

        if let Some(entry_type) = old_entry_type {
            if entry_type == FileType::RegularFile {
                self.remove(new_parent, new_name.as_os_str())?;
            } else {
                panic!(
                    "Entry {:?} has unsupported file type {:?}",
                    name, old_entry_type
                );
            }
        }

        let ws = self
            .dirs
            .get_mut(&new_parent)
            .ok_or_else(|| WorkspaceError::DirLookup(new_parent))?;
        ws.add_entry(src_entry.index, new_name.clone(), src_entry.entry_type);

        self.dirty = true;

        Ok(())
    }

    pub(in crate) fn commit(&mut self) -> DenebResult<CommitSummary> {
        match commit_workspace(self) {
            Ok(summary) => Ok(summary),
            Err(e) => {
                error!("Error encountered during commit: {}", e);
                Err(e)
            }
        }
    }

    // Note: We perform inefficient double lookups since Catalog::inode returns a Result
    //       and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    fn inode_ws(&mut self, index: u64) -> DenebResult<&INodeWorkspace> {
        if !self.inodes.contains_key(&index) {
            let inode = self.catalog.inode(index)?;
            self.inodes.insert(index, INodeWorkspace::new(inode, false));
        }
        self.inodes
            .get(&index)
            .ok_or_else(|| WorkspaceError::INodeLookup(index).into())
    }

    // Note: We perform inefficient double lookups since Catalog::inode returns a Result
    //       and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    fn inode_ws_mut(&mut self, index: u64) -> DenebResult<&mut INodeWorkspace> {
        if !self.inodes.contains_key(&index) {
            let inode = self.catalog.inode(index)?;
            self.inodes.insert(index, INodeWorkspace::new(inode, false));
        }
        self.inodes
            .get_mut(&index)
            .ok_or_else(|| WorkspaceError::INodeLookup(index).into())
    }
}

fn init(
    store: &mut dyn Store,
    catalog_type: CatalogType,
    catalog_path: &Path,
    manifest_path: &Path,
    sync_dir: &Path,
    chunk_size: usize,
) -> DenebResult<()> {
    let mut catalog = open_catalog(catalog_type, catalog_path, true)?;
    populate_with_dir(&mut *catalog, store, sync_dir, chunk_size)?;
    info!("Catalog populated with contents of {:?}", sync_dir);

    // Save the generated catalog as a content-addressed chunk in the store.
    let mut f = File::open(catalog_path)?;
    let chunk_descriptor = store.put_file(&mut f)?;

    // Create and save the repository manifest
    let manifest = Manifest::new(chunk_descriptor.digest, now_utc()).serialize()?;
    store.write_special_file(&manifest_path, &mut &manifest[..], false)?;

    Ok(())
}
