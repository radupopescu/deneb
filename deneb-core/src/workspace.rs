mod dir_workspace;
mod file_workspace;
pub(in crate) use dir_workspace::*;
pub(in crate) use file_workspace::*;

use {
    crate::{
        catalog::{Catalog, IndexGenerator},
        errors::{DenebResult, DirWorkspaceEntryLookupError, EngineError, WorkspaceError},
        inode::{mode_to_permissions, FileAttributeChanges, FileAttributes, FileType, INode},
        store::Store,
        util::{get_egid, get_euid},
    },
    failure::ResultExt,
    log::info,
    nix::libc::mode_t,
    std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        ffi::OsStr,
        path::PathBuf,
        rc::Rc,
    },
    time::now_utc,
};

pub(in crate) struct Workspace {
    catalog: Box<dyn Catalog>,
    store: Rc<RefCell<Box<dyn Store>>>,
    index_generator: IndexGenerator,
    dirs: HashMap<u64, DirWorkspace>,
    files: HashMap<u64, FileWorkspace>,
    inodes: HashMap<u64, INode>,
    deleted_inodes: HashSet<u64>,
}

impl Workspace {
    pub(in crate) fn new(
        catalog: Box<dyn Catalog>,
        store: Rc<RefCell<Box<dyn Store>>>,
    ) -> Workspace {
        let index_generator = IndexGenerator::starting_at(catalog.get_max_index());
        Workspace {
            catalog,
            store,
            index_generator,
            dirs: HashMap::new(),
            files: HashMap::new(),
            inodes: HashMap::new(),
            deleted_inodes: HashSet::new(),
        }
    }

    // Note: We perform inefficient double lookups since Catalog::get_inode returns a Result
    //       and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    pub(in crate) fn get_inode(&mut self, index: u64) -> DenebResult<INode> {
        if !self.inodes.contains_key(&index) {
            let inode = self.catalog.get_inode(index)?;
            self.inodes.insert(index, inode);
        }
        self.inodes
            .get(&index)
            .cloned()
            .ok_or_else(|| WorkspaceError::INodeLookup(index).into())
    }

    pub(in crate) fn update_inode(&mut self, index: u64, inode: &INode) -> DenebResult<()> {
        self.inodes.insert(index, inode.clone());
        Ok(())
    }

    pub(in crate) fn get_attr(&mut self, index: u64) -> DenebResult<FileAttributes> {
        let inode = self.get_inode(index)?;
        Ok(inode.attributes)
    }

    pub(in crate) fn set_attr(
        &mut self,
        index: u64,
        changes: &FileAttributeChanges,
    ) -> DenebResult<FileAttributes> {
        let mut inode = self.get_inode(index)?;
        inode.attributes.update(changes);
        let attrs = inode.attributes;
        self.update_inode(index, &inode)?;

        if let Some(new_size) = changes.size {
            if let Some(ref mut ws) = self.files.get_mut(&index) {
                ws.truncate(new_size);
            } else {
                return Err(WorkspaceError::FileLookup(index).into());
            }
        }
        Ok(attrs)
    }

    pub(in crate) fn lookup(&mut self, parent: u64, name: &OsStr) -> DenebResult<Option<FileAttributes>> {
        let index = if let Some(ws) = self.dirs.get(&parent) {
            ws.get_entries()
                .iter()
                .find(|DirEntry { name: ref n, .. }| n == &PathBuf::from(name))
                .map(|&DirEntry { index, .. }| index)
        } else {
            self.catalog
                .get_dir_entry_index(parent, PathBuf::from(name).as_path())?
        };
        if let Some(index) = index {
            self.get_attr(index).map(Some)
        } else {
            Ok(None)
        }
    }

    // Note: We perform inefficient double lookups since Catalog::get_dir_entries returns
    //       a Result and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    pub(in crate) fn open_dir(&mut self, index: u64) -> DenebResult<()> {
        if !self.dirs.contains_key(&index) {
            let entries = self
                .catalog
                .get_dir_entries(index)?
                .iter()
                .map(|&(ref name, idx)| {
                    if let Ok(inode) = self.get_inode(idx) {
                        DirEntry::new(idx, name.clone(), inode.attributes.kind)
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
            .map(DirWorkspace::get_entries_tuple)
            .ok_or_else(|| WorkspaceError::DirLookup(index).into())
    }

    // Note: We perform inefficient double lookups since Catalog::get_inode returns
    //       a Result and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    pub(in crate) fn open_file(&mut self, index: u64, _flags: u32) -> DenebResult<()> {
        if !self.files.contains_key(&index) {
            let inode = self.get_inode(index)?;
            self.files
                .insert(index, FileWorkspace::try_new(&inode, &self.store)?);
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

    pub(in crate) fn write_data(&mut self, index: u64, offset: i64, data: &[u8]) -> DenebResult<u32> {
        let offset = ::std::cmp::max(offset, 0) as usize;
        let (written, new_size) = {
            let ws = self
                .files
                .get_mut(&index)
                .ok_or_else(|| WorkspaceError::FileLookup(index))?;
            ws.write_at(offset, data)
        };
        let mut inode = self.get_inode(index)?;
        if inode.attributes.size != new_size {
            inode.attributes.size = new_size;
            self.update_inode(index, &inode)?;
        }
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
        let index = self.index_generator.get_next();

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
        let inode = INode::new(attributes, vec![]);
        self.inodes.insert(index, inode.clone());

        // Create new file workspace
        let ws = FileWorkspace::try_new(&inode, &self.store)?;
        self.files.insert(index, ws);

        // Update the parent directory workspace
        self.open_dir(parent)?;

        if let Some(ws) = self.dirs.get_mut(&parent) {
            ws.add_entry(index, PathBuf::from(name), inode.attributes.kind);
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }

        Ok((index, attributes))
    }

    pub(in crate) fn create_dir(&mut self, parent: u64, name: &OsStr, mode: u32) -> DenebResult<FileAttributes> {
        let index = self.index_generator.get_next();

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
        self.inodes.insert(index, inode.clone());

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

        Ok(attributes)
    }

    pub(in crate) fn remove(&mut self, parent: u64, name: &OsStr) -> DenebResult<()> {
        self.open_dir(parent)?;
        if let Some(ws) = self.dirs.get_mut(&parent) {
            let pname = PathBuf::from(name);
            let index = ws
                .get_entry_index(&pname)
                .ok_or_else(|| DirWorkspaceEntryLookupError {
                    parent,
                    name: name.to_owned(),
                })?;
            self.deleted_inodes.insert(index);
            ws.remove_entry(&PathBuf::from(name));
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }
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
            let entry =
                ws.get_entry(&pname)
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
            .and_then(|ws| ws.get_entry(&new_name))
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

        Ok(())
    }

    pub(in crate) fn commit(&mut self) -> DenebResult<String> {
        info!("Committing the current state of the workspace.");
        Ok("Commit finished.".to_string())
    }
}
