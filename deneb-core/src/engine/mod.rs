use crossbeam_channel::bounded as channel;
use failure::{Error, ResultExt};
use nix::libc::mode_t;
use time::now_utc;

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs::{create_dir_all, File},
    path::{Path, PathBuf},
    rc::Rc,
    thread::spawn as tspawn,
    time::Duration,
};

use crate::catalog::{open_catalog, Catalog, CatalogType, IndexGenerator};
use crate::dir_workspace::{DirEntry, DirWorkspace};
use crate::errors::{DenebResult, DirWorkspaceEntryLookupError, EngineError, WorkspaceError};
use crate::file_workspace::FileWorkspace;
use crate::inode::{mode_to_permissions, FileAttributeChanges, FileAttributes, FileType, INode};
use crate::manifest::Manifest;
use crate::populate_with_dir;
use crate::store::{open_store, Store, StoreType};
use crate::util::{atomic_write, get_egid, get_euid};

mod handle;
mod protocol;
mod requests;
mod timer;

use self::{
    protocol::{HandlerProxy, Request, RequestHandler},
    requests::{
        CreateDir, CreateFile, GetAttr, Lookup, OpenDir, OpenFile, Ping, ReadData, ReadDir,
        ReleaseDir, ReleaseFile, RemoveDir, Rename, SetAttr, StopEngine, TryCommit, Unlink,
        WriteData,
    },
    timer::{Resolution, Timer},
};

pub use self::{handle::Handle, requests::RequestId};

/// Start engine with pre-built catalog and store
pub fn start_engine_prebuilt(
    catalog: Box<dyn Catalog>,
    store: Box<dyn Store>,
    queue_size: usize,
) -> DenebResult<Handle> {
    let (cmd_tx, cmd_rx) = channel(queue_size);
    let (quit_tx, quit_rx) = channel(1);
    let index_generator = IndexGenerator::starting_at(catalog.get_max_index())?;
    let engine_hd = Handle::new(cmd_tx, quit_rx);
    let timer_engine_hd = engine_hd.clone();
    let _ = tspawn(move || {
        let mut engine = Engine {
            catalog,
            store: Rc::new(RefCell::new(store)),
            workspace: Workspace::new(),
            index_generator,
            stopped: false,
        };
        let mut timer = Timer::new(Resolution::Second);
        timer.schedule(Duration::from_secs(5), true, move || {
            timer_engine_hd.try_commit();
        });
        info!("Starting engine event loop");
        for request in &cmd_rx {
            request.run_handler(&mut engine);
            if engine.stopped {
                break;
            }
        }
        info!("Engine event loop finished.");
        timer.stop();
        quit_tx.send(()).map_err(|_| EngineError::Send).unwrap();
    });

    engine_hd.ping();

    Ok(engine_hd)
}

/// Start the engine using catalog and store builders
pub fn start_engine(
    catalog_type: CatalogType,
    store_type: StoreType,
    work_dir: &Path,
    sync_dir: Option<PathBuf>,
    chunk_size: usize,
    queue_size: usize,
) -> DenebResult<Handle> {
    let (catalog, store) = init(catalog_type, store_type, work_dir, sync_dir, chunk_size)?;

    start_engine_prebuilt(catalog, store, queue_size)
}

fn init(
    catalog_type: CatalogType,
    store_type: StoreType,
    work_dir: &Path,
    sync_dir: Option<PathBuf>,
    chunk_size: usize,
) -> DenebResult<(Box<dyn Catalog>, Box<dyn Store>)> {
    // Create an object store
    let mut store = open_store(store_type, work_dir, chunk_size)?;

    let catalog_root = work_dir.to_path_buf().join("scratch");
    create_dir_all(catalog_root.as_path())?;
    let catalog_path = catalog_root.join("current_catalog");
    info!("Catalog path: {:?}", catalog_path);

    let manifest_path = work_dir.to_path_buf().join("manifest");
    info!("Manifest path: {:?}", manifest_path);

    // Create the file metadata catalog and populate it with the contents of "sync_dir"
    if let Some(sync_dir) = sync_dir {
        {
            //use std::ops::DerefMut;
            let mut catalog = open_catalog(catalog_type, catalog_path.as_path(), true)?;
            populate_with_dir(&mut *catalog, &mut *store, sync_dir.as_path(), chunk_size)?;
            info!(
                "Catalog populated with contents of {:?}",
                sync_dir.as_path()
            );
        }

        // Save the generated catalog as a content-addressed chunk in the store.
        let mut f = File::open(catalog_path.as_path())?;
        let chunk_descriptor = store.put_file(&mut f)?;

        // Create and save the repository manifest
        let manifest = Manifest::new(chunk_descriptor.digest, None, now_utc());
        manifest.save(manifest_path.as_path())?;
    }

    // Load the repository manifest
    let manifest = Manifest::load(manifest_path.as_path())?;

    // Get the catalog out of storage and open it
    {
        let root_hash = manifest.root_hash;
        let chunk = store.get_chunk(&root_hash)?;
        atomic_write(catalog_path.as_path(), chunk.get_slice())?;
    }

    let catalog = open_catalog(catalog_type, catalog_path.as_path(), false)?;
    catalog.show_stats();

    Ok((catalog, store))
}

struct Workspace {
    dirs: HashMap<u64, DirWorkspace>,
    files: HashMap<u64, FileWorkspace>,
    inodes: HashMap<u64, INode>,
    deleted_inodes: HashSet<u64>,
}

impl Workspace {
    fn new() -> Workspace {
        Workspace {
            dirs: HashMap::new(),
            files: HashMap::new(),
            inodes: HashMap::new(),
            deleted_inodes: HashSet::new(),
        }
    }
}

pub(in crate::engine) struct Engine {
    catalog: Box<dyn Catalog>,
    store: Rc<RefCell<Box<dyn Store>>>,
    workspace: Workspace,
    index_generator: IndexGenerator,
    stopped: bool,
}

impl RequestHandler<GetAttr> for Engine {
    fn handle(&mut self, request: &GetAttr) -> DenebResult<<GetAttr as Request>::Reply> {
        self.get_attr(request.index)
            .context(EngineError::GetAttr(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<SetAttr> for Engine {
    fn handle(&mut self, request: &SetAttr) -> DenebResult<<SetAttr as Request>::Reply> {
        self.set_attr(request.index, &request.changes)
            .context(EngineError::SetAttr(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<Lookup> for Engine {
    fn handle(&mut self, request: &Lookup) -> DenebResult<<Lookup as Request>::Reply> {
        self.lookup(request.parent, &request.name)
            .context(EngineError::Lookup(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<OpenDir> for Engine {
    fn handle(&mut self, request: &OpenDir) -> DenebResult<<OpenDir as Request>::Reply> {
        self.open_dir(request.index)
            .context(EngineError::DirOpen(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReleaseDir> for Engine {
    fn handle(&mut self, request: &ReleaseDir) -> DenebResult<<ReleaseDir as Request>::Reply> {
        self.release_dir(request.index)
            .context(EngineError::DirClose(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReadDir> for Engine {
    fn handle(&mut self, request: &ReadDir) -> DenebResult<<ReadDir as Request>::Reply> {
        self.read_dir(request.index)
            .context(EngineError::DirRead(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<OpenFile> for Engine {
    fn handle(&mut self, request: &OpenFile) -> DenebResult<<OpenFile as Request>::Reply> {
        self.open_file(request.index, request.flags)
            .context(EngineError::FileOpen(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReadData> for Engine {
    fn handle(&mut self, request: &ReadData) -> DenebResult<<ReadData as Request>::Reply> {
        self.read_data(request.index, request.offset, request.size)
            .context(EngineError::FileRead(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<WriteData> for Engine {
    fn handle(&mut self, request: &WriteData) -> DenebResult<<WriteData as Request>::Reply> {
        self.write_data(request.index, request.offset, &request.data)
            .context(EngineError::FileWrite(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<ReleaseFile> for Engine {
    fn handle(&mut self, request: &ReleaseFile) -> DenebResult<<ReleaseFile as Request>::Reply> {
        self.release_file(request.index)
            .context(EngineError::FileClose(request.index))
            .map_err(Error::from)
    }
}

impl RequestHandler<CreateFile> for Engine {
    fn handle(&mut self, request: &CreateFile) -> DenebResult<<CreateFile as Request>::Reply> {
        self.create_file(request.parent, &request.name, request.mode, request.flags)
            .context(EngineError::FileCreate(
                request.parent,
                request.name.clone(),
            ))
            .map_err(Error::from)
    }
}

impl RequestHandler<CreateDir> for Engine {
    fn handle(&mut self, request: &CreateDir) -> DenebResult<<CreateDir as Request>::Reply> {
        self.create_dir(request.parent, &request.name, request.mode)
            .context(EngineError::DirCreate(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<Unlink> for Engine {
    fn handle(&mut self, request: &Unlink) -> DenebResult<<Unlink as Request>::Reply> {
        self.remove(request.parent, &request.name)
            .context(EngineError::Unlink(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<RemoveDir> for Engine {
    fn handle(&mut self, request: &RemoveDir) -> DenebResult<<RemoveDir as Request>::Reply> {
        self.remove(request.parent, &request.name)
            .context(EngineError::RemoveDir(request.parent, request.name.clone()))
            .map_err(Error::from)
    }
}

impl RequestHandler<Rename> for Engine {
    fn handle(&mut self, request: &Rename) -> DenebResult<<Rename as Request>::Reply> {
        self.rename(
            request.parent,
            &request.name,
            request.new_parent,
            &request.new_name,
        )
        .context(EngineError::Rename(
            request.parent,
            request.name.clone(),
            request.new_parent,
            request.new_name.clone(),
        ))
        .map_err(Error::from)
    }
}

impl RequestHandler<TryCommit> for Engine {
    fn handle(&mut self, _request: &TryCommit) -> DenebResult<()> {
        trace!("Engine will commit workspace");
        Ok(())
    }
}

impl RequestHandler<Ping> for Engine {
    fn handle(&mut self, _request: &Ping) -> DenebResult<()> {
        debug!("Engine received ping request.");
        Ok(())
    }
}

impl RequestHandler<StopEngine> for Engine {
    fn handle(&mut self, _request: &StopEngine) -> DenebResult<()> {
        info!("StopEngine request received.");
        self.stop();
        Ok(())
    }
}

impl Engine {
    // Note: We perform inefficient double lookups since Catalog::get_inode returns a Result
    //       and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    fn get_inode(&mut self, index: u64) -> DenebResult<INode> {
        if !self.workspace.inodes.contains_key(&index) {
            let inode = self.catalog.get_inode(index)?;
            self.workspace.inodes.insert(index, inode);
        }
        self.workspace
            .inodes
            .get(&index)
            .cloned()
            .ok_or_else(|| WorkspaceError::INodeLookup(index).into())
    }

    fn update_inode(&mut self, index: u64, inode: &INode) -> DenebResult<()> {
        self.workspace.inodes.insert(index, inode.clone());
        Ok(())
    }

    fn get_attr(&mut self, index: u64) -> DenebResult<FileAttributes> {
        let inode = self.get_inode(index)?;
        Ok(inode.attributes)
    }

    fn set_attr(
        &mut self,
        index: u64,
        changes: &FileAttributeChanges,
    ) -> DenebResult<FileAttributes> {
        let mut inode = self.get_inode(index)?;
        inode.attributes.update(changes);
        let attrs = inode.attributes;
        self.update_inode(index, &inode)?;

        if let Some(new_size) = changes.size {
            if let Some(ref mut ws) = self.workspace.files.get_mut(&index) {
                ws.truncate(new_size);
            } else {
                return Err(WorkspaceError::FileLookup(index).into());
            }
        }
        Ok(attrs)
    }

    fn lookup(&mut self, parent: u64, name: &OsStr) -> DenebResult<Option<FileAttributes>> {
        let index = if let Some(ws) = self.workspace.dirs.get(&parent) {
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
    fn open_dir(&mut self, index: u64) -> DenebResult<()> {
        if !self.workspace.dirs.contains_key(&index) {
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
            self.workspace
                .dirs
                .insert(index, DirWorkspace::new(&entries));
        }
        Ok(())
    }

    fn release_dir(&mut self, _index: u64) -> DenebResult<()> {
        // Nothing needs to be done here.
        Ok(())
    }

    fn read_dir(&self, index: u64) -> DenebResult<Vec<(PathBuf, u64, FileType)>> {
        self.workspace
            .dirs
            .get(&index)
            .map(DirWorkspace::get_entries_tuple)
            .ok_or_else(|| WorkspaceError::DirLookup(index).into())
    }

    // Note: We perform inefficient double lookups since Catalog::get_inode returns
    //       a Result and can't be used inside Entry::or_insert_with
    #[allow(clippy::map_entry)]
    fn open_file(&mut self, index: u64, _flags: u32) -> DenebResult<()> {
        if !self.workspace.files.contains_key(&index) {
            let inode = self.get_inode(index)?;
            self.workspace
                .files
                .insert(index, FileWorkspace::try_new(&inode, &self.store)?);
        }
        Ok(())
    }

    fn read_data(&self, index: u64, offset: i64, size: u32) -> DenebResult<Vec<u8>> {
        let offset = ::std::cmp::max(offset, 0) as usize;
        let ws = self
            .workspace
            .files
            .get(&index)
            .ok_or_else(|| WorkspaceError::FileLookup(index))?;
        ws.read_at(offset, size as usize)
    }

    fn write_data(&mut self, index: u64, offset: i64, data: &[u8]) -> DenebResult<u32> {
        let offset = ::std::cmp::max(offset, 0) as usize;
        let (written, new_size) = {
            let ws = self
                .workspace
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

    fn release_file(&mut self, index: u64) -> DenebResult<()> {
        let ws = self
            .workspace
            .files
            .get_mut(&index)
            .ok_or_else(|| WorkspaceError::FileLookup(index))?;
        ws.unload();
        Ok(())
    }

    fn create_file(
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
        self.workspace.inodes.insert(index, inode.clone());

        // Create new file workspace
        let ws = FileWorkspace::try_new(&inode, &self.store)?;
        self.workspace.files.insert(index, ws);

        // Update the parent directory workspace
        self.open_dir(parent)?;

        if let Some(ws) = self.workspace.dirs.get_mut(&parent) {
            ws.add_entry(index, PathBuf::from(name), inode.attributes.kind);
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }

        Ok((index, attributes))
    }

    fn create_dir(&mut self, parent: u64, name: &OsStr, mode: u32) -> DenebResult<FileAttributes> {
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
        self.workspace.inodes.insert(index, inode.clone());

        // Create new dir workspace
        let mut ws = DirWorkspace::new(&[]);
        ws.add_entry(index, PathBuf::from("."), FileType::Directory);
        ws.add_entry(parent, PathBuf::from(".."), FileType::Directory);
        self.workspace.dirs.insert(index, ws);

        // Update the parent directory workspace
        self.open_dir(parent)?;

        if let Some(ws) = self.workspace.dirs.get_mut(&parent) {
            ws.add_entry(index, PathBuf::from(name), inode.attributes.kind);
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }

        Ok(attributes)
    }

    fn remove(&mut self, parent: u64, name: &OsStr) -> DenebResult<()> {
        self.open_dir(parent)?;
        if let Some(ws) = self.workspace.dirs.get_mut(&parent) {
            let pname = PathBuf::from(name);
            let index = ws
                .get_entry_index(&pname)
                .ok_or_else(|| DirWorkspaceEntryLookupError {
                    parent,
                    name: name.to_owned(),
                })?;
            self.workspace.deleted_inodes.insert(index);
            ws.remove_entry(&PathBuf::from(name));
        } else {
            return Err(WorkspaceError::DirLookup(parent).into());
        }
        Ok(())
    }

    // Note: this implementation isn't atomic
    fn rename(
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

        let src_entry = if let Some(ws) = self.workspace.dirs.get_mut(&parent) {
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
            .workspace
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
            .workspace
            .dirs
            .get_mut(&new_parent)
            .ok_or_else(|| WorkspaceError::DirLookup(new_parent))?;
        ws.add_entry(src_entry.index, new_name.clone(), src_entry.entry_type);

        Ok(())
    }

    fn stop(&mut self) {
        info!("Engine stopping...");
        self.stopped = true;
        info!("Engine stopped.");
    }
}
