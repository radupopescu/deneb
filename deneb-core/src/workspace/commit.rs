use {
    super::{Workspace, MANIFEST_PATH, REFLOG_PATH},
    crate::{
        errors::DenebResult, inode::ChunkDescriptor, workspace::inode::Workspace as INodeWorkspace,
    },
    log::debug,
    std::{
        collections::HashMap,
        fmt::{Display, Formatter, Result as FmtResult},
        fs::File,
        io::Write,
        path::PathBuf,
    },
    time::now_utc,
};

#[derive(Debug)]
pub(in crate) struct Summary {
    inodes_deleted: usize,
    inodes_updated: usize,
    files_written: usize,
    chunks_written: usize,
    dir_entries_added: usize,
    new_root_hash: Option<String>,
}

impl Summary {
    fn new() -> Summary {
        Summary::default()
    }
}

impl Default for Summary {
    fn default() -> Summary {
        Summary {
            inodes_deleted: 0,
            inodes_updated: 0,
            files_written: 0,
            chunks_written: 0,
            dir_entries_added: 0,
            new_root_hash: None,
        }
    }
}

impl Display for Summary {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        write!(f, "{:#?}", self)
    }
}

struct Updates {
    delete_indices: Vec<u64>,
    new_chunks: HashMap<u64, (u64, Vec<ChunkDescriptor>)>,
}

pub(super) fn commit_workspace(ws: &mut Workspace) -> DenebResult<Summary> {
    let mut summary = Summary::new();

    if !ws.dirty {
        debug!("Workspace is not dirty. Nothing to commit.");
        return Ok(summary);
    }

    prune_inodes(ws, &mut summary)?;

    let updates = write_file_data(ws, &mut summary)?;
    update_chunks(ws, &updates.new_chunks, &mut summary)?;

    write_inodes(ws, &mut summary)?;
    write_dirs(ws, &mut summary)?;

    finalize(ws, &mut summary)?;

    cleanup_workspace(ws, &updates.delete_indices)?;

    Ok(summary)
}

/// Iterate through the list of deleted inodes and:
/// - remove the corresponding file, directory and inode workspaces
/// - update any dir workspaces that had the inodes as children
/// - remove the inode entries from the catalog
fn prune_inodes(ws: &mut Workspace, summary: &mut Summary) -> DenebResult<()> {
    for idx in &ws.deleted_inodes {
        ws.files.remove(idx);
        ws.dirs.remove(idx);
        ws.inodes.remove(idx);
        ws.dirs.iter_mut().for_each(|(_, dws)| {
            dws.remove_entry_idx(*idx);
        });
        ws.catalog.remove_inode(*idx)?;
    }
    summary.inodes_deleted = ws.deleted_inodes.len();
    Ok(())
}

/// Iterate over the remaining dirty file workspaces and write them to the
/// store. For each file, the resulting chunks should be associated with the
/// inode workspace. Once written, the file workspace should be deleted, as
/// it needs to be rebuilt with the new chunks as lower level
fn write_file_data(ws: &mut Workspace, summary: &mut Summary) -> DenebResult<Updates> {
    let mut delete_indices = Vec::new();
    let mut new_chunks = HashMap::new();
    let mut store = ws.store.borrow_mut();
    for (idx, fws) in &ws.files {
        if fws.dirty {
            let mut rdr = fws.reader();
            let chunks = store.put_file_chunked(&mut rdr)?;
            summary.chunks_written += chunks.len();
            new_chunks.insert(*idx, (fws.size, chunks));
            delete_indices.push(*idx);
            summary.files_written += 1;
        }
    }

    Ok(Updates {
        delete_indices,
        new_chunks,
    })
}

fn update_chunks(
    ws: &mut Workspace,
    new_chunks: &HashMap<u64, (u64, Vec<ChunkDescriptor>)>,
    _summary: &mut Summary,
) -> DenebResult<()> {
    for (idx, (file_size, chunks)) in new_chunks {
        let mut inode = ws.inode_ws(*idx)?.inode().clone();
        inode.attributes.size = *file_size;
        inode.chunks = chunks.clone();
        ws.inodes.insert(*idx, INodeWorkspace::new(inode, true));
    }
    Ok(())
}

// Write directory workspaces to the catalog
fn write_dirs(ws: &mut Workspace, summary: &mut Summary) -> DenebResult<()> {
    for (idx, dws) in &ws.dirs {
        if dws.dirty {
            for (name, entry_index, _) in dws.entries_tuple() {
                ws.catalog.add_dir_entry(*idx, &name, entry_index)?;
                summary.dir_entries_added += 1;
            }
        }
    }
    Ok(())
}

// Write inode workspaces to the catalog
fn write_inodes(ws: &mut Workspace, summary: &mut Summary) -> DenebResult<()> {
    for iws in ws.inodes.values() {
        if iws.dirty {
            ws.catalog.add_inode(iws.inode())?;
            summary.inodes_updated += 1;
        }
    }
    Ok(())
}

// Finalize commit: write the new catalog into storage, write the old root hash
// to the reflog, write the new manifest
fn finalize(ws: &mut Workspace, summary: &mut Summary) -> DenebResult<()> {
    let mut store = ws.store.borrow_mut();

    // Save the generated catalog as a content-addressed chunk in the store.
    let catalog_path = ws.work_dir.join("scratch/current_catalog");
    let mut f = File::open(catalog_path.as_path())?;

    let chunk_descriptor = store.put_file(&mut f)?;

    // Write the old root hash to the reflog
    let ref_log_path = PathBuf::from(REFLOG_PATH);
    let mut ref_log = Vec::new();
    writeln!(ref_log, "{}", ws.manifest.root_hash)?;
    store.write_special_file(&ref_log_path, &mut (&ref_log[..]), true)?;

    // Create and save the repository manifest
    ws.manifest.root_hash = chunk_descriptor.digest;
    ws.manifest.timestamp = now_utc();
    let manifest_path = ws.work_dir.join(MANIFEST_PATH);
    let buf = ws.manifest.serialize()?;
    store.write_special_file(&manifest_path, &mut (&buf[..]), false)?;

    summary.new_root_hash = Some(ws.manifest.root_hash.to_string());

    Ok(())
}

fn cleanup_workspace(ws: &mut Workspace, delete_idx: &[u64]) -> DenebResult<()> {
    for idx in delete_idx {
        ws.files.remove(&idx);
    }
    ws.deleted_inodes.clear();
    ws.dirty = false;
    Ok(())
}
