use std::fs::{read_dir, DirEntry};
use std::path::Path;

use nix::sys::stat;

use errors::*;

pub fn visit_dirs(dir: &Path, cb: &Fn(&DirEntry) -> Result<()>) -> Result<()> {
    if dir.is_dir() {
        for entry in read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(&entry)?;
            }
        }
    }
    Ok(())
}

pub fn list_info(entry: &DirEntry) -> Result<()> {
    let stats = stat::stat(entry.path().as_path())?;
    let metadata = entry.metadata()?;
    info!("Path: {:?}, uid: {}, gid: {}, metadata: {:?}",
          entry.path(),
          stats.st_uid,
          stats.st_gid,
          metadata);
    Ok(())
}
