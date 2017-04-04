use std::fs::{read_dir, DirEntry};
use std::path::Path;

use errors::*;

pub fn visit_dirs(dir: &Path, cb: &mut FnMut(&DirEntry) -> Result<()>) -> Result<()> {
    if dir.is_dir() {
        for entry in read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            cb(&entry)?;
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            }
        }
    }
    Ok(())
}
