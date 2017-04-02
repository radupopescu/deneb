extern crate deneb;
#[macro_use]
extern crate log;
extern crate merkle;
extern crate nix;

use std::fs::{read_dir, DirEntry};
use std::path::Path;

use nix::sys::stat;

use deneb::errors::*;
use deneb::logging;
use deneb::params::AppParameters;

fn visit_dirs(dir: &Path, cb: &Fn(&DirEntry) -> Result<()>) -> Result<()> {
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

fn list_info(entry: &DirEntry) -> Result<()> {
    let stats = stat::stat(entry.path().as_path())?;
    let metadata = entry.metadata()?;
    info!("Path: {:?}, uid: {}, gid: {}, metadata: {:?}",
          entry.path(),
          stats.st_uid,
          stats.st_gid,
          metadata);
    Ok(())
}

fn run() -> Result<()> {
    logging::init().chain_err(|| "Could not initialize log4rs")?;
    info!("Welcome to Deneb!");

    let AppParameters { sync_dir, work_dir: _ } = AppParameters::read().chain_err(|| "Could not read command-line parameters")?;
    info!("Dir: {}", sync_dir.display());
    let _ = visit_dirs(sync_dir.as_path(), &list_info)?;

    Ok(())
}

fn main() {
    if let Err(ref e) = run() {
        error!("error: {}", e);

        for e in e.iter().skip(1) {
            error!("caused by: {}", e);
        }

        if let Some(backtrace) = e.backtrace() {
            error!("backtrace: {:?}", backtrace);
        }

        ::std::process::exit(1)
    }
}
