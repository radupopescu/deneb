extern crate deneb;
extern crate merkle;
#[macro_use]
extern crate log;

use std::fs::{read_dir, DirEntry};
use std::path::Path;

use deneb::errors::*;
use deneb::logging;
use deneb::params::{read_params, Parameters};

// one possible implementation of walking a directory only visiting files
fn visit_dirs(dir: &Path, cb: &Fn(&DirEntry)) -> Result<()> {
    if dir.is_dir() {
        for entry in read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(&entry);
            }
        }
    }
    Ok(())
}

fn list_info(entry: &DirEntry) {
    if let Ok(metadata) = entry.metadata() {
        info!("Path: {:?}; metadata: {:?}", entry.path(), metadata);
    }
}

fn run() -> Result<()> {
    logging::init().chain_err(|| "Could not initialize log4rs")?;
    info!("Welcome to Deneb!");

    let Parameters { dir } = read_params()?;
    info!("Dir: {}", dir);
    let _ = visit_dirs(Path::new(dir.as_str()), &list_info)?;

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
