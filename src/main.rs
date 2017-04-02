extern crate deneb;
#[macro_use]
extern crate log;
extern crate merkle;
extern crate nix;

use deneb::errors::*;
use deneb::fs;
use deneb::logging;
use deneb::params::AppParameters;

fn run() -> Result<()> {
    logging::init().chain_err(|| "Could not initialize log4rs")?;
    info!("Welcome to Deneb!");

    let AppParameters { sync_dir, work_dir } = AppParameters::read()
        .chain_err(|| "Could not read command-line parameters")?;
    info!("Sync dir: {:?}", sync_dir);
    info!("Work dir: {:?}", work_dir);

    let _ = fs::visit_dirs(sync_dir.as_path(), &fs::list_info)?;
    let _ = fs::visit_dirs(work_dir.as_path(), &fs::list_info)?;

    Ok(())
}

fn main() {
    if let Err(ref e) = run() {
        error!("error: {}", e);

        for e in e.iter().skip(1) {
            error!("caused by: {}", e);
        }

        if let Some(bt) = e.backtrace() {
            error!("Backtrace: {:?}", bt);
        }

        ::std::process::exit(1)
    }
}
