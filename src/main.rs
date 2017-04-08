extern crate deneb;
#[macro_use]
extern crate log;

use deneb::catalog::Catalog;
use deneb::errors::*;
use deneb::logging;
use deneb::params::AppParameters;

fn run() -> Result<()> {
    logging::init().chain_err(|| "Could not initialize log4rs")?;
    info!("Welcome to Deneb!");

    let AppParameters { sync_dir, work_dir } = AppParameters::read()
        .chain_err(|| "Could not read command-line parameters")?;
    info!("Sync dir: {:?}", sync_dir);
    info!("Work dir: {:?}", work_dir);

    let catalog = Catalog::from_dir(sync_dir.as_path())?;
    info!("Catalog populated with initial contents.");
    catalog.show();

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
