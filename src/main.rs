extern crate deneb;
extern crate merkle;
#[macro_use]
extern crate log;

use deneb::errors::*;
use deneb::logging;

fn run() -> Result<()> {
    logging::init().chain_err(|| "Could not initialize log4rs")?;
    info!("Welcome to Deneb!");

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
    let Parameters { dir } = read_params()?;
    info!("Dir: {}", dir);

