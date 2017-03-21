use clap::{App, Arg};

use std::path::PathBuf;

use errors::*;

pub struct Parameters {
    pub dir: PathBuf,
}

pub fn read_params() -> Result<Parameters> {
    let matches = App::new("Deneb")
        .version("0.1.0")
        .author("Radu Popescu <mail@radupopescu.net>")
        .about("Flew into the light of Deneb")
        .arg(Arg::with_name("dir")
            .short("d")
            .long("dir")
            .takes_value(true)
            .value_name("DIR")
            .required(true)
            .help("Work directory"))
        .get_matches();

    let dir = PathBuf::from(matches
                           .value_of("dir")
                           .map(|d| d.to_string())
                           .ok_or(ErrorKind::MissingCommandLineParameter("dir".to_owned()))?);

    Ok(Parameters { dir: dir })
}
