use clap::{App, Arg};

use std::path::PathBuf;

use common::errors::*;

const DEFAULT_CHUNK_SIZE: u64 = 4194304; // 4MB default

pub struct Params {
    pub sync_dir: PathBuf,
    pub work_dir: PathBuf,
    pub chunk_size: u64,
}

impl Params {
    pub fn read() -> Result<Params> {
        let matches = App::new("Deneb")
            .version("0.1.0")
            .author("Radu Popescu <mail@radupopescu.net>")
            .about("Flew into the light of Deneb")
            .arg(Arg::with_name("sync_dir")
                     .short("s")
                     .long("sync_dir")
                     .takes_value(true)
                     .value_name("SYNC_DIR")
                     .required(true)
                     .help("Synced directory"))
            .arg(Arg::with_name("work_dir")
                     .short("w")
                     .long("work_dir")
                     .takes_value(true)
                     .value_name("WORK_DIR")
                     .required(true)
                     .help("Work (scratch) directory"))
            .arg(Arg::with_name("chunk_size")
                 .long("chunk_size")
                 .takes_value(true)
                 .value_name("CHUNK_SIZE")
                 .required(false)
                 .default_value("DEFAULT")//DEFAULT_CHUNK_SIZE) // default 4MB chunks
                 .help("Chunk size used for storing files"))
            .get_matches();

        let sync_dir =
            PathBuf::from(matches
                              .value_of("sync_dir")
                              .map(|d| d.to_string())
                              .ok_or_else(|| {
                                              ErrorKind::CommandLineParameter("sync_dir missing"
                                                                                  .to_owned())
                                          })?);
        let work_dir =
            PathBuf::from(matches
                              .value_of("work_dir")
                              .map(|d| d.to_string())
                              .ok_or_else(|| {
                                              ErrorKind::CommandLineParameter("work_dir missing"
                                                                                  .to_owned())
                                          })?);
        let chunk_size = match matches.value_of("chunk_size") {
            Some("DEFAULT") | None => DEFAULT_CHUNK_SIZE,
            Some(chunk_size) => {
                match u64::from_str_radix(chunk_size, 10) {
                    Ok(size) => size,
                    _ => DEFAULT_CHUNK_SIZE,
                }
            }
        };


        Ok(Params {
               sync_dir: sync_dir,
               work_dir: work_dir,
               chunk_size: chunk_size,
           })
    }
}
