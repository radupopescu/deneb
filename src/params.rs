use clap::{App, Arg};

use std::path::PathBuf;

use errors::*;

pub struct AppParameters {
    pub sync_dir: PathBuf,
    pub work_dir: PathBuf,
    pub mount_point: PathBuf,
}

impl AppParameters {
    pub fn read() -> Result<AppParameters> {
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
            .arg(Arg::with_name("mount_point")
                .short("m")
                .long("mount_point")
                .takes_value(true)
                .value_name("MOUNT_POINT")
                .required(true)
                .help("Mount point"))
            .get_matches();

        let sync_dir = PathBuf::from(matches.value_of("sync_dir")
            .map(|d| d.to_string())
            .ok_or(ErrorKind::MissingCommandLineParameter("sync_dir".to_owned()))?);
        let work_dir = PathBuf::from(matches.value_of("work_dir")
            .map(|d| d.to_string())
            .ok_or(ErrorKind::MissingCommandLineParameter("sync_dir".to_owned()))?);
        let mount_point = PathBuf::from(matches.value_of("mount_point")
            .map(|d| d.to_string())
            .ok_or(ErrorKind::MissingCommandLineParameter("mount_point".to_owned()))?);

        Ok(AppParameters {
            sync_dir: sync_dir,
            work_dir: work_dir,
            mount_point: mount_point,
        })
    }
}
