use {log::LevelFilter, structopt::StructOpt, toml};

use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use deneb_core::errors::{DenebError, DenebResult};

#[derive(Debug, StructOpt)]
#[structopt(about = "Flew into the light of Deneb")]
pub(super) struct CommandLine {
    #[structopt(
        short = "n",
        long = "instance_name",
        default_value = "main",
        help = "Name of the Deneb instance"
    )]
    pub instance_name: String,
    #[structopt(
        short = "c",
        long = "config_dir",
        parse(from_os_str),
        help = "Configuration directory"
    )]
    pub config_dir: Option<PathBuf>,
    #[structopt(
        short = "m",
        long = "mount_point",
        parse(from_os_str),
        help = "Location where the file system is mounted"
    )]
    pub(super) mount_point: Option<PathBuf>,
    #[structopt(
        short = "l",
        long = "log_level",
        parse(try_from_str = "parse_log_level_str"),
        help = "Logging level (off|error|warn|info|debug|trace)"
    )]
    pub log_level: Option<LevelFilter>,
    #[structopt(long = "chunk_size", help = "Default chunk size for storing files")]
    pub chunk_size: Option<usize>,
    #[structopt(
        short = "s",
        long = "sync_dir",
        parse(from_os_str),
        help = "Populate the repository with the contents of this directory"
    )]
    pub sync_dir: Option<PathBuf>,
    #[structopt(
        short = "f",
        long = "force_unmount",
        help = "Force unmount the file system on exit"
    )]
    pub force_unmount: bool,
    #[structopt(long = "foreground", help = "Stay in the foreground, don't fork")]
    pub foreground: bool,
}

impl CommandLine {
    pub(super) fn read() -> CommandLine {
        CommandLine::from_args()
    }
}

#[derive(Deserialize, Serialize)]
pub(super) struct ConfigFile {
    pub(super) mount_point: Option<PathBuf>,
    pub(super) log_level: Option<LevelFilter>,
    pub(super) chunk_size: Option<usize>,
}

impl ConfigFile {
    pub(super) fn load<P: AsRef<Path>>(file_name: P) -> DenebResult<ConfigFile> {
        let cfg = if file_name.as_ref().exists() {
            let mut f = File::open(file_name)?;
            let mut contents = String::new();
            f.read_to_string(&mut contents)?;
            toml::from_str(&contents)?
        } else {
            ConfigFile {
                mount_point: None,
                log_level: None,
                chunk_size: None,
            }
        };
        Ok(cfg)
    }

    pub(super) fn save<P: AsRef<Path>>(&self, file_name: P) -> DenebResult<()> {
        let new_cfg_file = toml::to_string(&self)?;
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_name)?;
        f.write_all(new_cfg_file.as_bytes())?;
        Ok(())
    }
}

pub(super) fn parse_log_level_str(s: &str) -> Result<LevelFilter, DenebError> {
    match s {
        "off" => Ok(LevelFilter::Off),
        "error" => Ok(LevelFilter::Error),
        "warn" => Ok(LevelFilter::Warn),
        "info" => Ok(LevelFilter::Info),
        "debug" => Ok(LevelFilter::Debug),
        "trace" => Ok(LevelFilter::Trace),
        _ => Err(DenebError::CommandLineParameter(
            "log_level: ".to_string() + s,
        )),
    }
}
