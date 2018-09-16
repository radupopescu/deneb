use {directories::ProjectDirs, dirs::home_dir, failure::err_msg, toml};

use std::{fs::File, io::Read, path::PathBuf};

use deneb_core::errors::DenebResult;

mod params;

pub use self::params::*;

pub struct App {
    pub parameters: Parameters,
    pub directories: Directories,
}

impl App {
    pub fn init() -> DenebResult<App> {
        // Read application parameters, configure directories etc.
        let mut parameters = Parameters::read();

        let mut directories = Directories::with_name(&parameters.instance_name)?;

        if let Some(config_dir) = parameters.config_dir.clone() {
            directories.config = config_dir;
        }

        let config_file = directories.config.join("config.toml");

        if config_file.as_path().exists() {
            let mut f = File::open(config_file)?;
            let mut contents = String::new();
            f.read_to_string(&mut contents)?;
            let cfg: ConfigFileParameters = toml::from_str(&contents)?;
            parameters.mount_point = cfg.mount_point;
            parameters.log_level = cfg.log_level;
            parameters.chunk_size = cfg.chunk_size;
        }

        if let Some(mount_point) = parameters.mount_point.clone() {
            directories.mount_point = mount_point;
        }

        // Create all dirs

        // Save new config file§

        Ok(App {
            parameters,
            directories,
        })
    }
}

pub struct Directories {
    pub workspace: PathBuf,
    pub config: PathBuf,
    pub log: PathBuf,
    pub mount_point: PathBuf,
}

impl Directories {
    pub fn with_name(instance_name: &str) -> DenebResult<Directories> {
        let dirs = ProjectDirs::from(qualifier(), organization(), application())
            .ok_or(err_msg("Unable to create application directories."))?;

        let mount_point = home_dir().ok_or(err_msg("Unable to obtain home directory."))?;

        let directories = Directories {
            workspace: dirs.data_dir().join(instance_name),
            config: dirs.config_dir().join(instance_name),
            log: dirs.data_dir().join(instance_name),
            mount_point,
        };

        Ok(directories)
    }
}

fn qualifier() -> &'static str {
    "org"
}

fn organization() -> &'static str {
    "Radu Popescu"
}

fn application() -> &'static str {
    "Deneb"
}
