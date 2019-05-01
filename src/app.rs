use {
    self::config::{CommandLine, ConfigFile},
    deneb_core::errors::DenebResult,
    directories::ProjectDirs,
    dirs::home_dir,
    failure::err_msg,
    log::{info, LevelFilter},
    std::{fs::create_dir_all, path::PathBuf},
};

mod config;

const DEFAULT_LOG_LEVEL: LevelFilter = LevelFilter::Info;
const DEFAULT_CHUNK_SIZE: usize = 4_194_304;
const DEFAULT_AUTO_COMMIT_INTERVAL: usize = 5; // 5 sec interval

pub struct App {
    pub settings: Settings,
    pub directories: Directories,
}

impl App {
    pub fn init() -> DenebResult<App> {
        // Read application parameters, configure directories etc.

        let mut cmd_line = CommandLine::read();
        let mut directories = Directories::with_name(&cmd_line.instance_name)?;

        if let Some(config_dir) = cmd_line.config_dir.clone() {
            directories.config = config_dir;
            create_dir_all(&directories.config)?;
        }

        let config_file_name = directories.config.join("config.toml");
        let mut cfg_file = ConfigFile::load(&config_file_name)?;

        let settings = Settings::merge(&mut cmd_line, &mut cfg_file, &mut directories);

        // Create all dirs
        directories.ensure_created()?;

        // Save new config file
        cfg_file.save(&config_file_name)?;

        Ok(App {
            settings,
            directories,
        })
    }

    pub fn print_settings(&self) {
        info!("Log level: {}", self.settings.log_level);
        info!("Work dir: {:?}", self.directories.workspace);
        info!("Mount point: {:?}", self.directories.mount_point);
        info!("Chunk size: {:?}", self.settings.chunk_size);
        info!("Sync dir: {:?}", self.settings.sync_dir);
        info!("Force unmount: {}", self.settings.force_unmount);
        if self.settings.auto_commit_interval > 0 {
            info!(
                "Auto commit interval: {}",
                self.settings.auto_commit_interval
            );
        } else {
            info!("Auto commit disabled");
        }
    }

    pub fn fs_name(&self) -> String {
        format!("{}:{}", application(), self.settings.instance_name)
    }
}

pub struct Settings {
    pub instance_name: String,
    pub config_dir: PathBuf,
    pub mount_point: PathBuf,
    pub log_level: LevelFilter,
    pub chunk_size: usize,
    pub sync_dir: Option<PathBuf>,
    pub force_unmount: bool,
    pub auto_commit_interval: usize,
    pub foreground: bool,
}

impl Settings {
    fn merge(
        cmd_line: &mut CommandLine,
        cfg_file: &mut ConfigFile,
        dirs: &mut Directories,
    ) -> Settings {
        let instance_name = cmd_line.instance_name.clone();

        let config_dir = cmd_line
            .config_dir
            .get_or_insert(dirs.config.clone())
            .to_owned();
        dirs.config = config_dir.clone();

        let mount_point = cmd_line
            .mount_point
            .get_or_insert(
                cfg_file
                    .mount_point
                    .get_or_insert(dirs.mount_point.clone())
                    .to_owned(),
            )
            .to_owned();
        dirs.mount_point = mount_point.clone();

        let log_level = *cmd_line
            .log_level
            .get_or_insert(*cfg_file.log_level.get_or_insert(DEFAULT_LOG_LEVEL));

        let chunk_size = *cmd_line
            .chunk_size
            .get_or_insert(*cfg_file.chunk_size.get_or_insert(DEFAULT_CHUNK_SIZE));

        let auto_commit_interval = *cmd_line.auto_commit_interval.get_or_insert(
            *cfg_file
                .auto_commit_interval
                .get_or_insert(DEFAULT_AUTO_COMMIT_INTERVAL),
        );

        let sync_dir = cmd_line.sync_dir.clone();
        let force_unmount = cmd_line.force_unmount;
        let foreground = cmd_line.foreground;

        Settings {
            instance_name,
            config_dir,
            mount_point,
            log_level,
            chunk_size,
            sync_dir,
            force_unmount,
            auto_commit_interval,
            foreground,
        }
    }
}

#[derive(Debug)]
pub struct Directories {
    pub workspace: PathBuf,
    pub config: PathBuf,
    pub log: PathBuf,
    pub mount_point: PathBuf,
}

impl Directories {
    pub fn with_name(instance_name: &str) -> DenebResult<Directories> {
        let dirs = ProjectDirs::from(qualifier(), organization(), application())
            .ok_or_else(|| err_msg("Unable to create application directories."))?;

        let mount_point = home_dir()
            .ok_or_else(|| err_msg("Unable to obtain home directory."))?
            .join(application())
            .join(instance_name);

        let directories = Directories {
            workspace: dirs.data_dir().join(instance_name).join("internal"),
            config: dirs.config_dir().join(instance_name),
            log: dirs.data_dir().join(instance_name).join("log"),
            mount_point,
        };

        Ok(directories)
    }

    fn ensure_created(&self) -> DenebResult<()> {
        create_dir_all(&self.workspace)?;
        create_dir_all(&self.config)?;
        create_dir_all(&self.log)?;
        create_dir_all(&self.mount_point)?;

        Ok(())
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
