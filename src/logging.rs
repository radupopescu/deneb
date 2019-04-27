use {
    deneb_core::errors::DenebResult,
    failure::err_msg,
    log::LevelFilter,
    log4rs::{
        append::{
            console::ConsoleAppender,
            rolling_file::{
                policy::compound::{
                    roll::fixed_window::FixedWindowRoller, trigger::size::SizeTrigger,
                    CompoundPolicy,
                },
                RollingFileAppender,
            },
        },
        config::{Appender, Config, Root},
    },
    std::path::Path,
};

const MAX_LOG_SIZE: u64 = 10 * 1024 * 1024; // 10 MB
const MAX_NUM_LOGS: u32 = 5;

pub fn init_logger(level: LevelFilter, foreground: bool, dir: &Path) -> DenebResult<()> {
    let stdout = ConsoleAppender::builder().build();
    let policy = Box::new(CompoundPolicy::new(
        Box::new(SizeTrigger::new(MAX_LOG_SIZE)),
        Box::new(
            FixedWindowRoller::builder()
                .base(0)
                .build(
                    dir.join("deneb.log.{}.gz")
                        .to_str()
                        .ok_or_else(|| err_msg("Invalid log rotation pattern."))?,
                    MAX_NUM_LOGS,
                )
                .map_err(|_| err_msg("Could not configure log rotation."))?,
        ),
    ));
    let log_file = RollingFileAppender::builder().build(dir.join("deneb.log"), policy)?;

    let mut root_builder = Root::builder().appender("log_file");
    if foreground {
        root_builder = root_builder.appender("stdout");
    }

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("log_file", Box::new(log_file)))
        .build(root_builder.build(level))?;

    ::log4rs::init_config(config)?;

    Ok(())
}
