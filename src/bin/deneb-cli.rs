use {
    deneb::{
        app::Directories,
        talk::{ask, Command},
    },
    deneb_core::errors::DenebResult,
    structopt::StructOpt,
};

#[derive(StructOpt)]
#[structopt(about = "Deneb CLI tool")]
struct Cli {
    #[structopt(
        short = "n",
        long = "instance_name",
        default_value = "main",
        help = "Name of the Deneb instance"
    )]
    instance_name: String,
    #[structopt(subcommand)]
    cmd: Cmd,
}

impl Cli {
    fn init() -> Cli {
        Cli::from_args()
    }
}

#[derive(StructOpt)]
enum Cmd {
    #[structopt(name = "status", about = "Display the status of the Deneb process")]
    Status,
    #[structopt(name = "ping", about = "Ping the Deneb process")]
    Ping,
    #[structopt(name = "commit", about = "Send a commit request")]
    Commit,
}

fn main() -> DenebResult<()> {
    let app = Cli::init();

    let dirs = Directories::with_name(&app.instance_name)?;

    let socket_file = dirs.workspace.join("cmd.sock");

    let (text, send_cmd) = match app.cmd {
        Cmd::Status => ("status", Command::Status),
        Cmd::Ping => ("ping", Command::Ping),
        Cmd::Commit => ("commit", Command::Commit),
    };

    println!("Sending {} command", text);

    let reply = ask(socket_file, send_cmd)?;

    println!("Reply: {}", reply);

    Ok(())
}
