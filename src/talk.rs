use {
    bincode::{deserialize, serialize},
    deneb_core::errors::DenebResult,
    log::{error, info},
    serde::{Deserialize, Serialize},
    std::{
        fs::remove_file,
        io::{Read, Write},
        net::Shutdown,
        os::unix::net::{UnixListener, UnixStream},
        path::Path,
        thread::spawn,
    },
};

#[derive(Clone, Copy, Serialize, Deserialize)]
pub enum Command {
    Status,
    Ping,
    Commit,
}

pub fn listen<P, A>(socket_file: P, action: A) -> DenebResult<()>
where
    P: AsRef<Path> + Send + 'static,
    A: Fn(Command) -> DenebResult<String> + Send + 'static,
{
    spawn(move || {
        let _ = remove_file(&socket_file);
        match UnixListener::bind(socket_file) {
            Ok(listener) => {
                for stream in listener.incoming() {
                    let mut socket = stream?;
                    let mut bytes = Vec::new();
                    socket.read_to_end(&mut bytes)?;
                    let cmd = deserialize(&bytes)?;
                    let reply = action(cmd)?;
                    socket.write_all(reply.as_bytes())?;
                }
            }
            Err(e) => {
                error!("Could not bind listener socket: {}", e);
            }
        }
        let ret: DenebResult<()> = Ok(());
        ret
    });

    info!("Started command listener");

    Ok(())
}

pub fn ask<P: AsRef<Path>>(socket_file: P, cmd: Command) -> DenebResult<String> {
    let mut stream = UnixStream::connect(&socket_file)?;

    let msg = serialize(&cmd)?;
    stream.write_all(&msg)?;
    stream.shutdown(Shutdown::Write)?;

    let mut reply = String::new();
    stream.read_to_string(&mut reply)?;

    Ok(reply)
}
