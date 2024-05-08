use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use tracing::{error, info};

use crate::redis::{RedisCommand, RedisCommandHandler};

pub fn start_redis_server(address: &str) -> anyhow::Result<()> {
    let listener = TcpListener::bind(address)?;
    info!("Redis Server listening on {}", address);

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => handle_client(&mut stream)?,
            Err(e) => {
                error!("Connection failed: {}", e);
                continue;
            }
        }
    }
    Ok(())
}

fn handle_client(stream: &mut TcpStream) -> Result<(), anyhow::Error> {
    let mut buffer = [0; 1024];
    match stream.read(&mut buffer) {
        Ok(_) => {
            let command = RedisCommand::from_buffer(&buffer)?;
            let response = RedisCommandHandler::handle_command(command);
            stream.write_all(response.message.as_bytes())?;
            info!("Sent response: {}", response.message);
        }
        Err(e) => {
            error!("Failed to read from connection: {}", e);
        }
    }

    Ok(())
}
