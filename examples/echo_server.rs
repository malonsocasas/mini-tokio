use std::{net::TcpListener, sync::Arc};

use mini_tokio::{executor::Executor, tcp_stream::AsyncTcpStream};

fn main() {
    let listening_address = "127.0.0.1:9000";
    println!("Listening on {listening_address}");
    let listener = TcpListener::bind(listening_address).unwrap();
    let executor = Executor::run(3);

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection from {:?}", stream.peer_addr().unwrap());
                let reactor = Arc::clone(&executor.reactor);
                let read_future = async {
                    let mut async_stream = AsyncTcpStream::new(stream, reactor);
                    loop {
                        let mut buf = [0; 128];
                        match async_stream.read(&mut buf).await {
                            Ok(0) => {
                                println!("Close connection with {:?}", async_stream.peer_addr().unwrap());
                                break
                            },
                            Ok(size) => {
                                println!("Received from {:?}: {:?}", async_stream.peer_addr().unwrap(), &buf[..size]);
                            },
                            _ => todo!()
                        }
                    }
                };
                executor.execute(read_future);
            },
            Err(_) => todo!(),
        };
        
    }
}
