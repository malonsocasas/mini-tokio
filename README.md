# mini-tokio

A very light implementation of Rust's [`tokio`](https://tokio.rs/) async runtime, built on top of [`mio`](https://github.com/tokio-rs/mio).

> ⚠️ **For learning purposes only.** Many error cases are not handled (plenty of `todo!()` and `unwrap()` around). There are NO unit tests. Do not use in production.

## What's inside

A minimal async runtime with the core pieces of an executor:

- `executor` — spawns tasks onto a small pool of worker threads
- `scheduler` — queues tasks that are ready to run
- `reactor` — wraps `mio` to wake tasks when their I/O is ready
- `tcp_stream` — an `async` wrapper over a `TcpStream`

## Example

The `main` function is a tiny echo-ish TCP server that reads from each connection asynchronously:

```rust
use std::{net::TcpListener, sync::Arc};

use crate::{executor::Executor, tcp_stream::AsyncTcpStream};

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
                            }
                            Ok(size) => {
                                println!("Received from {:?}: {:?}", async_stream.peer_addr().unwrap(), &buf[..size]);
                            }
                            _ => todo!(),
                        }
                    }
                };
                executor.execute(read_future);
            }
            Err(_) => todo!(),
        };
    }
}
```

## Running

Start the server:

```sh
cargo run
```

Then connect with one or more `netcat` clients in separate terminals and type away:

```sh
nc 127.0.0.1 9000
```

Each client runs as its own async task on the runtime, so you can have several connected at once and watch the server handle them concurrently.
