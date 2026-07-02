use std::{future::Future, io::{self, Read}, net::TcpStream, os::fd::AsRawFd, pin::Pin, sync::Arc, task::{Context, Poll}};

use crate::reactor::Reactor;



pub struct AsyncTcpStream {
    stream: TcpStream,
    reactor: Arc<Reactor>,
    token: mio::Token
}

impl AsyncTcpStream {

    pub fn new(stream: TcpStream, reactor: Arc<Reactor>) -> Self {
        let token = mio::Token(stream.as_raw_fd() as usize);
        stream.set_nonblocking(true).unwrap();
        AsyncTcpStream { 
            stream, 
            reactor, 
            token
        }
    }

    pub fn read<'a>(&'a mut self, buf: &'a mut [u8]) -> TcpRead<'a> {
        TcpRead {
            stream: self,
            buf
        }
    }

    pub fn peer_addr(&self) -> Result<std::net::SocketAddr, io::Error> {
        self.stream.peer_addr()
    }

}

pub struct TcpRead<'a> {
    stream: &'a mut AsyncTcpStream,
    buf: &'a mut [u8]
}

impl Future for TcpRead<'_> {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future = self.get_mut();     
        // let mut buf = [0; 128];

        match future.stream.stream.read(future.buf) {
            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                let raw_fd = future.stream.stream.as_raw_fd();
                let mut source = mio::unix::SourceFd(&raw_fd);
                future.stream.reactor.register_waker(future.stream.token, cx.waker().clone(), &mut source);
                Poll::Pending
            },
            result => Poll::Ready(result)
        }
    }
}