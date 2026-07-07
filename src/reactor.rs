use std::{collections::HashMap, sync::{Arc, Mutex}, task::Waker, thread};

pub struct Reactor {
    poll: Mutex<mio::Poll>,
    registry: mio::Registry,
    token_waker_mapping: Mutex<HashMap<mio::Token, Waker>>
}

impl Reactor {
    pub fn register_waker(&self, token: mio::Token, waker: Waker, source: &mut impl mio::event::Source) {
        if self.registry.register(source, token, mio::Interest::READABLE).is_err() {
            self.registry.reregister(source, token, mio::Interest::READABLE).unwrap();
        };
        self.token_waker_mapping.lock().unwrap().insert(token, waker);
    }

    fn take_waker(&self, token: &mio::Token) -> Waker {
        self.token_waker_mapping.lock().unwrap().remove(token).unwrap()
    }

    pub fn run() -> Arc<Self> {
        let poll = mio::Poll::new().unwrap();
        let registry = poll.registry().try_clone().unwrap();
        let reactor = Arc::new(Reactor { 
            poll: Mutex::new(poll), 
            registry,
            token_waker_mapping: Mutex::new(HashMap::new()) 
        });
        let reactor_clone = Arc::clone(&reactor);
        thread::Builder::new()
            .name("Reactor thread".to_string())
            .spawn(move || { 
                let mut events = mio::Events::with_capacity(128);
                loop {
                    reactor_clone.poll.lock().unwrap().poll(&mut events, None).unwrap();
                    for event in events.iter() {
                        let waker = reactor_clone.take_waker(&event.token());
                        waker.wake();
                    }
                }
             })
            .unwrap();
        reactor
    }
}