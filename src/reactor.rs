use std::{collections::HashMap, sync::Mutex, task::Waker};

pub struct Reactor {
    poll: Mutex<mio::Poll>,
    registry: mio::Registry,
    token_waker_mapping: Mutex<HashMap<mio::Token, Waker>>
}

impl Reactor {
    pub fn new() -> Self {
        let poll = mio::Poll::new().unwrap();
        let registry = poll.registry().try_clone().unwrap();
        Reactor { 
            poll: Mutex::new(poll), 
            registry,
            token_waker_mapping: Mutex::new(HashMap::new()) 
        }
    }

    pub fn register_waker(&self, token: mio::Token, waker: Waker, source: &mut impl mio::event::Source) {
        if self.registry.register(source, token, mio::Interest::READABLE).is_err() {
            self.registry.reregister(source, token, mio::Interest::READABLE).unwrap();
        };
        self.token_waker_mapping.lock().unwrap().insert(token, waker);
    }

    fn take_waker(&self, token: &mio::Token) -> Waker {
        self.token_waker_mapping.lock().unwrap().remove(token).unwrap()
    }

    pub fn run(&self) {
        let mut events = mio::Events::with_capacity(128);
        loop {
            self.poll.lock().unwrap().poll(&mut events, None).unwrap();
            for event in events.iter() {
                let waker = self.take_waker(&event.token());
                waker.wake();
            }
        }
    }
}