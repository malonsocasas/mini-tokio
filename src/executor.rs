use std::{future::Future, sync::Arc, thread};

use crate::{reactor::Reactor, scheduler::Scheduler};

pub struct Executor {
    pub reactor: Arc<Reactor>,
    scheduler: Arc<Scheduler>
}

impl Executor {

    pub fn execute<T: Future<Output = ()> + Send + Sync + 'static>(&self, f: T) {
        let task = Box::pin(f);
        self.scheduler.poll(task);
    }

    pub fn run(nb_workers: usize) -> Self {
        let scheduler = Scheduler::run(nb_workers);
        let reactor = Arc::new(Reactor::new());

        let reactor_clone = Arc::clone(&reactor);
        thread::Builder::new()
            .name("Reactor thread".to_string())
            .spawn(move || { reactor_clone.run() })
            .unwrap();

        Self { reactor, scheduler }
    }

}