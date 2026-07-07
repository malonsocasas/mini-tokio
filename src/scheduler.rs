use std::{future::Future, pin::Pin, sync::{Arc, Mutex, atomic::AtomicUsize, mpsc::{Sender, channel}}, task::{Context, Wake, Waker}, thread};

type Task = Arc<Mutex<Pin<Box<dyn Future<Output = ()> + Send>>>>;

struct TaskWaker {
    task: Task,
    task_tx: Sender<Task>
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.task_tx.send(Arc::clone(&self.task)).unwrap();
    }
}

struct Worker {
    task_tx: Sender<Task>,
    run_queue: Sender<Task>
}

impl Worker {
    pub fn run(id: usize, run_queue: Sender<Task>) -> Arc<Self> {
        let (task_tx, task_rx) = channel();
        let worker = Arc::new(Worker {
            task_tx, 
            run_queue
        });
        let worker_clone = Arc::clone(&worker);
        thread::Builder::new()
            .name(format!("Worker thread {}", id))
            .spawn(move || { 
                loop {
                    let future = task_rx.recv().unwrap();
                    let waker = Waker::from(Arc::new(worker_clone.get_task_waker(Arc::clone(&future))));
                    let mut ctx = Context::from_waker(&waker);
                    let _ = future.lock().unwrap().as_mut().poll(&mut ctx);
                }
            }).unwrap();
        worker
    }

    pub fn add_task(&self, task: Task) {
        self.task_tx.send(task).unwrap();
    }

    fn get_task_waker(&self, task: Task) -> TaskWaker {
        TaskWaker { 
            task, 
            task_tx: self.run_queue.clone() 
        }
    }
}


pub struct Scheduler {
    future_tx: Sender<Task>,
    workers: Vec<Arc<Worker>>, 
    round_robin_idx: AtomicUsize
}

impl Scheduler {
    pub fn run(nb_workers: usize) -> Arc<Self> {
        let (future_tx, future_rx) = channel();
        let workers = (0..nb_workers).map(|id| Worker::run(id, future_tx.clone()));
        let scheduler = Arc::new(Scheduler { 
            future_tx: future_tx.clone(), 
            workers: workers.collect(), 
            round_robin_idx: AtomicUsize::new(0)
        });
        let scheduler_clone = Arc::clone(&scheduler);
        thread::Builder::new()
            .name("Scheduler thread".to_string())
            .spawn(move || { 
                loop {
                    let future = future_rx.recv().unwrap();
                    scheduler_clone.next_worker().add_task(future);
                }
             }).unwrap();

        scheduler
    }

    fn next_worker(&self) -> &Worker {
        self.round_robin_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.workers.get(self.round_robin_idx.load(std::sync::atomic::Ordering::Relaxed) % self.workers.len()).unwrap()
    }
    
    pub fn poll(&self, f: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        self.future_tx.send(Arc::new(Mutex::new(f))).unwrap();
    }

}