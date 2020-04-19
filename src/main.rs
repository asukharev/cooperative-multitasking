mod task {
    use std::{
        future::Future,
        pin::Pin,
        sync::atomic::{AtomicU64, Ordering},
        task::{Context, Poll},
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    pub struct TaskId(u64);

    impl TaskId {
        fn new() -> Self {
            static NEXT_ID: AtomicU64 = AtomicU64::new(0);
            TaskId(NEXT_ID.fetch_add(1, Ordering::Relaxed))
        }
    }

    pub struct Task {
        pub(crate) id: TaskId,
        pub(crate) future: Pin<Box<dyn Future<Output = ()>>>,
    }

    impl Task {
        pub fn new(future: impl Future<Output = ()> + 'static) -> Self {
            Task {
                id: TaskId::new(),
                future: Box::pin(future),
            }
        }
        pub fn poll(&mut self, context: &mut Context) -> Poll<()> {
            self.future.as_mut().poll(context)
        }
    }
}

mod waker {
    use crate::task::TaskId;
    use crossbeam_queue::ArrayQueue;
    use std::{
        sync::{atomic::AtomicBool, Arc},
        task::{RawWaker, RawWakerVTable, Waker},
        thread::Thread,
    };

    pub(crate) struct TaskWaker {
        pub(crate) task_id: TaskId,
        pub(crate) wake_queue: Arc<ArrayQueue<TaskId>>, // worker wake_queue ref
        pub(crate) blocked: Arc<(AtomicBool, Thread)>,
    }

    impl TaskWaker {
        fn wake_task(&self) {
            self.wake_queue.push(self.task_id).expect("wake_queue full");
            // println!("try unpark thread {:?}", self.blocked.1.id());
            self.blocked.1.unpark();
        }
    }

    impl From<TaskWaker> for Waker {
        fn from(my_waker: TaskWaker) -> Self {
            let aw = Arc::into_raw(Arc::new(my_waker));
            let raw_waker = RawWaker::new(aw as *const (), &VTABLE);
            unsafe { Waker::from_raw(raw_waker) }
        }
    }

    const VTABLE: RawWakerVTable = unsafe {
        RawWakerVTable::new(
            |s| waker_clone(&*(s as *const TaskWaker)),     // clone
            |s| waker_wake(&*(s as *const TaskWaker)),      // wake
            |s| waker_wake(*(s as *const &TaskWaker)),      // wake by ref
            |s| drop(Arc::from_raw(s as *const TaskWaker)), // decrease refcount
        )
    };

    fn waker_wake(s: &TaskWaker) {
        s.wake_task();
    }

    fn waker_clone(s: &TaskWaker) -> RawWaker {
        let arc = unsafe { Arc::from_raw(s) };
        std::mem::forget(arc.clone()); // increase ref count
        RawWaker::new(Arc::into_raw(arc) as *const (), &VTABLE)
    }
}

mod worker {
    use crate::{
        task::{Task, TaskId},
        waker::TaskWaker,
    };
    use crossbeam_queue::ArrayQueue;
    use std::{
        collections::{BTreeMap, VecDeque},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        task::{Context, Poll, Waker},
        thread::Thread,
    };

    pub struct Worker {
        task_queue: VecDeque<Task>,
        waiting_tasks: BTreeMap<TaskId, Task>,
        wake_queue: Arc<ArrayQueue<TaskId>>,
        blocked: Arc<(AtomicBool, Thread)>,
    }

    impl Worker {
        pub fn new(pool_depth: usize) -> Self {
            Worker {
                task_queue: VecDeque::new(),
                waiting_tasks: BTreeMap::new(),
                wake_queue: Arc::new(ArrayQueue::new(pool_depth)),
                blocked: Arc::new((AtomicBool::new(false), std::thread::current())),
            }
        }

        pub fn spawn(&mut self, task: Task) {
            self.task_queue.push_back(task);
        }

        fn run_ready_tasks(&mut self) {
            while let Some(mut task) = self.task_queue.pop_front() {
                let waker = self.create_waker(task.id); // Waker::from(task);
                let mut context = Context::from_waker(&waker);
                match task.poll(&mut context) {
                    Poll::Ready(()) => {
                        // task done
                        println!("task done");
                    }
                    Poll::Pending => {
                        if self.waiting_tasks.insert(task.id, task).is_some() {
                            panic!("task with same ID already in waiting_tasks");
                        }
                        // std::thread::yield_now();
                    }
                }
            }
        }

        fn create_waker(&self, task_id: TaskId) -> Waker {
            let task_waker = TaskWaker {
                task_id: task_id,
                wake_queue: self.wake_queue.clone(),
                blocked: self.blocked.clone(),
            };
            Waker::from(task_waker)
        }

        fn wake_tasks(&mut self) {
            while let Ok(task_id) = self.wake_queue.pop() {
                if let Some(task) = self.waiting_tasks.remove(&task_id) {
                    self.task_queue.push_back(task);
                }
            }
        }

        pub fn run(&mut self) {
            loop {
                self.wake_tasks();
                self.run_ready_tasks();
                if self.wake_queue.is_empty() {
                    self.blocked.0.store(true, Ordering::Release);
                }
                while self.blocked.0.load(Ordering::Acquire) {
                    println!("parking thread {:?}", self.blocked.1.id());
                    std::thread::yield_now();
                    std::thread::park();
                    println!("unparking thread {:?}", self.blocked.1.id());
                    self.blocked
                        .0
                        .store(self.wake_queue.is_empty(), Ordering::Release);
                }
            }
        }
    }
}

extern crate rand;

use crate::task::*;
use crate::worker::*;
use conquer_once::spin::OnceCell;
use crossbeam_queue::ArrayQueue;
use futures_util::task::AtomicWaker;
use rand::Rng;
use std::future::Future;

struct Input {}

impl Input {
    fn new() -> Self {
        Input {}
    }
}

impl Future for Input {
    type Output = Option<u8>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<<Self as std::future::Future>::Output> {
        WAKER.register(cx.waker());
        if let Ok(queue) = QUEUE.try_get() {
            if let Ok(v) = queue.pop() {
                WAKER.take();
                return std::task::Poll::Ready(Some(v));
            }
        }
        std::task::Poll::Pending
    }
}

static QUEUE: OnceCell<ArrayQueue<u8>> = OnceCell::uninit();
static WAKER: AtomicWaker = AtomicWaker::new();

fn main() {
    QUEUE
        .try_init_once(|| ArrayQueue::new(100000))
        .expect("ScancodeStream::new should only be called once");

    std::thread::spawn(|| {
        let mut rng = rand::thread_rng();

        loop {
            let secs = rng.gen_range(1, 10);
            if let Ok(queue) = QUEUE.try_get() {
                if let Err(_) = queue.push(secs as u8) {
                    println!("WARNING: scancode queue full; dropping keyboard input");
                } else {
                    println!("send {:?}", secs);
                    WAKER.wake();
                }
            } else {
                println!("WARNING: scancode queue uninitialized");
            }
            std::thread::sleep(std::time::Duration::from_secs(secs));
        }
    });

    let print = async {
        loop {
            let input = Input::new();
            let v = input.await;
            println!("recv {:?}", v);
        }
    };

    let mut worker = Worker::new(10_000_000);
    worker.spawn(Task::new(print));
    println!("run worker");
    worker.run();

    println!("done");
}
