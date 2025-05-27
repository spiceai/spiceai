# Spice Scheduler Crate

The `scheduler` crate provides a flexible, asynchronous task scheduling framework for Rust applications. It is designed to support complex scheduling scenarios, including interval-based and manual task triggering, with support for cancellation, notification, and queue management.

## Features

- **Composable Schedules:** Define schedules with multiple components and channels.
- **Task Channels:** Support for interval-based and manual task request channels.
- **Cancellation & Notification:** Built-in cancellation tokens and notification channels for robust control.
- **Queue Management:** Ability to clear queued tasks and interrupt running tasks.
- **Async/Await:** Fully asynchronous, leveraging Tokio for concurrency.
- **Extensible:** Easily add new types of channels or scheduled tasks.

## Concepts

- **Schedule:** A collection of tasks and channels (triggers).
- **TaskRequestChannel:** Mechanism for generating task requests (e.g. interval, manual).
- **ScheduledTask:** Trait for components that can be scheduled and executed.
- **Scheduler:** Manages the lifecycle of schedules and their execution.

## Architecture

This mermaid chart describes the role and flow of the scheduler, in the context of a service that performs data refreshes. This diagram includes elements from the roadmap, such as retry mechanisms and paralellism controls:

![Mermaid chart describing the scheduler architecture](./chart.png)

### Example

```rust
use scheduler::{
    schedule::Schedule,
    scheduler::Scheduler,
    channel::interval::IntervalRequestChannel,
    task::ScheduledTask,
};
use std::sync::Arc;
use tokio::sync::RwLock;
use async_trait::async_trait;

struct MyTask;

#[async_trait]
impl ScheduledTask for MyTask {
    async fn execute(&self) -> scheduler::Result<()> {
        println!("Task executed!");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let schedule = Schedule::new(Arc::new(MyTask))
        .add_trigger(Arc::new(RwLock::new(IntervalRequestChannel::new(5)))); // every 5 seconds

    let scheduler = Scheduler::new("example_scheduler".into(), vec![Arc::new(schedule)]);
    let running_scheduler = scheduler.start().await.expect("Scheduler should start");

    // Let it run for some time
    tokio::time::sleep(std::time::Duration::from_secs(15)).await;

    running_scheduler.stop().await;
}
```

## Extending

You can implement your own `ScheduledTask` or `TaskRequestChannel` by implementing the respective traits.

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Roadmap

The following features are planned:

- **Retry Mechanisms:** Support for automatic retries of failed tasks, with configurable backoff strategies and retry limits.
- **Parallelism Controls:** Ability to limit the number of concurrently running tasks per schedule or globally, to better manage resource usage.
- **Cron Scheduling:** Native support for cron-like scheduling expressions, enabling more flexible and calendar-based task triggers.
