# Kanal

**The fast sync and async channel that Rust deserves!**

[![Crates.io][crates-badge]][crates-url]
[![Documentation][doc-badge]][doc-url]
[![MIT licensed][mit-badge]][mit-url]

[crates-badge]: https://img.shields.io/crates/v/kanal.svg?style=for-the-badge
[crates-url]: https://crates.io/crates/kanal
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg?style=for-the-badge
[mit-url]: https://github.com/fereidani/kanal/blob/master/LICENSE
[doc-badge]: https://img.shields.io/docsrs/kanal?style=for-the-badge
[doc-url]: https://docs.rs/kanal

## What is Kanal

The Kanal library is a Rust implementation of channels inspired by the CSP (Communicating Sequential Processes) model. It aims to help programmers create efficient concurrent programs by providing multi-producer and multi-consumer channels with advanced features for fast communication. The library focuses on unifying message passing between synchronous and asynchronous parts of Rust code, offering a combination of synchronous and asynchronous APIs while maintaining high performance.

## Why Kanal is faster?

1. Kanal employs a highly optimized composite technique for the transfer of objects. When the data size is less than or equal to the pointer size, it utilizes serialization, encoding the data as the pointer address. Conversely, when the data size exceeds the pointer size, the protocol employs a strategy similar to that utilized by the Golang programming language, utilizing direct memory access to copy objects from the sender's stack or write directly to the receiver's stack. This composite method not only eliminates unnecessary pointer access but also eliminates heap allocations for bounded(0) channels.
2. Kanal utilizes a specially tuned mutex for its channel locking mechanism, made possible by the predictable internal lock time of the channel. That said it's possible to use Rust standard mutex with the `std-mutex` feature and Kanal will perform better than competitors with that feature too.
3. Utilizing Rust high-performance compiler and powerful LLVM backend with highly optimized memory access and deeply thought algorithms.

## Usage

To use Kanal in your Rust project, add the following line to your `Cargo.toml` file:

```toml
[dependencies]
kanal = "0.1"
```

Sync channel example:

```rust,ignore
// Initialize a bounded sync channel with a capacity for 8 messages
let (sender, receiver) = kanal::bounded(8);

let s = sender.clone();
std::thread::spawn(move || {
    s.send("hello")?;
    anyhow::Ok(())
});

// Receive an example message in another thread
let msg = receiver.recv()?;
println!("I got msg: {}", msg);


// Convert and use channel in async context to communicate between sync and async
tokio::spawn(async move {
    // Borrow the channel as an async channel and use it in an async context ( or convert it to async using to_async() )
    sender.as_async().send("hello").await?;
    anyhow::Ok(())
});
```

Async channel example:

```rust,ignore
// Initialize a bounded channel with a capacity for 8 messages
let (sender, receiver) = kanal::bounded_async(8);

sender.send("hello").await?;
sender.send("hello").await?;

// Clone receiver and convert it to a sync receiver
let receiver_sync = receiver.clone().to_sync();

tokio::spawn(async move {
    let msg = receiver.recv().await?;
    println!("I got msg: {}", msg);
    anyhow::Ok(())
});

// Spawn a thread and use receiver in sync context
std::thread::spawn(move || {
    let msg = receiver_sync.recv()?;
    println!("I got msg in sync context: {}", msg);
    anyhow::Ok(())
});
```

## Why use Kanal?

- Kanal offers fast and efficient communication capabilities.
- Kanal simplifies communication in and between synchronous and asynchronous contexts, thanks to its flexible API like `as_sync` and `as_async`.
- Kanal provides a clean and intuitive API, making it easier to work with compared to other Rust libraries.
- Similar to Golang, Kanal allows you to close channels using the `Close` function, enabling you to broadcast a close signal from any channel instance and close the channel for both senders and receivers.
- Kanal includes high-performance MPMC (Multiple Producers Multiple Consumers) and SPSC (Single Producer Single Consumer) channels in a single package.

### Benchmark Results

Results are based on how many messages can be passed in each scenario per second.

#### Test types:

1. Seq is sequentially writing and reading to a channel in the same thread.
2. SPSC is one receiver, and one sender and passing messages between them.
3. MPSC is multiple sender threads with only one receiver.
4. MPMC is multiple senders and multiple receivers communicating through the same channel.

#### Message types:

1. `usize` tests are transferring messages of size hardware pointer.
2. `big` tests are transferring messages of 8x the size of the hardware pointer.

N/A means that the test subject is unable to perform the test due to its limitations, Some of the test subjects don't have implementation for size 0 channels, MPMC or unbounded channels.

Machine: `AMD Ryzen 9 9950X 16-Core Processor`<br />
Rust: `rustc 1.85.1 (4eb161250 2025-03-15)`<br />
Go: `go version go1.24.1 linux/amd64`<br />
OS (`uname -a`): `Linux 6.11.0-19-generic #19~24.04.1-Ubuntu SMP PREEMPT_DYNAMIC Mon Feb 17 11:51:52 UTC 2 x86_64`<br />
Date: Mar 19, 2025

[Benchmark codes](https://github.com/fereidani/rust-channel-benchmarks)

![Benchmarks](https://i.imgur.com/VPwyam0.png)

#### Why does async outperform sync in some tests?

In certain tests, asynchronous communication may exhibit superior performance compared to synchronous communication. This can be attributed to the context-switching performance of libraries such as tokio, which, similar to Golang, utilize context-switching within the same thread to switch to the next coroutine when a message is ready on a channel. This approach is more efficient than communicating between separate threads. This same principle applies to asynchronous network applications, which generally exhibit better performance compared to synchronous implementations. As the channel size increases, one may observe improved performance in synchronous benchmarks, as the sending threads are able to push data directly to the channel queue without requiring awaiting blocking/suspending signals from receiving threads.
