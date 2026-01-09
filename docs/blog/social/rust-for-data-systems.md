# Rust for Data Systems

Educational and thought leadership content for LinkedIn and X (Twitter).
By Luke Kim, Founder.

---

## LinkedIn

**Rust for Data Infrastructure: Compile-Time Memory Safety**

Data infrastructure has unique requirements: memory safety without garbage collection pauses, predictable latency, safe concurrent access to shared state. Rust addresses all three through its ownership system—memory safety enforced at compile time, not runtime.

```
┌─────────────────────────────────────────────────────────────────┐
│            RUST'S OWNERSHIP MODEL: HOW IT PREVENTS BUGS          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   RUNTIME SAFETY (Go, Java, Python):                            │
│                                                                  │
│   • Garbage collector: Automatic memory management              │
│     └─ Trade-off: Unpredictable pause times                     │
│                                                                  │
│   • Race detector: Catches data races at runtime                │
│     └─ Trade-off: Only catches races that actually happen       │
│     └─ Trade-off: Too slow for production                       │
│                                                                  │
│   Thread 1        Shared Data        Thread 2                   │
│      │──── read ────►│                   │                       │
│      │               │◄──── write ───────│                       │
│      │◄─── (stale) ──│                   │                       │
│      ▼               ▼                   ▼                       │
│   Uses old value    Corrupted          Unaware                  │
│                                                                  │
│   Runtime: May or may not detect. Production: silent corruption.│
│                                                                  │
│   COMPILE-TIME SAFETY (Rust):                                    │
│                                                                  │
│   OWNERSHIP RULES:                                               │
│   1. Each value has exactly one owner                           │
│   2. When owner goes out of scope, value is dropped             │
│   3. You can have EITHER:                                        │
│      - One mutable reference (&mut T)                           │
│      - Multiple immutable references (&T)                       │
│      - But never both simultaneously                             │
│                                                                  │
│   let data = HashMap::new();                                     │
│   thread::spawn(|| {                                             │
│       data.insert(k, v);  // COMPILE ERROR                       │
│   });                     // cannot borrow `data` as mutable    │
│                                                                  │
│   This code doesn't compile. The bug cannot exist in production.│
│                                                                  │
│   CORRECT RUST:                                                  │
│                                                                  │
│   let data = Arc::new(RwLock::new(HashMap::new()));             │
│   let data_clone = Arc::clone(&data);                           │
│   thread::spawn(move || {                                        │
│       data_clone.write().unwrap().insert(k, v);  // OK          │
│   });                                                            │
│                                                                  │
│   Synchronization is explicit. The type system enforces it.     │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**What Rust's ownership prevents:**

**Data races**: The compiler rejects code where multiple threads access shared data without synchronization. Not detected at runtime—rejected at compile time.

**Use-after-free**: Memory is freed when its owner goes out of scope. No dangling pointers. No accessing freed memory.

**Null pointer dereferences**: Rust has no null. Optional values use `Option<T>`—you must handle the `None` case explicitly.

**Memory leaks** (mostly): Automatic deallocation when owners go out of scope. (Reference cycles can still leak, but they're rare in practice.)

**The tradeoffs:**

| Cost             | Magnitude                       |
| ---------------- | ------------------------------- |
| Learning curve   | 2-6 weeks to productivity       |
| Compile times    | 2-5x slower than Go             |
| Async complexity | Explicit Pin, Future, lifetimes |
| Ecosystem size   | Smaller than Python/JS, growing |
| Hiring           | Smaller talent pool             |

**Why these tradeoffs are worth it for data infrastructure:**

Data systems process millions of rows. A single corrupted result can cascade through downstream systems. The bugs Rust prevents—data races, use-after-free, null pointers—aren't edge cases. They're the bugs that corrupt production data at 2am.

**Key patterns for async data systems:**

```rust
// Blocking I/O: wrap in spawn_blocking
let result = tokio::task::spawn_blocking(move || {
    std::fs::read_to_string("file.txt")
}).await?;

// CPU-bound: use rayon with channel
let (tx, rx) = tokio::sync::oneshot::channel();
rayon::spawn(move || {
    let result = expensive_computation();
    let _ = tx.send(result);
});
let result = rx.await?;
```

**Rule**: Async code should reach `.await` within 10-100 microseconds. Blocking operations starve the runtime.

From our experience: we spent three weeks debugging a data race that only appeared under production load. After rewriting in Rust, that class of bug became impossible. Zero data races, zero use-after-free, zero null pointers in over two years of production.

---

## X

Rust for query engines: the tradeoffs

Why Rust:
1. Memory safety without GC pauses (latency predictability)
2. Zero-cost abstractions (high-level code, efficient output)
3. Fearless concurrency (data races caught at compile time)
4. Data ecosystem (Arrow, DataFusion, DuckDB bindings)

The costs:
- 2-4 week learning curve for ownership/borrowing
- Slow compilation (workspace splitting helps)
- Async complexity (`Pin`, `Future`, lifetimes)
- Smaller talent pool

Key patterns for data systems:

```rust
// Blocking ops need spawn_blocking
let result = tokio::task::spawn_blocking(move || {
    // sync I/O here
}).await?;

// CPU-bound uses rayon
let (tx, rx) = tokio::sync::oneshot::channel();
rayon::spawn(move || {
    let _ = tx.send(expensive_computation());
});
```

Rule: async code must reach .await within 10-100μs

Error handling: SNAFU > expect/unwrap
Logging: tracing > log
Clippy: pedantic mode in CI

Bugs Rust prevents would be devastating in production. Worth the learning curve.
