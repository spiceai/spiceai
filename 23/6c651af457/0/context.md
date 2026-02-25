# Session Context

## User Prompts

### Prompt 1

Fix these clippy issues 

error: unused import: `ObjectStore`
   --> crates/runtime/src/cluster/executor_registry.rs:444:24
    |
444 |     use object_store::{ObjectStore, memory::InMemory};
    |                        ^^^^^^^^^^^
    |
    = note: `-D unused-imports` implied by `-D warnings`
    = help: to override `-D warnings` add `#[allow(unused_imports)]`

error: used `unwrap()` on a `Result` value
   --> crates/runtime/src/cluster/partition/executor_selection.rs:154:17
    |
154 |        ...

