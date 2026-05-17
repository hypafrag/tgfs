//! Library facade. The binary at `src/main.rs` is the user-facing tgfs
//! daemon; this `lib.rs` re-exports the same modules so the integration-test
//! example (`examples/integration_test.rs`) can reuse the indexer, FUSE
//! filesystem, and auth plumbing without duplicating the code.
//!
//! Unit tests stay attached as private submodules of their respective
//! modules (`#[path = "../tests/<module>.rs"] mod tests;` in each source
//! file), so this exposure does not affect the test setup.

pub mod config;
pub mod fuse;
pub mod index;
pub mod indexer;
pub mod login;
pub mod mtproxy;
pub mod realtime;
pub mod server;
pub mod zip_cache;
