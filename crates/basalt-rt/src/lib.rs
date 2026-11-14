//! The production backend: real clocks, sockets, disks, and threads behind
//! `basalt_env::backend::Backend`.

// The one crate that reaches the real world directly, so the determinism lints don't apply.
#![allow(
    clippy::disallowed_macros,
    clippy::disallowed_methods,
    clippy::disallowed_types
)]
