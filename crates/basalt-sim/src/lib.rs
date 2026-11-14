//! The simulated world: every process, the network, disks, and clocks in one thread on virtual
//! time, driven by one seed. It implements `basalt_env::backend::Backend`.

// The simulator installs the ambient world, so it may use `thread_local!`. It must be as
// deterministic as the code it runs, so the other determinism lints still apply.
#![allow(clippy::disallowed_macros)]
