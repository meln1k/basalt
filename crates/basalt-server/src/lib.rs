//! `basalt server`: one register replica on `basalt-rt`, the real network and disks.
//!
//! Simulation-only code must never run in a server. `deny.toml` lets only `basalt-harness` and
//! `toy-register`'s tests depend on `basalt-sim`, and only the `basalt` binary depend on
//! `basalt-harness`, so this crate can't construct a simulated world or reach `SimControl`.

use std::path::PathBuf;
use std::process::ExitCode;

use basalt_rt::{Config, Runtime};
use clap::Args;
use toy_register::{DEFAULT_PORT, ReplicaConfig, run_replica};

#[derive(Args)]
pub struct ServerArgs {
    /// The directory the replica stores its data in, created if missing.
    #[arg(long)]
    data_dir: PathBuf,
    /// The TCP port to listen on, on every local IPv4 address.
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,
}

/// Runs the replica until the process is killed. Returns only if it can't start.
pub fn main(args: ServerArgs) -> ExitCode {
    let runtime = match Runtime::new(Config {
        disks: vec![args.data_dir.clone()],
        ..Config::default()
    }) {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!(
                "basalt server: can't start the runtime on {}: {e}",
                args.data_dir.display()
            );
            return ExitCode::FAILURE;
        }
    };
    let config = ReplicaConfig {
        port: args.port,
        ..ReplicaConfig::default()
    };
    runtime.block_on(run_replica(runtime.ctx(), config));
    eprintln!("basalt server: the replica stopped; its trace above says why");
    ExitCode::FAILURE
}
