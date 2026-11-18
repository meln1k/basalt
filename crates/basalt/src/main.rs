//! The one shipped binary: `basalt server` runs on `basalt-rt`, and `basalt sim` runs tests on
//! `basalt-sim`.

use std::process::ExitCode;

use basalt_harness::cli::SimCommand;
use clap::{Parser, Subcommand};

/// Basalt: run a server, or run the deterministic simulation tests built into this binary.
#[derive(Parser)]
#[command(version = basalt_harness::VERSION)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a register replica on the real network and disks.
    Server(basalt_server::ServerArgs),
    /// Run simulation tests against this exact build.
    #[command(subcommand)]
    Sim(SimCommand),
}

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Server(args) => basalt_server::main(args),
        Command::Sim(command) => {
            basalt_harness::cli::main(command, basalt_harness::selftest::TESTS)
        }
    }
}
