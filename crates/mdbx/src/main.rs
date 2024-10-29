use clap::{Parser, Subcommand};
use color_eyre::Result;
use dump::{mdbx_dump, DumpArguments};
use reth::{reth_main, RethArguments};
use server::{server_main, ServerArguments};
use stat::{stat_main, StatArguments};
mod dump;
mod reth;
mod server;
mod stat;

#[derive(Subcommand)]
enum MDBXCommand {
    Stat(StatArguments),
    Server(ServerArguments),
    Reth(RethArguments),
    Dump(DumpArguments),
}

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    pub command: MDBXCommand,
}

async fn main_entry() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        MDBXCommand::Server(args) => server_main(args).await,
        MDBXCommand::Stat(args) => stat_main(args).await,
        MDBXCommand::Reth(args) => reth_main(args).await,
        MDBXCommand::Dump(args) => mdbx_dump(args).await,
    }
}

fn main() -> Result<()> {
    color_eyre::install()?;
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(main_entry())
}
