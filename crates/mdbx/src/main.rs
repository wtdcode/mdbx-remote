use clap::{Parser, Subcommand};
use color_eyre::Result;
use server::{server_main, ServerArguments};
use stat::{stat_main, StatArguments};
mod server;
mod stat;

#[derive(Subcommand)]
enum MDBXCommand {
    Stat(StatArguments),
    Server(ServerArguments),
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
