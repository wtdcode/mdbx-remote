use std::{net::SocketAddr, sync::Arc, usize};

use clap::Args;
use color_eyre::Result;
use libmdbx_remote::{MDBXServerState, RemoteMDBX, RemoteMDBXServer};
use tarpc::server::Channel;
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

#[derive(Args)]
pub struct ServerArguments {
    #[arg(short, long, default_value = "0.0.0.0:1899")]
    pub listen: SocketAddr,
}

pub async fn server_main(args: ServerArguments) -> Result<()> {
    let mut listener = tarpc::serde_transport::tcp::listen(
        &args.listen,
        tarpc::tokio_serde::formats::Bincode::default,
    )
    .await?;
    listener.config_mut().max_frame_length(usize::MAX);

    tracing::info!("Server started at {}", &args.listen);
    let state = Arc::new(RwLock::new(MDBXServerState::default()));
    while let Some(transport) = listener.next().await {
        match transport {
            Ok(transport) => {
                tracing::info!("A new connection from {}", transport.peer_addr()?);
                let state = state.clone();
                tokio::spawn(async move {
                    let ch = tarpc::server::BaseChannel::with_defaults(transport);
                    let server = RemoteMDBXServer::new(state.clone());

                    let mut st = Box::pin(ch.execute(server.serve()));
                    while let Some(resp) = st.next().await {
                        tokio::spawn(resp);
                    }
                });
            }
            Err(e) => {
                tracing::warn!("Fail to accept tcp connection due to {}", e);
            }
        }
    }

    Ok(())
}
