use std::{sync::Arc, time::Duration};

use libmdbx_remote::{
    Environment, EnvironmentAny, MDBXServerState, RemoteEnvironment, RemoteMDBX, RemoteMDBXClient,
    RemoteMDBXServer,
};
use tarpc::server::Channel;
use tempfile::{tempdir, TempDir};
use tokio::sync::RwLock;
use tokio_stream::StreamExt;

pub async fn remote_env() -> (TempDir, EnvironmentAny) {
    let dir = tempdir().unwrap();
    let (cl, sv) = tarpc::transport::channel::unbounded();
    let server = tarpc::server::BaseChannel::with_defaults(sv);

    tokio::spawn(async move {
        let mut st = Box::pin(server.execute(RemoteMDBXServer::new().serve()));
        while let Some(res) = st.next().await {
            tokio::spawn(res);
        }
    });

    let client = RemoteMDBXClient::new(tarpc::client::Config::default(), cl);
    let env = RemoteEnvironment::open_with_builder(
        dir.path().to_path_buf(),
        Environment::builder(),
        client,
        Duration::from_secs(5),
    )
    .await
    .unwrap();

    (dir, EnvironmentAny::Remote(env))
}

pub fn local_env() -> (TempDir, EnvironmentAny) {
    let dir = tempdir().unwrap();
    let env = Environment::builder().open(dir.path()).unwrap();
    (dir, EnvironmentAny::Local(env))
}
