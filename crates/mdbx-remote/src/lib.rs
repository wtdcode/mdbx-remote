#![doc = include_str!("../README.md")]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![allow(missing_docs, clippy::needless_pass_by_ref_mut)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

pub extern crate mdbx_remote_sys as ffi;

#[cfg(feature = "read-tx-timeouts")]
pub use crate::environment::read_transactions::MaxReadTransactionDuration;
pub use crate::{
    any::{CursorAny, DatabaseAny, EnvironmentAny, TransactionAny},
    codec::*,
    cursor::{Cursor, Iter, IterDup},
    database::Database,
    environment::{
        Environment, EnvironmentBuilder, EnvironmentKind, Geometry, HandleSlowReadersCallback,
        HandleSlowReadersReturnCode, Info, PageSize, Stat,
    },
    error::{Error, Result},
    flags::*,
    remote::{RemoteCursor, RemoteDatabase, RemoteEnvironment, RemoteTransaction, ClientError},
    service::{MDBXServerState, RemoteMDBX, RemoteMDBXClient, RemoteMDBXServer, ServerError},
    transaction::{CommitLatency, Transaction, TransactionKind, RO, RW},
};

mod any;
mod codec;
mod cursor;
mod database;
mod environment;
mod error;
mod flags;
mod remote;
mod service;
mod transaction;
mod txn_manager;

#[cfg(test)]
mod test_utils {
    use super::*;
    use byteorder::{ByteOrder, LittleEndian};
    use tempfile::tempdir;

    /// Regression test for <https://github.com/danburkert/lmdb-rs/issues/21>.
    /// This test reliably segfaults when run against lmbdb compiled with opt level -O3 and newer
    /// GCC compilers.
    #[test]
    fn issue_21_regression() {
        const HEIGHT_KEY: [u8; 1] = [0];

        let dir = tempdir().unwrap();

        let env = {
            let mut builder = Environment::builder();
            builder.set_max_dbs(2);
            builder.set_geometry(Geometry {
                size: Some(1_000_000..1_000_000),
                ..Default::default()
            });
            builder.open(dir.path()).expect("open mdbx env")
        };

        for height in 0..1000 {
            let mut value = [0u8; 8];
            LittleEndian::write_u64(&mut value, height);
            let tx = env.begin_rw_txn().expect("begin_rw_txn");
            let index = tx
                .create_db(None, DatabaseFlags::DUP_SORT)
                .expect("open index db");
            tx.put(index.dbi(), HEIGHT_KEY, value, WriteFlags::empty())
                .expect("tx.put");
            tx.commit().expect("tx.commit");
        }
    }
}
