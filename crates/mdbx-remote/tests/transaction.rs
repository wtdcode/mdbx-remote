#![allow(missing_docs)]
use libmdbx_remote::*;
use std::{borrow::Cow, sync::Arc};
use tempfile::tempdir;
use tokio::sync::Barrier;
// use tokio_stream::StreamExt;
use futures_util::StreamExt;
mod any;

macro_rules! local_remote {
    ($test: tt) => {
        paste::paste! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)] // Intentionally reduce worker threads to expose deadlock issues
            async fn [<$test _remote>]() {
                // console_subscriber::init();
                let (db, env) = any::remote_env().await;
                $test(env).await;
                db.close().unwrap();
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 1)] // Intentionally reduce worker threads to expose deadlock issues
            async fn [<$test _local>]() {
                // console_subscriber::init();
                let (db, env) = any::local_env();
                $test(env).await;
                db.close().unwrap();
            }
        }
    };
}

async fn test_put_get_del(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    assert_eq!(txn.get(db.dbi(), b"key1").await.unwrap(), Some(*b"val1"));
    assert_eq!(txn.get(db.dbi(), b"key2").await.unwrap(), Some(*b"val2"));
    assert_eq!(txn.get(db.dbi(), b"key3").await.unwrap(), Some(*b"val3"));
    assert_eq!(txn.get::<()>(db.dbi(), b"key").await.unwrap(), None);

    txn.del(db.dbi(), b"key1", None).await.unwrap();
    assert_eq!(txn.get::<()>(db.dbi(), b"key1").await.unwrap(), None);
}

async fn test_put_get_del_multi(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).await.unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key1", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    {
        let mut cur = txn.cursor(&db).await.unwrap();
        let iter = cur.iter_dup_of::<(), [u8; 4]>(b"key1").await.unwrap();
        let vals = iter
            .map(|x| x.unwrap())
            .map(|(_, x)| x)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(vals, vec![*b"val1", *b"val2", *b"val3"]);
    }
    txn.commit().await.unwrap();

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    txn.del(db.dbi(), b"key1", Some(b"val2")).await.unwrap();
    txn.del(db.dbi(), b"key2", None).await.unwrap();
    txn.commit().await.unwrap();

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    {
        let mut cur = txn.cursor(&db).await.unwrap();
        let iter = cur.iter_dup_of::<(), [u8; 4]>(b"key1").await.unwrap();
        let vals = iter
            .map(|x| x.unwrap())
            .map(|(_, x)| x)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(vals, vec![*b"val1", *b"val3"]);

        let iter = cur.iter_dup_of::<(), ()>(b"key2").await.unwrap();
        assert_eq!(0, iter.count().await);
    }
    txn.commit().await.unwrap();
}

async fn test_put_get_del_empty_key(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.create_db(None, Default::default()).await.unwrap();
    txn.put(db.dbi(), b"", b"hello", WriteFlags::empty())
        .await
        .unwrap();
    assert_eq!(txn.get(db.dbi(), b"").await.unwrap(), Some(*b"hello"));
    txn.commit().await.unwrap();

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    assert_eq!(txn.get(db.dbi(), b"").await.unwrap(), Some(*b"hello"));
    txn.put(db.dbi(), b"", b"", WriteFlags::empty())
        .await
        .unwrap();
    assert_eq!(txn.get(db.dbi(), b"").await.unwrap(), Some(*b""));
}

// Does it really make sense?
// Maybe we can return something like `BufferProxy` and implement AsyncWrite
// async fn test_reserve(env: EnvironmentAny) {
//     let txn = env.begin_rw_txn().await.unwrap();
//     let db = txn.open_db(None).await.unwrap();
//     {
//         let mut writer = txn.reserve(&db, b"key1", 4, WriteFlags::empty()).await.unwrap();
//         writer.write_all(b"val1").await.unwrap();
//     }
//     txn.commit().await.unwrap();

//     let txn = env.begin_rw_txn().await.unwrap();
//     let db = txn.open_db(None).await.unwrap();
//     assert_eq!(txn.get(db.dbi(), b"key1").await.unwrap(), Some(*b"val1"));
//     assert_eq!(txn.get::<()>(db.dbi(), b"key").await.unwrap(), None);

//     txn.del(db.dbi(), b"key1", None).await.unwrap();
//     assert_eq!(txn.get::<()>(db.dbi(), b"key1").await.unwrap(), None);
// }

async fn test_nested_txn(env: EnvironmentAny) {
    let mut txn = env.begin_rw_txn().await.unwrap();
    txn.put(
        txn.open_db(None).await.unwrap().dbi(),
        b"key1",
        b"val1",
        WriteFlags::empty(),
    )
    .await
    .unwrap();

    {
        let nested = txn.begin_nested_txn().await.unwrap();
        let db = nested.open_db(None).await.unwrap();
        nested
            .put(db.dbi(), b"key2", b"val2", WriteFlags::empty())
            .await
            .unwrap();
        assert_eq!(nested.get(db.dbi(), b"key1").await.unwrap(), Some(*b"val1"));
        assert_eq!(nested.get(db.dbi(), b"key2").await.unwrap(), Some(*b"val2"));
    }

    let db = txn.open_db(None).await.unwrap();
    assert_eq!(txn.get(db.dbi(), b"key1").await.unwrap(), Some(*b"val1"));
    assert_eq!(txn.get::<()>(db.dbi(), b"key2").await.unwrap(), None);
}

async fn test_clear_db(env: EnvironmentAny) {
    {
        let txn = env.begin_rw_txn().await.unwrap();
        txn.put(
            txn.open_db(None).await.unwrap().dbi(),
            b"key",
            b"val",
            WriteFlags::empty(),
        )
        .await
        .unwrap();
        assert!(!txn.commit().await.unwrap().0);
    }

    {
        let txn = env.begin_rw_txn().await.unwrap();
        txn.clear_db(txn.open_db(None).await.unwrap().dbi())
            .await
            .unwrap();
        assert!(!txn.commit().await.unwrap().0);
    }

    let txn = env.begin_ro_txn().await.unwrap();
    assert_eq!(
        txn.get::<()>(txn.open_db(None).await.unwrap().dbi(), b"key")
            .await
            .unwrap(),
        None
    );
}

// TODO: Rewrite this to use EnvironmentAny
#[test]
fn test_drop_db() {
    let dir = tempdir().unwrap();
    {
        let env = Environment::builder()
            .set_max_dbs(2)
            .open(dir.path())
            .unwrap();

        {
            let txn = env.begin_rw_txn().unwrap();
            txn.put(
                txn.create_db(Some("test"), DatabaseFlags::empty())
                    .unwrap()
                    .dbi(),
                b"key",
                b"val",
                WriteFlags::empty(),
            )
            .unwrap();
            // Workaround for MDBX dbi drop issue
            txn.create_db(Some("canary"), DatabaseFlags::empty())
                .unwrap();
            assert!(!txn.commit().unwrap().0);
        }
        {
            let txn = env.begin_rw_txn().unwrap();
            let db = txn.open_db(Some("test")).unwrap();
            unsafe {
                txn.drop_db(db).unwrap();
            }
            assert!(matches!(
                txn.open_db(Some("test")).unwrap_err(),
                Error::NotFound
            ));
            assert!(!txn.commit().unwrap().0);
        }
    }

    let env = Environment::builder()
        .set_max_dbs(2)
        .open(dir.path())
        .unwrap();

    let txn = env.begin_ro_txn().unwrap();
    txn.open_db(Some("canary")).unwrap();
    assert!(matches!(
        txn.open_db(Some("test")).unwrap_err(),
        Error::NotFound
    ));
}

async fn test_concurrent_readers_single_writer(env: EnvironmentAny) {
    let n = 10usize; // Number of concurrent readers
    let barrier = Arc::new(Barrier::new(n + 1));
    let mut threads: Vec<tokio::task::JoinHandle<bool>> = Vec::with_capacity(n);

    let key = b"key";
    let val = b"val";

    for _ in 0..n {
        let reader_env = env.clone();
        let reader_barrier = barrier.clone();

        threads.push(tokio::spawn(async move {
            {
                let txn = reader_env.begin_ro_txn().await.unwrap();
                let db = txn.open_db(None).await.unwrap();
                assert_eq!(txn.get::<()>(db.dbi(), key).await.unwrap(), None);
            }
            reader_barrier.wait().await;
            reader_barrier.wait().await;
            {
                let txn = reader_env.begin_ro_txn().await.unwrap();
                let db = txn.open_db(None).await.unwrap();
                txn.get::<[u8; 3]>(db.dbi(), key).await.unwrap().unwrap() == *val
            }
        }));
    }

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();

    barrier.wait().await;
    txn.put(db.dbi(), key, val, WriteFlags::empty())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    barrier.wait().await;

    for hdl in threads {
        assert_eq!(hdl.await.unwrap(), true);
    }
}

async fn test_concurrent_writers(env: EnvironmentAny) {
    // tracing_subscriber::fmt::fmt()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .init();
    let n = 10usize; // Number of concurrent writers
    let mut threads: Vec<tokio::task::JoinHandle<bool>> = Vec::with_capacity(n);

    let key = "key";
    let val = "val";

    for i in 0..n {
        let writer_env = env.clone();

        threads.push(tokio::spawn(async move {
            let txn = writer_env.begin_rw_txn().await.unwrap();
            tracing::debug!("Open-ed txn...");
            let db = txn.open_db(None).await.unwrap();
            txn.put(
                db.dbi(),
                format!("{key}{i}").as_bytes(),
                format!("{val}{i}").as_bytes(),
                WriteFlags::empty(),
            )
            .await
            .unwrap();
            tracing::debug!("Will commit");
            txn.commit().await.is_ok()
        }));
    }

    for hdl in threads {
        assert!(hdl.await.unwrap());
    }

    let txn = env.begin_ro_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();

    for i in 0..n {
        assert_eq!(
            Cow::<Vec<u8>>::Owned(format!("{val}{i}").into_bytes()),
            txn.get(db.dbi(), format!("{key}{i}").as_bytes())
                .await
                .unwrap()
                .unwrap()
        );
    }
}

async fn test_stat(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.create_db(None, DatabaseFlags::empty()).await.unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    {
        let txn = env.begin_ro_txn().await.unwrap();
        let db = txn.open_db(None).await.unwrap();
        let stat = txn.db_stat(&db).await.unwrap();
        assert_eq!(stat.entries(), 3);
    }

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    txn.del(db.dbi(), b"key1", None).await.unwrap();
    txn.del(db.dbi(), b"key2", None).await.unwrap();
    txn.commit().await.unwrap();

    {
        let txn = env.begin_ro_txn().await.unwrap();
        let db = txn.open_db(None).await.unwrap();
        let stat = txn.db_stat(&db).await.unwrap();
        assert_eq!(stat.entries(), 1);
    }

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    txn.put(db.dbi(), b"key4", b"val4", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key5", b"val5", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key6", b"val6", WriteFlags::empty())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    {
        let txn = env.begin_ro_txn().await.unwrap();
        let db = txn.open_db(None).await.unwrap();
        let stat = txn.db_stat(&db).await.unwrap();
        assert_eq!(stat.entries(), 4);
    }
}

async fn test_stat_dupsort(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.create_db(None, DatabaseFlags::DUP_SORT).await.unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key1", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    {
        let txn = env.begin_ro_txn().await.unwrap();
        let stat = txn
            .db_stat(&txn.open_db(None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(stat.entries(), 9);
    }

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    txn.del(db.dbi(), b"key1", Some(b"val2")).await.unwrap();
    txn.del(db.dbi(), b"key2", None).await.unwrap();
    txn.commit().await.unwrap();

    {
        let txn = env.begin_ro_txn().await.unwrap();
        let stat = txn
            .db_stat(&txn.open_db(None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(stat.entries(), 5);
    }

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    txn.put(db.dbi(), b"key4", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key4", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key4", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.commit().await.unwrap();

    {
        let txn = env.begin_ro_txn().await.unwrap();
        let stat = txn
            .db_stat(&txn.open_db(None).await.unwrap())
            .await
            .unwrap();
        assert_eq!(stat.entries(), 8);
    }
}

local_remote!(test_put_get_del);
local_remote!(test_put_get_del_multi);
local_remote!(test_put_get_del_empty_key);
local_remote!(test_nested_txn);
local_remote!(test_clear_db);
local_remote!(test_concurrent_readers_single_writer);
local_remote!(test_concurrent_writers);
local_remote!(test_stat);
local_remote!(test_stat_dupsort);
