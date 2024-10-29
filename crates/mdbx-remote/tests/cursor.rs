#![allow(missing_docs)]
use std::borrow::Cow;

use any::{local_env, remote_env};
use futures_util::StreamExt;
use libmdbx_remote::*;
use paste::paste;
// use tokio_stream::StreamExt as TokioStreamExt;

mod any;

macro_rules! local_remote {
    ($test: tt) => {
        paste! {
            #[tokio::test(flavor = "multi_thread", worker_threads = 1)] // Intentionally reduce worker threads to expose deadlock issues
            async fn [<$test _remote>]() {
                let (db, env) = remote_env().await;
                $test(env).await;
                db.close().unwrap();
            }

            #[tokio::test(flavor = "multi_thread", worker_threads = 1)] // Intentionally reduce worker threads to expose deadlock issues
            async fn [<$test _local>]() {
                let (db, env) = local_env();
                $test(env).await;
                db.close().unwrap();
            }
        }
    };
}

async fn test_get(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();

    assert_eq!(
        None,
        txn.cursor(&db)
            .await
            .unwrap()
            .first::<(), ()>()
            .await
            .unwrap()
    );

    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key3", b"val3", WriteFlags::empty())
        .await
        .unwrap();

    let mut cursor = txn.cursor(&db).await.unwrap();
    assert_eq!(cursor.first().await.unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(
        cursor.get_current().await.unwrap(),
        Some((*b"key1", *b"val1"))
    );
    assert_eq!(cursor.next().await.unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.prev().await.unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.last().await.unwrap(), Some((*b"key3", *b"val3")));
    assert_eq!(cursor.set(b"key1").await.unwrap(), Some(*b"val1"));
    assert_eq!(
        cursor.set_key(b"key3").await.unwrap(),
        Some((*b"key3", *b"val3"))
    );
    assert_eq!(
        cursor.set_range(b"key2\0").await.unwrap(),
        Some((*b"key3", *b"val3"))
    );
}

async fn test_get_dup(env: EnvironmentAny) {
    // tracing_subscriber::fmt::fmt()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .init();
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

    let mut cursor = txn.cursor(&db).await.unwrap();
    assert_eq!(cursor.first().await.unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.first_dup().await.unwrap(), Some(*b"val1"));
    assert_eq!(
        cursor.get_current().await.unwrap(),
        Some((*b"key1", *b"val1"))
    );
    assert_eq!(
        cursor.next_nodup().await.unwrap(),
        Some((*b"key2", *b"val1"))
    );
    assert_eq!(cursor.next().await.unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.prev().await.unwrap(), Some((*b"key2", *b"val1")));
    assert_eq!(cursor.next_dup().await.unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.next_dup().await.unwrap(), Some((*b"key2", *b"val3")));
    assert_eq!(cursor.next_dup::<(), ()>().await.unwrap(), None);
    assert_eq!(cursor.prev_dup().await.unwrap(), Some((*b"key2", *b"val2")));
    assert_eq!(cursor.last_dup().await.unwrap(), Some(*b"val3"));
    assert_eq!(
        cursor.prev_nodup().await.unwrap(),
        Some((*b"key1", *b"val3"))
    );
    assert_eq!(cursor.next_dup::<(), ()>().await.unwrap(), None);
    assert_eq!(cursor.set(b"key1").await.unwrap(), Some(*b"val1"));
    assert_eq!(cursor.set(b"key2").await.unwrap(), Some(*b"val1"));
    assert_eq!(
        cursor.set_range(b"key1\0").await.unwrap(),
        Some((*b"key2", *b"val1"))
    );
    assert_eq!(
        cursor.get_both(b"key1", b"val3").await.unwrap(),
        Some(*b"val3")
    );
    assert_eq!(
        cursor.get_both_range::<()>(b"key1", b"val4").await.unwrap(),
        None
    );
    assert_eq!(
        cursor.get_both_range(b"key2", b"val").await.unwrap(),
        Some(*b"val1")
    );

    assert_eq!(cursor.last().await.unwrap(), Some((*b"key2", *b"val3")));
    cursor.del(WriteFlags::empty()).await.unwrap();
    assert_eq!(cursor.last().await.unwrap(), Some((*b"key2", *b"val2")));
    cursor.del(WriteFlags::empty()).await.unwrap();
    assert_eq!(cursor.last().await.unwrap(), Some((*b"key2", *b"val1")));
    cursor.del(WriteFlags::empty()).await.unwrap();
    assert_eq!(cursor.last().await.unwrap(), Some((*b"key1", *b"val3")));
}

async fn test_get_dupfixed(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn
        .create_db(None, DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED)
        .await
        .unwrap();
    txn.put(db.dbi(), b"key1", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key1", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key1", b"val3", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val4", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val5", WriteFlags::empty())
        .await
        .unwrap();
    txn.put(db.dbi(), b"key2", b"val6", WriteFlags::empty())
        .await
        .unwrap();

    let mut cursor = txn.cursor(&db).await.unwrap();
    assert_eq!(cursor.first().await.unwrap(), Some((*b"key1", *b"val1")));
    assert_eq!(cursor.get_multiple().await.unwrap(), Some(*b"val1val2val3"));
    assert_eq!(cursor.next_multiple::<(), ()>().await.unwrap(), None);
}

async fn test_iter(env: EnvironmentAny) {
    let items: Vec<(_, _)> = vec![
        (*b"key1", *b"val1"),
        (*b"key2", *b"val2"),
        (*b"key3", *b"val3"),
        (*b"key5", *b"val5"),
    ];

    {
        let txn = env.begin_rw_txn().await.unwrap();
        let db = txn.open_db(None).await.unwrap();
        for (key, data) in &items {
            txn.put(db.dbi(), key, data, WriteFlags::empty())
                .await
                .unwrap();
        }
        assert!(!txn.commit().await.unwrap().0);
    }

    let txn = env.begin_ro_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    let mut cursor = txn.cursor(&db).await.unwrap();

    // Because Result implements FromIterator, we can collect the iterator
    // of items of type Result<_, E> into a Result<Vec<_, E>> by specifying
    // the collection type via the turbofish syntax.
    assert_eq!(
        items,
        cursor.iter().map(|t| t.unwrap()).collect::<Vec<_>>().await
    );

    // Alternately, we can collect it into an appropriately typed variable.
    let retr: Vec<_> = cursor
        .iter_start()
        .map(|t| t.unwrap())
        .collect::<Vec<_>>()
        .await;
    assert_eq!(items, retr);

    cursor.set::<()>(b"key2").await.unwrap();
    // cnt variant
    assert_eq!(
        items.clone().into_iter().skip(2).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_cnt(64)
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.clone().into_iter().skip(2).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_cnt(1)
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.clone().into_iter().skip(2).collect::<Vec<_>>(),
        cursor.iter().map(|t| t.unwrap()).collect::<Vec<_>>().await
    );

    // cnt variant
    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_start_cnt(1)
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_start_cnt(64)
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items,
        cursor
            .iter_start()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items.clone().into_iter().skip(1).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_from_cnt(b"key2", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.clone().into_iter().skip(1).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_from_cnt(b"key2", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.clone().into_iter().skip(1).collect::<Vec<_>>(),
        cursor
            .iter_from(b"key2")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items.clone().into_iter().skip(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_from_cnt(b"key4", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.clone().into_iter().skip(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_from_cnt(b"key4", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.into_iter().skip(3).collect::<Vec<_>>(),
        cursor
            .iter_from(b"key4")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        Vec::<((), ())>::new(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_from_cnt(b"key6", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        Vec::<((), ())>::new(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_from_cnt(b"key6", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        Vec::<((), ())>::new(),
        cursor
            .iter_from(b"key6")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        Vec::<((), ())>::new(),
        cursor
            .iter_from(b"key6")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );
}

async fn test_iter_empty_database(env: EnvironmentAny) {
    let txn = env.begin_ro_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    let mut cursor = txn.cursor(&db).await.unwrap();

    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_cnt::<(), ()>(64)
        .next()
        .await
        .is_none());
    assert!(cursor.iter::<(), ()>().next().await.is_none());
    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_start_cnt::<(), ()>(64)
        .next()
        .await
        .is_none());
    assert!(cursor.iter_start::<(), ()>().next().await.is_none());
    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_from_cnt::<(), ()>(b"foo", 64)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
    assert!(cursor
        .iter_from::<(), ()>(b"foo")
        .await
        .unwrap()
        .next()
        .await
        .is_none());
}

async fn test_iter_empty_dup_database(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    txn.create_db(None, DatabaseFlags::DUP_SORT).await.unwrap();
    txn.commit().await.unwrap();

    let txn = env.begin_ro_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    let mut cursor = txn.cursor(&db).await.unwrap();

    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_cnt::<(), ()>(64)
        .next()
        .await
        .is_none());
    assert!(cursor.iter::<(), ()>().next().await.is_none());
    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_start_cnt::<(), ()>(64)
        .next()
        .await
        .is_none());
    assert!(cursor.iter_start::<(), ()>().next().await.is_none());
    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_from_cnt::<(), ()>(b"foo", 64)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
    assert!(cursor
        .iter_from::<(), ()>(b"foo")
        .await
        .unwrap()
        .next()
        .await
        .is_none());
    assert!(cursor
        .iter_from::<(), ()>(b"foo")
        .await
        .unwrap()
        .next()
        .await
        .is_none());

    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_dup_cnt::<(), ()>(64)
        .map(|t| t.unwrap())
        .flatten()
        .next()
        .await
        .is_none());
    assert!(cursor
        .iter_dup::<(), ()>()
        .map(|t| t.unwrap())
        .flatten()
        .next()
        .await
        .is_none());
    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_dup_start_cnt::<(), ()>(64)
        .map(|t| t.unwrap())
        .flatten()
        .next()
        .await
        .is_none());
    assert!(cursor
        .iter_dup_start::<(), ()>()
        .map(|t| t.unwrap())
        .flatten()
        .next()
        .await
        .is_none());
    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_dup_from_cnt::<(), ()>(b"foo", 64)
        .await
        .unwrap()
        .map(|t| t.unwrap())
        .flatten()
        .next()
        .await
        .is_none());
    assert!(cursor
        .iter_dup_from::<(), ()>(b"foo")
        .await
        .unwrap()
        .map(|t| t.unwrap())
        .flatten()
        .next()
        .await
        .is_none());
    assert!(cursor
        .cursor_clone()
        .await
        .unwrap()
        .into_iter_dup_of_cnt::<(), ()>(b"foo", 64)
        .await
        .unwrap()
        .next()
        .await
        .is_none());
    assert!(cursor
        .iter_dup_of::<(), ()>(b"foo")
        .await
        .unwrap()
        .next()
        .await
        .is_none());
}

async fn test_iter_dup(env: EnvironmentAny) {
    // tracing_subscriber::fmt::fmt()
    //     .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    //     .init();
    let txn = env.begin_rw_txn().await.unwrap();
    txn.create_db(None, DatabaseFlags::DUP_SORT).await.unwrap();
    txn.commit().await.unwrap();

    let items: Vec<(_, _)> = [
        (b"a", b"1"),
        (b"a", b"2"),
        (b"a", b"3"),
        (b"b", b"1"),
        (b"b", b"2"),
        (b"b", b"3"),
        (b"c", b"1"),
        (b"c", b"2"),
        (b"c", b"3"),
        (b"e", b"1"),
        (b"e", b"2"),
        (b"e", b"3"),
    ]
    .iter()
    .map(|&(&k, &v)| (k, v))
    .collect();

    {
        let txn = env.begin_rw_txn().await.unwrap();
        for (key, data) in items.clone() {
            let db = txn.open_db(None).await.unwrap();
            txn.put(db.dbi(), &key, &data, WriteFlags::empty())
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
    }

    let txn = env.begin_ro_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    let mut cursor = txn.cursor(&db).await.unwrap();
    // cnt variant
    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_cnt(1)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );
    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_cnt(64)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items,
        cursor
            .iter_dup()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    cursor.set::<()>(b"b").await.unwrap();
    // cnt variant
    assert_eq!(
        items.iter().copied().skip(4).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_cnt(1)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );
    assert_eq!(
        items.iter().copied().skip(4).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_cnt(64)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(4).collect::<Vec<_>>(),
        cursor
            .iter_dup()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_start_cnt(1)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );
    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_start_cnt(64)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items,
        cursor
            .iter_dup_start()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"b", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );
    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"b", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .iter_dup_from(b"b")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"ab", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"ab", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(3).collect::<Vec<_>>(),
        cursor
            .iter_dup_from(b"ab")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items.iter().copied().skip(9).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"d", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(9).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"d", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(9).collect::<Vec<_>>(),
        cursor
            .iter_dup_from(b"d")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        Vec::<([u8; 1], [u8; 1])>::new(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"f", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        Vec::<([u8; 1], [u8; 1])>::new(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_from_cnt(b"f", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        Vec::<([u8; 1], [u8; 1])>::new(),
        cursor
            .iter_dup_from(b"f")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items.iter().copied().skip(3).take(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt(b"b", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(3).take(3).collect::<Vec<_>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt(b"b", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().skip(3).take(3).collect::<Vec<_>>(),
        cursor
            .iter_dup_of(b"b")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        0,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt::<(), ()>(b"foo", 1)
            .await
            .unwrap()
            .count()
            .await
    );

    assert_eq!(
        0,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt::<(), ()>(b"foo", 64)
            .await
            .unwrap()
            .count()
            .await
    );
    assert_eq!(
        0,
        cursor
            .iter_dup_of::<(), ()>(b"foo")
            .await
            .unwrap()
            .count()
            .await
    );
}

async fn test_iter_del_get(env: EnvironmentAny) {
    let items = vec![(*b"a", *b"1"), (*b"b", *b"2")];
    {
        let txn = env.begin_rw_txn().await.unwrap();
        let db = txn.create_db(None, DatabaseFlags::DUP_SORT).await.unwrap();
        assert_eq!(
            txn.cursor(&db)
                .await
                .unwrap()
                .iter_dup_of::<(), ()>(b"a")
                .await
                .unwrap()
                .map(|t| t.unwrap())
                .collect::<Vec<_>>()
                .await
                .len(),
            0
        );
        txn.commit().await.unwrap();
    }

    {
        let txn = env.begin_rw_txn().await.unwrap();
        let db = txn.open_db(None).await.unwrap();
        for (key, data) in &items {
            txn.put(db.dbi(), key, data, WriteFlags::empty())
                .await
                .unwrap();
        }
        txn.commit().await.unwrap();
    }

    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    let mut cursor = txn.cursor(&db).await.unwrap();
    // cnt variant
    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_cnt(1)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items,
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_cnt(64)
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items,
        cursor
            .iter_dup()
            .map(|t| t.unwrap())
            .flatten()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    // cnt variant
    assert_eq!(
        items.iter().copied().take(1).collect::<Vec<(_, _)>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt(b"a", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );
    assert_eq!(
        items.iter().copied().take(1).collect::<Vec<(_, _)>>(),
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt(b"a", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(
        items.iter().copied().take(1).collect::<Vec<(_, _)>>(),
        cursor
            .iter_dup_of(b"a")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
    );

    assert_eq!(cursor.set(b"a").await.unwrap(), Some(*b"1"));

    cursor.del(WriteFlags::empty()).await.unwrap();

    // cnt variant
    assert_eq!(
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt::<(), ()>(b"a", 1)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
            .len(),
        0
    );

    assert_eq!(
        cursor
            .cursor_clone()
            .await
            .unwrap()
            .into_iter_dup_of_cnt::<(), ()>(b"a", 64)
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
            .len(),
        0
    );

    assert_eq!(
        cursor
            .iter_dup_of::<(), ()>(b"a")
            .await
            .unwrap()
            .map(|t| t.unwrap())
            .collect::<Vec<_>>()
            .await
            .len(),
        0
    );
}

async fn test_put_del(env: EnvironmentAny) {
    let txn = env.begin_rw_txn().await.unwrap();
    let db = txn.open_db(None).await.unwrap();
    let mut cursor = txn.cursor(&db).await.unwrap();

    cursor
        .put(b"key1", b"val1", WriteFlags::empty())
        .await
        .unwrap();
    cursor
        .put(b"key2", b"val2", WriteFlags::empty())
        .await
        .unwrap();
    cursor
        .put(b"key3", b"val3", WriteFlags::empty())
        .await
        .unwrap();

    assert_eq!(
        cursor.get_current().await.unwrap().unwrap(),
        (
            Cow::Borrowed(b"key3" as &[u8]),
            Cow::Borrowed(b"val3" as &[u8])
        )
    );

    cursor.del(WriteFlags::empty()).await.unwrap();
    assert_eq!(
        cursor.get_current::<Vec<u8>, Vec<u8>>().await.unwrap(),
        None
    );
    assert_eq!(
        cursor.last().await.unwrap().unwrap(),
        (
            Cow::Borrowed(b"key2" as &[u8]),
            Cow::Borrowed(b"val2" as &[u8])
        )
    );
}

local_remote!(test_get);
local_remote!(test_get_dup);
local_remote!(test_get_dupfixed);
local_remote!(test_iter);
local_remote!(test_iter_empty_database);
local_remote!(test_iter_empty_dup_database);
local_remote!(test_iter_dup);
local_remote!(test_iter_del_get);
local_remote!(test_put_del);
