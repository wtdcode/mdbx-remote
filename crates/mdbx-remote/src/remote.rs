use std::{
    future::Future,
    marker::PhantomData,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    }, time::Duration,
};

use async_stream::try_stream;
use ffi::{
    MDBX_FIRST, MDBX_FIRST_DUP, MDBX_GET_BOTH, MDBX_GET_BOTH_RANGE, MDBX_GET_CURRENT,
    MDBX_GET_MULTIPLE, MDBX_LAST, MDBX_LAST_DUP, MDBX_NEXT, MDBX_NEXT_DUP, MDBX_NEXT_MULTIPLE,
    MDBX_NEXT_NODUP, MDBX_PREV, MDBX_PREV_DUP, MDBX_PREV_MULTIPLE, MDBX_PREV_NODUP, MDBX_SET,
    MDBX_SET_KEY, MDBX_SET_LOWERBOUND, MDBX_SET_RANGE,
};
use tarpc::{client::NewClient, context::Context};
use thiserror::Error;
use tokio::{runtime::Handle, sync::oneshot};
use tokio_stream::Stream;

use crate::{
    environment::RemoteEnvironmentConfig,
    service::{RemoteMDBXClient, ServerError},
    CommitLatency, DatabaseFlags, EnvironmentBuilder, Stat, TableObject,
    TransactionKind, WriteFlags, RO, RW,
};

macro_rules! mdbx_try_optional {
    ($expr:expr) => {{
        match $expr {
            Err(ClientError::MDBX(crate::error::Error::NotFound | crate::error::Error::NoData)) => {
                return Ok(None)
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        }
    }};
}

fn escape_to_async<F, O>(fut: F) -> O
where
    F: Future<Output = O>,
{
    match Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(move || handle.block_on(fut)),
        Err(_) => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut),
    }
}

fn context_deadline(duration: Duration) -> Context {
    let mut ctx = Context::current();
    ctx.deadline = std::time::SystemTime::now() + duration;
    ctx
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("MDBX error: {0}")]
    MDBX(crate::error::Error),
    #[error("RPC error: {0}")]
    RPC(tarpc::client::RpcError),
    #[error("url parse error")]
    ParseError,
    #[error("IO error: {0}")]
    IO(std::io::Error),
    #[error("Server error: {0}")]
    Server(ServerError),
}

impl From<std::io::Error> for ClientError {
    fn from(value: std::io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<tarpc::client::RpcError> for ClientError {
    fn from(value: tarpc::client::RpcError) -> Self {
        Self::RPC(value)
    }
}

impl From<crate::error::Error> for ClientError {
    fn from(value: crate::error::Error) -> Self {
        Self::MDBX(value)
    }
}

impl From<ServerError> for ClientError {
    fn from(value: ServerError) -> Self {
        match value {
            ServerError::MBDX(e) => Self::MDBX(e),
            _ => Self::Server(value),
        }
    }
}

type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug)]
pub(crate) struct RemoteEnvironmentInner {
    handle: u64,
    cl: RemoteMDBXClient,
    deadline: Duration,
    ch: oneshot::Sender<()>,
}

impl RemoteEnvironmentInner {
    fn context(&self) -> Context {
        context_deadline(self.deadline)
    }

    async fn new<D, E>(
        path: PathBuf,
        builder: EnvironmentBuilder,
        client: RemoteMDBXClient,
        dispatcher: D,
        deadline: Duration
    ) -> Result<Self>
    where
        D: Future<Output = std::result::Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        // Spin dispatcher
        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            tokio::select! {
                _ = rx => {
                    tracing::info!("Exiting from dispatcher due to closer");
                },
                e = dispatcher => {
                    tracing::warn!("Dispatcher exits with {:?}", e);
                }
            }
            tracing::debug!("Dispatcher dies");
        });
        let remote = RemoteEnvironmentConfig::from(builder);
        let handle = client.open_env(context_deadline(deadline), path, remote).await??;
        Ok(Self {
            handle: handle,
            cl: client,
            deadline: deadline,
            ch: tx,
        })
    }

    async fn close(&self) -> Result<()> {
        self.cl.env_close(self.context(), self.handle).await??;
        Ok(())
    }
}

impl Drop for RemoteEnvironmentInner {
    fn drop(&mut self) {
        if let Err(e) = escape_to_async(self.close()) {
            tracing::warn!("Fail to close env {} due to {}", self.handle, e);
        }
        let (mut t, _) = oneshot::channel();
        std::mem::swap(&mut self.ch, &mut t); // ???
        if let Err(_) = t.send(()) {
            tracing::warn!("Fail to close dispatcher");
        }
    }
}

#[derive(Clone, Debug)]
pub struct RemoteEnvironment {
    inner: Arc<RemoteEnvironmentInner>,
}

impl RemoteEnvironment {
    pub async fn open_with_builder<D, E>(
        path: PathBuf,
        builder: EnvironmentBuilder,
        client: NewClient<RemoteMDBXClient, D>,
        deadline: Duration
    ) -> Result<Self>
    where
        D: Future<Output = std::result::Result<(), E>> + Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Ok(Self {
            inner: Arc::new(
                RemoteEnvironmentInner::new(path, builder, client.client, client.dispatch, deadline).await?,
            ),
        })
    }

    pub async fn begin_ro_txn(&self) -> Result<RemoteTransaction<RO>> {
        let tx = self
            .inner
            .cl
            .env_ro_tx(self.inner.context(), self.inner.handle)
            .await??;
        Ok(RemoteTransaction::new(self.inner.clone(), tx))
    }

    pub async fn begin_rw_txn(&self) -> Result<RemoteTransaction<RW>> {
        let tx = self
            .inner
            .cl
            .env_rw_tx(self.inner.context(), self.inner.handle)
            .await??;
        Ok(RemoteTransaction::new(self.inner.clone(), tx))
    }

    pub async fn sync(&self, force: bool) -> Result<bool> {
        Ok(self
            .inner
            .cl
            .env_sync(self.inner.context(), self.inner.handle, force)
            .await??)
    }

    pub async fn stat(&self) -> Result<Stat> {
        Ok(self
            .inner
            .cl
            .env_stat(self.inner.context(), self.inner.handle)
            .await??)
    }
}

#[derive(Debug, Clone)]
struct RemoteDatabaseInner {
    dbi: u32,
    _env: Arc<RemoteEnvironmentInner>,
}

#[derive(Debug, Clone)]
pub struct RemoteDatabase {
    inner: Arc<RemoteDatabaseInner>,
}

impl RemoteDatabase {
    pub fn dbi(&self) -> u32 {
        self.inner.dbi
    }
}

#[derive(Debug, Clone)]
struct RemoteTransactionInner<K: TransactionKind> {
    env: Arc<RemoteEnvironmentInner>,
    handle: u64,
    committed: Arc<AtomicBool>,
    _ph: PhantomData<K>,
}

impl<K: TransactionKind> RemoteTransactionInner<K> {
    async fn close(&self) -> Result<()> {
        let commited = self.committed.load(Ordering::SeqCst);
        if !commited && !K::IS_READ_ONLY {
            self.env
                .cl
                .tx_abort(self.env.context(), self.env.handle, self.handle)
                .await??;
        }
        Ok(())
    }
}

impl<K: TransactionKind> Drop for RemoteTransactionInner<K> {
    fn drop(&mut self) {
        if let Err(e) = escape_to_async(self.close()) {
            tracing::warn!("Fail to close tx {} due to {}", self.handle, e);
        }
    }
}

#[derive(Debug, Clone)]
pub struct RemoteTransaction<K: TransactionKind> {
    inner: Arc<RemoteTransactionInner<K>>,
}

impl<K: TransactionKind> RemoteTransaction<K> {
    pub(crate) fn new(env: Arc<RemoteEnvironmentInner>, handle: u64) -> Self {
        Self {
            inner: Arc::new(RemoteTransactionInner {
                env: env,
                handle: handle,
                committed: Arc::new(AtomicBool::new(false)),
                _ph: PhantomData::default(),
            }),
        }
    }

    async fn open_db_with_flags(
        &self,
        db: Option<String>,
        flags: DatabaseFlags,
    ) -> Result<RemoteDatabase> {
        let dbi = self
            .inner
            .env
            .cl
            .tx_create_db(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                db,
                flags.bits(),
            )
            .await??;

        Ok(RemoteDatabase {
            inner: Arc::new(RemoteDatabaseInner {
                dbi: dbi,
                _env: self.inner.env.clone(),
            }),
        })
    }

    pub async fn db_stat_with_dbi(&self, dbi: u32) -> Result<Stat> {
        let stat = self
            .inner
            .env
            .cl
            .tx_db_stat(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                dbi,
            )
            .await??;

        Ok(stat)
    }

    pub async fn db_stat(&self, db: &RemoteDatabase) -> Result<Stat> {
        self.db_stat_with_dbi(db.dbi()).await
    }

    pub async fn open_db(&self, db: Option<String>) -> Result<RemoteDatabase> {
        self.open_db_with_flags(db, DatabaseFlags::empty()).await
    }

    pub async fn get<V: TableObject>(&self, dbi: u32, key: Vec<u8>) -> Result<Option<V>> {
        let v = self
            .inner
            .env
            .cl
            .tx_get(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                dbi,
                key,
            )
            .await??;

        v.map(|t| V::decode(&t)).transpose().map_err(|e| e.into())
    }
}

impl RemoteTransaction<RO> {
    pub async fn cursor(&self, dbi: u32) -> Result<RemoteCursor<RO>> {
        let cur = self
            .inner
            .env
            .cl
            .tx_ro_cursor(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                dbi,
            )
            .await??;

        Ok(RemoteCursor {
            inner: Arc::new(RemoteCursorInner {
                tx: self.inner.clone(),
                handle: cur,
            }),
        })
    }
}

impl RemoteTransaction<RW> {
    pub async fn begin_nested_txn(&mut self) -> Result<Self> {
        let handle = self
            .inner
            .env
            .cl
            .tx_nested(self.inner.env.context(), self.inner.env.handle, self.inner.handle)
            .await??;
        Ok(Self::new(self.inner.env.clone(), handle))
    }

    pub async fn clear_db(&self, dbi: u32) -> Result<()> {
        self.inner
            .env
            .cl
            .clear_db(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                dbi,
            )
            .await??;

        Ok(())
    }

    pub async fn put(
        &self,
        dbi: u32,
        key: Vec<u8>,
        data: Vec<u8>,
        flags: WriteFlags,
    ) -> Result<()> {
        self.inner
            .env
            .cl
            .tx_put(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                dbi,
                key,
                data,
                flags.bits(),
            )
            .await??;
        Ok(())
    }

    pub async fn del(&self, dbi: u32, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<bool> {
        let ret = self
            .inner
            .env
            .cl
            .tx_del(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                dbi,
                key,
                value,
            )
            .await??;
        Ok(ret)
    }

    pub async fn create_db(
        &self,
        db: Option<String>,
        flags: DatabaseFlags,
    ) -> Result<RemoteDatabase> {
        self.open_db_with_flags(db, flags | DatabaseFlags::CREATE)
            .await
    }

    pub async fn cursor(&self, dbi: u32) -> Result<RemoteCursor<RW>> {
        let cur = self
            .inner
            .env
            .cl
            .tx_rw_cursor(
                self.inner.env.context(),
                self.inner.env.handle,
                self.inner.handle,
                dbi,
            )
            .await??;

        Ok(RemoteCursor {
            inner: Arc::new(RemoteCursorInner {
                tx: self.inner.clone(),
                handle: cur,
            }),
        })
    }

    pub async fn commit(self) -> Result<(bool, CommitLatency)> {
        tracing::debug!("going to commit tx {}", self.inner.handle);
        let (ret, lat) = self
            .inner
            .env
            .cl
            .tx_commit(self.inner.env.context(), self.inner.env.handle, self.inner.handle)
            .await??;
        self.inner.committed.store(true, Ordering::SeqCst);
        Ok((ret, lat))
    }
}

#[derive(Debug, Clone)]
struct RemoteCursorInner<K: TransactionKind> {
    tx: Arc<RemoteTransactionInner<K>>,
    handle: u64,
}

impl<K: TransactionKind> RemoteCursorInner<K> {
    async fn close(&self) -> Result<()> {
        self.tx
            .env
            .cl
            .cur_close(
                self.tx.env.context(),
                self.tx.env.handle,
                self.tx.handle,
                self.handle,
            )
            .await??;
        Ok(())
    }

    async fn cur_clone(&self) -> Result<Self> {
        let new_cur = self
            .tx
            .env
            .cl
            .cur_create(
                self.tx.env.context(),
                self.tx.env.handle,
                self.tx.handle,
                self.handle,
            )
            .await??;
        Ok(Self {
            tx: self.tx.clone(),
            handle: new_cur,
        })
    }
}

impl<K: TransactionKind> Drop for RemoteCursorInner<K> {
    fn drop(&mut self) {
        if let Err(e) = escape_to_async(self.close()) {
            tracing::warn!(
                "Fail to close cursor {} of tx {} from env {} due to {}",
                self.handle,
                self.tx.handle,
                self.tx.env.handle,
                e
            );
        }
    }
}

#[derive(Debug)]
pub struct RemoteCursor<K: TransactionKind> {
    inner: Arc<RemoteCursorInner<K>>,
}

impl<K: TransactionKind> RemoteCursor<K> {
    pub async fn cur_clone(&self) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(self.inner.cur_clone().await?),
        })
    }

    async fn get<Key: TableObject, V: TableObject>(
        &self,
        key: Option<Vec<u8>>,
        data: Option<Vec<u8>>,
        op: u32,
    ) -> Result<(Option<Key>, V, bool)> {
        let tp = self
            .inner
            .tx
            .env
            .cl
            .cur_get(
                self.inner.tx.env.context(),
                self.inner.tx.env.handle,
                self.inner.tx.handle,
                self.inner.handle,
                key,
                data,
                op,
            )
            .await??;
        let key_out = tp.0.map(|t| Key::decode(&t)).transpose()?;
        let val_out = V::decode(&tp.1)?;
        Ok((key_out, val_out, tp.2))
    }

    async fn get_value<V: TableObject>(
        &mut self,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        op: u32,
    ) -> Result<Option<V>> {
        let (_, v, _) = mdbx_try_optional!(self.get::<(), V>(key, value, op).await);

        Ok(Some(v))
    }

    async fn get_full<Key: TableObject, V: TableObject>(
        &mut self,
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
        op: u32,
    ) -> Result<Option<(Key, V)>> {
        let (k, v, _) = mdbx_try_optional!(self.get::<Key, V>(key, value, op).await);

        Ok(Some((k.unwrap(), v)))
    }

    pub async fn first<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_FIRST).await
    }

    pub async fn first_dup<Value>(&mut self) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        self.get_value(None, None, MDBX_FIRST_DUP).await
    }

    pub async fn get_both<Value>(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        self.get_value(Some(k), Some(v), MDBX_GET_BOTH).await
    }

    pub async fn get_both_range<Value>(&mut self, k: Vec<u8>, v: Vec<u8>) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        self.get_value(Some(k), Some(v), MDBX_GET_BOTH_RANGE).await
    }

    pub async fn get_current<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_GET_CURRENT).await
    }

    pub async fn get_multiple<Value>(&mut self) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        self.get_value(None, None, MDBX_GET_MULTIPLE).await
    }

    pub async fn last<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_LAST).await
    }

    pub async fn last_dup<Value>(&mut self) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        self.get_value(None, None, MDBX_LAST_DUP).await
    }

    pub async fn next<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_NEXT).await
    }

    pub async fn next_dup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_NEXT_DUP).await
    }

    pub async fn next_multiple<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_NEXT_MULTIPLE).await
    }

    pub async fn next_nodup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_NEXT_NODUP).await
    }

    pub async fn prev<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_PREV).await
    }

    pub async fn prev_dup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_PREV_DUP).await
    }

    pub async fn prev_nodup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_PREV_NODUP).await
    }

    pub async fn set<Value>(&mut self, key: Vec<u8>) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        self.get_value(Some(key), None, MDBX_SET).await
    }
    pub async fn set_key<Key, Value>(&mut self, key: Vec<u8>) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(Some(key), None, MDBX_SET_KEY).await
    }

    pub async fn set_range<Key, Value>(&mut self, key: Vec<u8>) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(Some(key), None, MDBX_SET_RANGE).await
    }

    pub async fn prev_multiple<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        self.get_full(None, None, MDBX_PREV_MULTIPLE).await
    }

    pub async fn set_lowerbound<Key, Value>(
        &mut self,
        key: Vec<u8>,
    ) -> Result<Option<(bool, Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        let (k, v, found) =
            mdbx_try_optional!(self.get(Some(key), None, MDBX_SET_LOWERBOUND).await);

        Ok(Some((found, k.unwrap(), v)))
    }

    fn stream_iter<'a, Key: TableObject + Send + 'a, Value: TableObject + Send + 'a>(
        inner: Arc<RemoteCursorInner<K>>,
        op: u32,
        next_op: u32,
    ) -> Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>> {
        let st = try_stream! {
            let cur = inner;
            let mut op = op;
            let next_op = next_op;
            loop {
                let op = std::mem::replace(&mut op, next_op);

                let ret = match cur.tx.env.cl.cur_get(cur.tx.env.context(), cur.tx.env.handle, cur.tx.handle, cur.handle, None, None, op).await? {
                    Ok(v) => {
                        let k = Key::decode(&v.0.unwrap_or(Vec::new()))?;
                        let v = Value::decode(&v.1)?;

                        Ok((k, v))
                    },
                    Err(ServerError::MBDX(crate::Error::NoData | crate::Error::NotFound)) => break,
                    Err(e) => {
                        Err(ClientError::from(e))
                    }
                };

                let ret = ret?;
                yield ret;
            }
        };

        Box::pin(st)
    }

    fn stream_iter_dup<'a, Key: TableObject + Send + 'a, Value: TableObject + Send + 'a>(
        inner: Arc<RemoteCursorInner<K>>,
        op: u32,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>>>
                + Send
                + 'a,
        >,
    > {
        let st = try_stream! {
            let cur = inner;
            let mut op = op;
            loop {
                let op = std::mem::replace(&mut op, MDBX_NEXT_NODUP);

                tracing::info!("iter_dup_stream cur_get");
                let ret = match cur.tx.env.cl.cur_get(cur.tx.env.context(), cur.tx.env.handle, cur.tx.handle, cur.handle, None, None, op).await? {
                    Ok(_) => {
                        let new_cur = cur.cur_clone().await?;
                        Ok(Self::stream_iter::<'a, Key, Value>(Arc::new(new_cur), MDBX_GET_CURRENT, MDBX_NEXT_DUP))
                    },
                    Err(ServerError::MBDX(crate::Error::NoData | crate::Error::NotFound)) => break,
                    Err(e) => {
                        Err(ClientError::from(e))
                    }
                };

                let ret = ret?;
                yield ret;
            }
        };

        Box::pin(st)
    }

    pub fn iter<'a, Key, Value>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        Self::stream_iter(self.inner.clone(), MDBX_NEXT, MDBX_NEXT)
    }

    pub fn iter_start<'a, Key, Value>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        Self::stream_iter(self.inner.clone(), MDBX_FIRST, MDBX_NEXT)
    }

    pub async fn iter_from<'a, Key, Value>(
        &'a mut self,
        key: Vec<u8>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        let _ = self.set_range::<(), ()>(key).await?;

        Ok(Self::stream_iter(
            self.inner.clone(),
            MDBX_GET_CURRENT,
            MDBX_NEXT,
        ))
    }

    pub fn iter_dup<'a, Key, Value>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>>>
                + Send
                + 'a,
        >,
    >
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        Self::stream_iter_dup(self.inner.clone(), MDBX_NEXT)
    }

    pub fn iter_dup_start<'a, Key, Value>(
        &'a mut self,
    ) -> Pin<
        Box<
            dyn Stream<Item = Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>>>
                + Send
                + 'a,
        >,
    >
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        Self::stream_iter_dup(self.inner.clone(), MDBX_FIRST)
    }

    pub async fn iter_dup_from<'a, Key, Value>(
        &'a mut self,
        key: Vec<u8>,
    ) -> Result<
        Pin<
            Box<
                dyn Stream<
                        Item = Result<
                            Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>,
                        >,
                    > + Send
                    + 'a,
            >,
        >,
    >
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        let _ = self.set_range::<(), ()>(key).await?;
        Ok(Self::stream_iter_dup(self.inner.clone(), MDBX_GET_CURRENT))
    }

    pub async fn iter_dup_of<'a, Key, Value>(
        &'a mut self,
        key: Vec<u8>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        let res = self.set::<()>(key).await?;
        if let Some(_) = res {
            Ok(Self::stream_iter(
                self.inner.clone(),
                MDBX_GET_CURRENT,
                MDBX_NEXT_DUP,
            ))
        } else {
            let _ = self.last::<(), ()>().await?;
            Ok(Self::stream_iter(self.inner.clone(), MDBX_NEXT, MDBX_NEXT))
        }
    }
}

impl RemoteCursor<RW> {
    pub async fn put(&mut self, key: Vec<u8>, data: Vec<u8>, flags: WriteFlags) -> Result<()> {
        self.inner
            .tx
            .env
            .cl
            .cur_put(
                self.inner.tx.env.context(),
                self.inner.tx.env.handle,
                self.inner.tx.handle,
                self.inner.handle,
                key,
                data,
                flags.bits(),
            )
            .await??;
        Ok(())
    }

    pub async fn del(&mut self, flags: WriteFlags) -> Result<()> {
        self.inner
            .tx
            .env
            .cl
            .cur_del(
                self.inner.tx.env.context(),
                self.inner.tx.env.handle,
                self.inner.tx.handle,
                self.inner.handle,
                flags.bits(),
            )
            .await??;
        Ok(())
    }
}
