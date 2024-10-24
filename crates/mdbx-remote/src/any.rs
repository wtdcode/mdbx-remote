use std::{
    collections::HashMap, net::SocketAddr, path::PathBuf, pin::Pin, str::FromStr, time::Duration,
};

use async_stream::try_stream;
use tokio_stream::Stream;

use crate::{
    remote::{ClientError, RemoteCursor, RemoteDatabase, RemoteEnvironment, RemoteTransaction},
    service::RemoteMDBXClient,
    CommitLatency, Cursor, Database, DatabaseFlags, Environment, EnvironmentFlags, Mode, Stat,
    TableObject, Transaction, TransactionKind, WriteFlags, RO, RW,
};

type Result<T> = std::result::Result<T, ClientError>;

#[derive(Debug, Clone)]
pub enum EnvironmentAny {
    Local(Environment),
    Remote(RemoteEnvironment),
}

impl EnvironmentAny {
    pub async fn open(url: &url::Url) -> Result<Self> {
        let mut builder = Environment::builder();

        let args: HashMap<String, String> = url
            .query_pairs()
            .into_iter()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();

        let mode = if args.contains_key("ro") {
            Mode::ReadOnly
        } else if args.contains_key("rw") {
            Mode::ReadWrite {
                sync_mode: crate::SyncMode::Durable,
            }
        } else {
            Mode::ReadOnly
        };

        let exclusive = args.contains_key("exclusive");
        let accede = args.contains_key("accede");
        let no_sub_dir = args.contains_key("no_sub_dir");
        let flags = EnvironmentFlags {
            mode,
            exclusive,
            accede,
            no_sub_dir,
            ..Default::default()
        };

        let max_readers = args
            .get("max_readers")
            .map(|t| u64::from_str_radix(&t, 10))
            .transpose()
            .map_err(|_| ClientError::ParseError)?
            .unwrap_or(256);
        let max_dbs = args
            .get("max_dbs")
            .map(|t| usize::from_str_radix(&t, 10))
            .transpose()
            .map_err(|_| ClientError::ParseError)?
            .unwrap_or(256);
        let sync_bytes = args
            .get("sync_bytes")
            .map(|t| usize::from_str_radix(&t, 10))
            .transpose()
            .map_err(|_| ClientError::ParseError)?;
        let sync_period = args
            .get("sync_period")
            .map(|t| u64::from_str_radix(&t, 10))
            .transpose()
            .map_err(|_| ClientError::ParseError)?;

        builder
            .set_flags(flags)
            .set_max_dbs(max_dbs)
            .set_max_readers(max_readers);

        if let Some(sync_bytes) = sync_bytes {
            builder.set_sync_bytes(sync_bytes);
        }

        if let Some(sync_period) = sync_period {
            builder.set_sync_period(Duration::from_secs(sync_period));
        }

        let deadline = args
            .get("deadline")
            .map(|t| u64::from_str_radix(&t, 10))
            .transpose()
            .map_err(|_| ClientError::ParseError)?
            .map(|t| Duration::from_secs(t))
            .unwrap_or(Duration::from_secs(30));

        match url.scheme() {
            "file" => {
                let fpath = PathBuf::from(url.path());
                let env = builder.open(&fpath)?;
                Ok(Self::Local(env))
            }
            "mdbx" => {
                let fpath = PathBuf::from(url.path());
                if let Some(host) = url.host_str() {
                    let target = format!(
                        "{}:{}",
                        host,
                        url.port().unwrap_or(1899)
                    );
                    let addr = SocketAddr::from_str(&target).map_err(|_| ClientError::ParseError)?;
                    let transport = tarpc::serde_transport::tcp::connect(
                        addr,
                        tarpc::tokio_serde::formats::Bincode::default,
                    )
                    .await?;
                    let client = RemoteMDBXClient::new(tarpc::client::Config::default(), transport);
                    let env = RemoteEnvironment::open_with_builder(fpath, builder, client, deadline).await?;
                    Ok(Self::Remote(env))
                } else {
                    

                    let env = builder.open(&fpath)?;
                    Ok(Self::Local(env))
                }
            
            }
            _ => Err(ClientError::ParseError),
        }
    }

    pub async fn begin_ro_txn(&self) -> Result<TransactionAny<RO>> {
        match self {
            Self::Local(env) => Ok(TransactionAny::Local(env.begin_ro_txn()?)),
            Self::Remote(env) => Ok(TransactionAny::Remote(env.begin_ro_txn().await?)),
        }
    }

    pub async fn begin_rw_txn(&self) -> Result<TransactionAny<RW>> {
        match self {
            Self::Local(env) => Ok(TransactionAny::Local(env.begin_rw_txn()?)),
            Self::Remote(env) => Ok(TransactionAny::Remote(env.begin_rw_txn().await?)),
        }
    }

    pub async fn sync(&self, force: bool) -> Result<bool> {
        match self {
            Self::Local(env) => Ok(env.sync(force)?),
            Self::Remote(env) => Ok(env.sync(force).await?),
        }
    }

    pub async fn stat(&self) -> Result<Stat> {
        match self {
            Self::Local(env) => Ok(env.stat()?),
            Self::Remote(env) => Ok(env.stat().await?),
        }
    }
}

#[derive(Debug)]
pub enum DatabaseAny {
    Local(Database),
    Remote(RemoteDatabase),
}

impl DatabaseAny {
    pub fn dbi(&self) -> u32 {
        match self {
            Self::Local(db) => db.dbi(),
            Self::Remote(db) => db.dbi(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum TransactionAny<K: TransactionKind> {
    Local(Transaction<K>),
    Remote(RemoteTransaction<K>),
}

impl<K: TransactionKind> TransactionAny<K> {
    pub async fn open_db(&self, db: Option<String>) -> Result<DatabaseAny> {
        match self {
            Self::Local(tx) => Ok(DatabaseAny::Local(
                tx.open_db(db.as_ref().map(|t| t.as_str()))?,
            )),
            Self::Remote(tx) => Ok(DatabaseAny::Remote(tx.open_db(db).await?)),
        }
    }

    pub async fn get<V: TableObject>(&self, dbi: u32, key: &[u8]) -> Result<Option<V>> {
        match self {
            Self::Local(tx) => Ok(tx.get::<V>(dbi, key)?),
            Self::Remote(tx) => Ok(tx.get::<V>(dbi, key.to_vec()).await?),
        }
    }

    pub async fn db_stat(&self, db: &DatabaseAny) -> Result<Stat> {
        self.db_stat_with_dbi(db.dbi()).await
    }

    pub async fn db_stat_with_dbi(&self, dbi: u32) -> Result<Stat> {
        match self {
            Self::Local(tx) => Ok(tx.db_stat_with_dbi(dbi)?),
            Self::Remote(tx) => Ok(tx.db_stat_with_dbi(dbi).await?),
        }
    }
}

impl TransactionAny<RO> {
    pub async fn cursor(&self, db: &DatabaseAny) -> Result<CursorAny<RO>> {
        self.cursor_with_dbi(db.dbi()).await
    }

    pub async fn cursor_with_dbi(&self, dbi: u32) -> Result<CursorAny<RO>> {
        match self {
            Self::Local(tx) => Ok(CursorAny::Local(tx.cursor_with_dbi(dbi)?)),
            Self::Remote(tx) => Ok(CursorAny::Remote(tx.cursor(dbi).await?)),
        }
    }
}

impl TransactionAny<RW> {
    pub async fn begin_nested_txn(&mut self) -> Result<Self> {
        match self {
            Self::Local(tx) => Ok(Self::Local(tx.begin_nested_txn()?)),
            Self::Remote(tx) => Ok(Self::Remote(tx.begin_nested_txn().await?)),
        }
    }

    pub async fn clear_db(&self, dbi: u32) -> Result<()> {
        match self {
            Self::Local(tx) => Ok(tx.clear_db(dbi)?),
            Self::Remote(tx) => Ok(tx.clear_db(dbi).await?),
        }
    }

    pub async fn put(&self, dbi: u32, key: &[u8], data: &[u8], flags: WriteFlags) -> Result<()> {
        match self {
            Self::Local(tx) => Ok(tx.put(dbi, key, data, flags)?),
            Self::Remote(tx) => Ok(tx.put(dbi, key.to_vec(), data.to_vec(), flags).await?),
        }
    }

    pub async fn del(&self, dbi: u32, key: &[u8], value: Option<&[u8]>) -> Result<bool> {
        match self {
            Self::Local(tx) => Ok(tx.del(dbi, key, value)?),
            Self::Remote(tx) => Ok(tx.del(dbi, key.to_vec(), value.map(|t| t.to_vec())).await?),
        }
    }

    pub async fn create_db(&self, db: Option<String>, flags: DatabaseFlags) -> Result<DatabaseAny> {
        match self {
            Self::Local(tx) => Ok(DatabaseAny::Local(
                tx.create_db(db.as_ref().map(|t| t.as_str()), flags)?,
            )),
            Self::Remote(tx) => Ok(DatabaseAny::Remote(tx.create_db(db, flags).await?)),
        }
    }

    pub async fn cursor(&self, db: &DatabaseAny) -> Result<CursorAny<RW>> {
        self.cursor_with_dbi(db.dbi()).await
    }

    pub async fn cursor_with_dbi(&self, dbi: u32) -> Result<CursorAny<RW>> {
        match self {
            Self::Local(tx) => Ok(CursorAny::Local(tx.cursor_with_dbi(dbi)?)),
            Self::Remote(tx) => Ok(CursorAny::Remote(tx.cursor(dbi).await?)),
        }
    }
    pub async fn commit(self) -> Result<(bool, CommitLatency)> {
        match self {
            Self::Local(tx) => Ok(tx.commit()?),
            Self::Remote(tx) => Ok(tx.commit().await?),
        }
    }
}

#[derive(Debug)]
pub enum CursorAny<K: TransactionKind> {
    Local(Cursor<K>),
    Remote(RemoteCursor<K>),
}

impl<K: TransactionKind> CursorAny<K> {
    pub async fn first<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.first()?),
            Self::Remote(cur) => Ok(cur.first().await?),
        }
    }

    pub async fn first_dup<Value>(&mut self) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.first_dup()?),
            Self::Remote(cur) => Ok(cur.first_dup().await?),
        }
    }

    pub async fn get_both<Value>(&mut self, k: &[u8], v: &[u8]) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.get_both(k, v)?),
            Self::Remote(cur) => Ok(cur.get_both(k.to_vec(), v.to_vec()).await?),
        }
    }

    pub async fn get_both_range<Value>(&mut self, k: &[u8], v: &[u8]) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.get_both_range(k, v)?),
            Self::Remote(cur) => Ok(cur.get_both_range(k.to_vec(), v.to_vec()).await?),
        }
    }

    pub async fn get_current<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.get_current()?),
            Self::Remote(cur) => Ok(cur.get_current().await?),
        }
    }

    pub async fn get_multiple<Value>(&mut self) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.get_multiple()?),
            Self::Remote(cur) => Ok(cur.get_multiple().await?),
        }
    }

    pub async fn last<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.last()?),
            Self::Remote(cur) => Ok(cur.last().await?),
        }
    }

    pub async fn last_dup<Value>(&mut self) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.last_dup()?),
            Self::Remote(cur) => Ok(cur.last_dup().await?),
        }
    }

    pub async fn next<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.next()?),
            Self::Remote(cur) => Ok(cur.next().await?),
        }
    }

    pub async fn next_dup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.next_dup()?),
            Self::Remote(cur) => Ok(cur.next_dup().await?),
        }
    }

    pub async fn next_multiple<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.next_multiple()?),
            Self::Remote(cur) => Ok(cur.next_multiple().await?),
        }
    }

    pub async fn next_nodup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.next_nodup()?),
            Self::Remote(cur) => Ok(cur.next_nodup().await?),
        }
    }

    pub async fn prev<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.prev()?),
            Self::Remote(cur) => Ok(cur.prev().await?),
        }
    }

    pub async fn prev_dup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.prev_dup()?),
            Self::Remote(cur) => Ok(cur.prev_dup().await?),
        }
    }

    pub async fn prev_nodup<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.prev_nodup()?),
            Self::Remote(cur) => Ok(cur.prev_nodup().await?),
        }
    }

    pub async fn set<Value>(&mut self, key: &[u8]) -> Result<Option<Value>>
    where
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.set(key)?),
            Self::Remote(cur) => Ok(cur.set(key.to_vec()).await?),
        }
    }
    pub async fn set_key<Key, Value>(&mut self, key: &[u8]) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.set_key(key)?),
            Self::Remote(cur) => Ok(cur.set_key(key.to_vec()).await?),
        }
    }

    pub async fn set_range<Key, Value>(&mut self, key: &[u8]) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.set_range(key)?),
            Self::Remote(cur) => Ok(cur.set_range(key.to_vec()).await?),
        }
    }

    pub async fn prev_multiple<Key, Value>(&mut self) -> Result<Option<(Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.prev_multiple()?),
            Self::Remote(cur) => Ok(cur.prev_multiple().await?),
        }
    }

    pub async fn set_lowerbound<Key, Value>(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(bool, Key, Value)>>
    where
        Key: TableObject,
        Value: TableObject,
    {
        match self {
            Self::Local(cur) => Ok(cur.set_lowerbound(key)?),
            Self::Remote(cur) => Ok(cur.set_lowerbound(key.to_vec()).await?),
        }
    }

    fn iter_to_stream<'cur, Key, Value>(
        itr: crate::cursor::Iter<'cur, K, Key, Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'cur>>
    where
        Key: TableObject + Send + 'cur,
        Value: TableObject + Send + 'cur,
    {
        Box::pin(try_stream! {
            for it in itr {
                let (k, v) = it?;
                yield (k, v);
            }
        })
    }

    fn intoiter_to_stream<'cur, Key, Value>(
        itr: crate::cursor::IntoIter<'cur, K, Key, Value>,
    ) -> Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'cur>>
    where
        Key: TableObject + Send + 'cur,
        Value: TableObject + Send + 'cur,
    {
        Box::pin(try_stream! {
            for it in itr {
                let (k, v) = it?;
                yield (k, v);
            }
        })
    }

    fn iterdup_to_steam<'cur, Key, Value>(
        iterdup: crate::cursor::IterDup<'cur, K, Key, Value>,
    ) -> Pin<
        Box<
            dyn Stream<
                    Item = Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'cur>>>,
                > + Send
                + 'cur,
        >,
    >
    where
        Key: TableObject + Send + 'cur,
        Value: TableObject + Send + 'cur,
    {
        Box::pin(try_stream! {
            for it in iterdup {
                let st = Self::intoiter_to_stream(it);
                yield st;
            }
        })
    }

    pub fn iter<'a, Key, Value>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        match self {
            Self::Local(cur) => Self::iter_to_stream(cur.iter::<Key, Value>()),
            Self::Remote(cur) => cur.iter(),
        }
    }

    pub fn iter_start<'a, Key, Value>(
        &'a mut self,
    ) -> Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        match self {
            Self::Local(cur) => Self::iter_to_stream(cur.iter_start::<Key, Value>()),
            Self::Remote(cur) => cur.iter_start(),
        }
    }

    pub async fn iter_from<'a, Key, Value>(
        &'a mut self,
        key: &[u8],
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        Ok(match self {
            Self::Local(cur) => Self::iter_to_stream(cur.iter_from::<Key, Value>(&key)),
            Self::Remote(cur) => cur.iter_from(key.to_vec()).await?,
        })
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
        match self {
            Self::Local(cur) => Self::iterdup_to_steam(cur.iter_dup()),
            Self::Remote(cur) => cur.iter_dup(),
        }
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
        match self {
            Self::Local(cur) => Self::iterdup_to_steam(cur.iter_dup_start()),
            Self::Remote(cur) => cur.iter_dup_start(),
        }
    }

    pub async fn iter_dup_from<'a, Key, Value>(
        &'a mut self,
        key: &[u8],
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
        Ok(match self {
            Self::Local(cur) => Self::iterdup_to_steam(cur.iter_dup_from(&key)),
            Self::Remote(cur) => cur.iter_dup_from(key.to_vec()).await?,
        })
    }

    pub async fn iter_dup_of<'a, Key, Value>(
        &'a mut self,
        key: &[u8],
    ) -> Result<Pin<Box<dyn Stream<Item = Result<(Key, Value)>> + Send + 'a>>>
    where
        Key: TableObject + Send + 'a,
        Value: TableObject + Send + 'a,
    {
        Ok(match self {
            Self::Local(cur) => Self::iter_to_stream(cur.iter_dup_of(&key)),
            Self::Remote(cur) => cur.iter_dup_of(key.to_vec()).await?,
        })
    }
}

impl CursorAny<RW> {
    pub async fn put(&mut self, key: &[u8], data: &[u8], flags: WriteFlags) -> Result<()> {
        match self {
            Self::Local(cur) => Ok(cur.put(key, data, flags)?),
            Self::Remote(cur) => Ok(cur.put(key.to_vec(), data.to_vec(), flags).await?),
        }
    }

    pub async fn del(&mut self, flags: WriteFlags) -> Result<()> {
        match self {
            Self::Local(cur) => Ok(cur.del(flags)?),
            Self::Remote(cur) => Ok(cur.del(flags).await?),
        }
    }
}
