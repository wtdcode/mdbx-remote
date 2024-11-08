use std::{collections::HashMap, path::PathBuf, sync::Arc};

use ffi::{
    MDBX_FIRST, MDBX_GET_CURRENT, MDBX_LAST, MDBX_NEXT, MDBX_NEXT_DUP, MDBX_NEXT_MULTIPLE,
    MDBX_NEXT_NODUP, MDBX_PREV, MDBX_PREV_DUP, MDBX_PREV_MULTIPLE, MDBX_PREV_NODUP, MDBX_SET_KEY,
    MDBX_SET_RANGE,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

use crate::{
    environment::RemoteEnvironmentConfig, CommitLatency, Cursor, DatabaseFlags, Environment,
    EnvironmentBuilder, Info, Stat, Transaction, TransactionKind, WriteFlags, RO, RW,
};

const ALLOWED_GET_FULL_OPS: &[u32] = &[
    MDBX_NEXT,
    MDBX_NEXT_DUP,
    MDBX_NEXT_MULTIPLE,
    MDBX_NEXT_NODUP,
    MDBX_PREV,
    MDBX_PREV_DUP,
    MDBX_PREV_NODUP,
    MDBX_PREV_MULTIPLE,
    MDBX_FIRST,
    MDBX_GET_CURRENT,
    MDBX_LAST,
    MDBX_SET_KEY,
    MDBX_SET_RANGE,
];

#[tarpc::service]
pub trait RemoteMDBX {
    async fn open_env(path: PathBuf, builder: RemoteEnvironmentConfig) -> Result<u64, ServerError>;

    async fn env_ro_tx(env: u64) -> Result<u64, ServerError>;
    async fn env_rw_tx(env: u64) -> Result<u64, ServerError>;
    async fn env_sync(env: u64, force: bool) -> Result<bool, ServerError>;
    async fn env_close(env: u64) -> Result<(), ServerError>;
    async fn env_stat(env: u64) -> Result<Stat, ServerError>;
    async fn env_info(env: u64) -> Result<Info, ServerError>;

    async fn tx_create_db(
        env: u64,
        tx: u64,
        db: Option<String>,
        flags: u32,
    ) -> Result<u32, ServerError>; // dbi
    async fn tx_get(
        env: u64,
        tx: u64,
        dbi: u32,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ServerError>;
    async fn tx_put(
        env: u64,
        tx: u64,
        dbi: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        flags: u32,
    ) -> Result<(), ServerError>;
    async fn tx_del(
        env: u64,
        tx: u64,
        dbi: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<bool, ServerError>;
    async fn tx_ro_cursor(env: u64, tx: u64, dbi: u32) -> Result<u64, ServerError>;
    async fn tx_rw_cursor(env: u64, tx: u64, dbi: u32) -> Result<u64, ServerError>;
    async fn tx_commit(env: u64, tx: u64) -> Result<(bool, CommitLatency), ServerError>;
    async fn tx_abort(env: u64, tx: u64) -> Result<(), ServerError>;
    async fn tx_nested(env: u64, tx: u64) -> Result<u64, ServerError>;
    async fn tx_db_stat(env: u64, tx: u64, dbi: u32) -> Result<Stat, ServerError>;
    async fn clear_db(env: u64, tx: u64, dbi: u32) -> Result<(), ServerError>;

    async fn cur_get(
        env: u64,
        tx: u64,
        cur: u64,
        key: Option<Vec<u8>>,
        data: Option<Vec<u8>>,
        op: u32,
    ) -> Result<(Option<Vec<u8>>, Vec<u8>, bool), ServerError>;
    async fn cur_put(
        env: u64,
        tx: u64,
        cur: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        flags: u32,
    ) -> Result<(), ServerError>;
    async fn cur_create(env: u64, tx: u64, cur: u64) -> Result<u64, ServerError>;
    async fn cur_del(env: u64, tx: u64, cur: u64, flags: u32) -> Result<(), ServerError>;
    async fn cur_close(env: u64, tx: u64, cur: u64) -> Result<(), ServerError>;

    // Our custom primitives
    async fn batch_cur_get_full(
        env: u64,
        tx: u64,
        cur: u64,
        cnt: u64,
        buffer: u64,
        op: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ServerError>;
}

#[derive(Debug)]
struct LocalTransactionState<K: TransactionKind> {
    tx: Transaction<K>,
    cursors: HashMap<u64, Cursor<K>>,
    next_cur_id: u64,
}

impl<K: TransactionKind> LocalTransactionState<K> {
    pub fn next_id(&mut self) -> u64 {
        while self.cursors.contains_key(&self.next_cur_id) {
            self.next_cur_id = self.next_cur_id.wrapping_add(1);
        }

        self.next_cur_id
    }

    pub fn cur_clone(&mut self, cur: u64) -> Option<u64> {
        let cur = self.cursors.get_mut(&cur)?;
        let new_cur = cur.clone();
        let new_id = self.next_id();
        self.cursors.insert(new_id, new_cur);
        Some(new_id)
    }
}

#[derive(Debug)]
struct DatabaseEnvState {
    rotxs: HashMap<u64, LocalTransactionState<RO>>,
    rwtxs: HashMap<u64, LocalTransactionState<RW>>,
    env: Environment,
    next_tx_id: u64,
}

impl DatabaseEnvState {
    fn next_id(&mut self) -> u64 {
        while self.rotxs.contains_key(&self.next_tx_id) || self.rwtxs.contains_key(&self.next_tx_id)
        {
            self.next_tx_id = self.next_tx_id.wrapping_add(1);
        }

        self.next_tx_id
    }
}

#[derive(Debug, Default)]
pub struct MDBXServerState {
    envs: HashMap<u64, DatabaseEnvState>,
    next_env_id: u64,
}

impl MDBXServerState {
    fn next_id(&mut self) -> u64 {
        while self.envs.contains_key(&self.next_env_id) {
            self.next_env_id = self.next_env_id.wrapping_add(1);
        }
        self.next_env_id
    }
}

#[derive(Clone, Debug, Error, Serialize, Deserialize)]
pub enum ServerError {
    #[error("mdbx error: {0}")]
    MBDX(crate::error::Error),
    #[error("no such env")]
    NOENV,
    #[error("no such tx")]
    NOTX,
    #[error("no such cursor")]
    NOCURSOR,
    #[error("incorrect flag")]
    INCORRECTFLAG,
    #[error("fail to get absolute path")]
    NOPATH,
    #[error("not writable")]
    NOWRITABLE,
    #[error("tokio: {0}")]
    TOKIO(String),
    #[error("invalid get_full: {0}")]
    INVALIDGETULL(u32),
}

impl From<tokio::task::JoinError> for ServerError {
    fn from(value: tokio::task::JoinError) -> Self {
        Self::TOKIO(value.to_string())
    }
}

impl From<crate::error::Error> for ServerError {
    fn from(value: crate::error::Error) -> Self {
        Self::MBDX(value.into())
    }
}

#[derive(Debug, Clone)]
pub struct RemoteMDBXServer {
    state: Arc<RwLock<MDBXServerState>>,
}

impl RemoteMDBXServer {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(MDBXServerState::default())),
        }
    }
}

impl RemoteMDBX for RemoteMDBXServer {
    async fn open_env(
        self,
        _context: tarpc::context::Context,
        path: PathBuf,
        cfg: RemoteEnvironmentConfig,
    ) -> Result<u64, ServerError> {
        let abs_path = std::path::absolute(path).map_err(|_| ServerError::NOPATH)?;
        let handle = {
            let builder = EnvironmentBuilder::from(cfg);
            let env = tokio::task::spawn_blocking(move || builder.open(&abs_path)).await??;
            let mut lg = self.state.write().await;
            let handle = lg.next_id();

            lg.envs.insert(
                handle,
                DatabaseEnvState {
                    rotxs: HashMap::new(),
                    rwtxs: HashMap::new(),
                    env: env,
                    next_tx_id: 0,
                },
            );
            handle
        };

        Ok(handle)
    }

    async fn env_close(
        self,
        _context: tarpc::context::Context,
        env: u64,
    ) -> Result<(), ServerError> {
        let mut lg = self.state.write().await;
        lg.envs.remove(&env);
        Ok(())
    }

    async fn env_stat(
        self,
        _context: tarpc::context::Context,
        env: u64,
    ) -> Result<Stat, ServerError> {
        let env = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .env
            .clone();
        let ret = env.stat()?;

        Ok(ret)
    }

    async fn env_info(
        self,
        _context: tarpc::context::Context,
        env: u64,
    ) -> Result<Info, ServerError> {
        let env = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .env
            .clone();
        let ret = env.info()?;

        Ok(ret)
    }

    async fn env_sync(
        self,
        _context: tarpc::context::Context,
        env: u64,
        force: bool,
    ) -> Result<bool, ServerError> {
        let env = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .env
            .clone();
        // This can block for a very time, though not forever
        let ret = tokio::task::spawn_blocking(move || env.sync(force)).await??;

        Ok(ret)
    }

    async fn env_ro_tx(
        self,
        _context: tarpc::context::Context,
        env: u64,
    ) -> Result<u64, ServerError> {
        let env_clone = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .env
            .clone();
        // This can block forever
        let tx = tokio::task::spawn_blocking(move || env_clone.begin_ro_txn()).await??;

        // hold write lock now, because no deadlock will happen
        let mut lg = self.state.write().await;
        let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;
        let tx_id = env.next_id();
        env.rotxs.insert(
            tx_id,
            LocalTransactionState {
                tx: tx,
                cursors: HashMap::new(),
                next_cur_id: 0,
            },
        );

        Ok(tx_id)
    }

    async fn env_rw_tx(
        self,
        _context: tarpc::context::Context,
        env: u64,
    ) -> Result<u64, ServerError> {
        let env_clone = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .env
            .clone();
        // This can block forever
        let tx = tokio::task::spawn_blocking(move || env_clone.begin_rw_txn()).await??;

        // hold write lock now, because no deadlock will happen
        let mut lg = self.state.write().await;
        let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;
        let tx_id = env.next_id();
        env.rwtxs.insert(
            tx_id,
            LocalTransactionState {
                tx: tx,
                cursors: HashMap::new(),
                next_cur_id: 0,
            },
        );
        Ok(tx_id)
    }

    async fn tx_create_db(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        db: Option<String>,
        flags: u32,
    ) -> Result<u32, ServerError> {
        let flags = DatabaseFlags::from_bits(flags).ok_or(ServerError::INCORRECTFLAG)?;

        let lg = self.state.read().await;
        let env = lg.envs.get(&env).ok_or(ServerError::NOENV)?;

        let db = if let Some(tx) = env.rwtxs.get(&tx) {
            let tx = tx.tx.clone();
            drop(lg);
            tokio::task::spawn_blocking(move || tx.open_db_with_flags(db.as_deref(), flags))
                .await??
        } else if let Some(tx) = env.rotxs.get(&tx) {
            let tx = tx.tx.clone();
            drop(lg);
            if flags.contains(DatabaseFlags::CREATE) {
                return Err(ServerError::NOWRITABLE);
            }
            // This can block? can it?
            // But anyway opening databases should be not so frequent so overhead should be acceptable.
            tokio::task::spawn_blocking(move || tx.open_db(db.as_deref())).await??
        } else {
            return Err(ServerError::NOTX);
        };

        Ok(db.dbi())
    }

    async fn tx_get(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        dbi: u32,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ServerError> {
        let lg = self.state.read().await;
        let env = lg.envs.get(&env).ok_or(ServerError::NOENV)?;

        let val = if let Some(tx) = env.rwtxs.get(&tx) {
            let tx = tx.tx.clone();
            drop(lg);
            tx.get::<Vec<u8>>(dbi, &key)?
        } else if let Some(ro) = env.rotxs.get(&tx) {
            let tx_clone = ro.tx.clone();
            drop(lg);
            tx_clone.get::<Vec<u8>>(dbi, &key)?
        } else {
            return Err(ServerError::NOTX);
        };
        Ok(val)
    }

    async fn tx_put(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        dbi: u32,
        key: Vec<u8>,
        value: Vec<u8>,
        flags: u32,
    ) -> Result<(), ServerError> {
        let flags = WriteFlags::from_bits(flags).ok_or(ServerError::INCORRECTFLAG)?;
        let lg = self.state.read().await;

        let env = lg.envs.get(&env).ok_or(ServerError::NOENV)?;

        if let Some(tx) = env.rwtxs.get(&tx) {
            let tx = tx.tx.clone();
            drop(lg);
            tx.put(dbi, &key, &value, flags)?;
        } else {
            return Err(ServerError::NOTX);
        }

        Ok(())
    }

    async fn tx_del(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        dbi: u32,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<bool, ServerError> {
        let lg = self.state.read().await;

        let env = lg.envs.get(&env).ok_or(ServerError::NOENV)?;

        let ret = if let Some(tx) = env.rwtxs.get(&tx) {
            let tx = tx.tx.clone();
            drop(lg);
            tx.del(dbi, &key, value.as_ref().map(|t| t.as_slice()))?
        } else {
            return Err(ServerError::NOTX);
        };

        Ok(ret)
    }

    async fn tx_commit(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
    ) -> Result<(bool, CommitLatency), ServerError> {
        let tx = {
            let mut lg = self.state.write().await;
            let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;
            env.rwtxs.remove(&tx).ok_or(ServerError::NOTX)?
        };

        // This can be slow, wrap it in spawn_blocking
        Ok(tokio::task::spawn_blocking(move || tx.tx.commit()).await??)
    }

    async fn tx_abort(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
    ) -> Result<(), ServerError> {
        let (rw, ro) = {
            let mut lg = self.state.write().await;

            let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;

            (env.rwtxs.remove(&tx), env.rotxs.remove(&tx))
        };

        if let Some(rw) = rw {
            drop(rw.tx);
        } else if let Some(ro) = ro {
            drop(ro.tx);
        } else {
            return Err(ServerError::NOTX);
        }

        Ok(())
    }

    async fn tx_nested(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
    ) -> Result<u64, ServerError> {
        let mut tx = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .rwtxs
            .get(&tx)
            .ok_or(ServerError::NOTX)?
            .tx
            .clone();

        // Can this block forever? Anyway, wrap it with spawn_blocking for safety.
        let new_tx = tokio::task::spawn_blocking(move || tx.begin_nested_txn()).await??;

        let mut lg = self.state.write().await;
        let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;
        let id = env.next_id();
        env.rwtxs.insert(
            id,
            LocalTransactionState {
                tx: new_tx,
                cursors: HashMap::new(),
                next_cur_id: 0,
            },
        );

        Ok(id)
    }

    async fn tx_db_stat(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        dbi: u32,
    ) -> Result<Stat, ServerError> {
        let lg = self.state.read().await;

        let env = lg.envs.get(&env).ok_or(ServerError::NOENV)?;

        let stat = if let Some(rw) = env.rwtxs.get(&tx) {
            let tx = rw.tx.clone();
            drop(lg);
            tx.db_stat_with_dbi(dbi)?
        } else if let Some(ro) = env.rotxs.get(&tx) {
            let tx = ro.tx.clone();
            drop(lg);
            tx.db_stat_with_dbi(dbi)?
        } else {
            return Err(ServerError::NOTX);
        };

        Ok(stat)
    }

    async fn clear_db(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        dbi: u32,
    ) -> Result<(), ServerError> {
        let tx = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .rwtxs
            .get(&tx)
            .ok_or(ServerError::NOTX)?
            .tx
            .clone();

        // This can be slow
        tokio::task::spawn_blocking(move || tx.clear_db(dbi)).await??;
        Ok(())
    }

    async fn tx_ro_cursor(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        dbi: u32,
    ) -> Result<u64, ServerError> {
        let tx_clone = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .rotxs
            .get(&tx)
            .ok_or(ServerError::NOTX)?
            .tx
            .clone();
        let cur = tokio::task::spawn_blocking(move || tx_clone.cursor_with_dbi(dbi)).await??;
        let mut lg = self.state.write().await;
        let tx_mut = lg
            .envs
            .get_mut(&env)
            .ok_or(ServerError::NOENV)?
            .rotxs
            .get_mut(&tx)
            .ok_or(ServerError::NOTX)?;

        let cur_id = tx_mut.next_id();
        tx_mut.cursors.insert(cur_id, cur);
        return Ok(cur_id);
    }

    async fn tx_rw_cursor(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        dbi: u32,
    ) -> Result<u64, ServerError> {
        let tx_clone = self
            .state
            .read()
            .await
            .envs
            .get(&env)
            .ok_or(ServerError::NOENV)?
            .rwtxs
            .get(&tx)
            .ok_or(ServerError::NOTX)?
            .tx
            .clone();
        let cur = tokio::task::spawn_blocking(move || tx_clone.cursor_with_dbi(dbi)).await??;
        let mut lg = self.state.write().await;
        let tx_mut = lg
            .envs
            .get_mut(&env)
            .ok_or(ServerError::NOENV)?
            .rwtxs
            .get_mut(&tx)
            .ok_or(ServerError::NOTX)?;

        let cur_id = tx_mut.next_id();
        tx_mut.cursors.insert(cur_id, cur);
        return Ok(cur_id);
    }

    async fn cur_create(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        cur: u64,
    ) -> Result<u64, ServerError> {
        // Cloning a cursor has an extra overhead of mdbx call, don't wrap them
        // in a tokio::task::spawn_blocking
        let mut lg = self.state.write().await;

        let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;

        if let Some(tx) = env.rwtxs.get_mut(&tx) {
            let new_cur = tx.cur_clone(cur).ok_or(ServerError::NOCURSOR)?;
            return Ok(new_cur);
        } else if let Some(tx) = env.rotxs.get_mut(&tx) {
            let new_cur = tx.cur_clone(cur).ok_or(ServerError::NOCURSOR)?;
            return Ok(new_cur);
        } else {
            return Err(ServerError::NOTX);
        };
    }

    async fn cur_get(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        cur: u64,
        key: Option<Vec<u8>>,
        data: Option<Vec<u8>>,
        op: u32,
    ) -> Result<(Option<Vec<u8>>, Vec<u8>, bool), ServerError> {
        let op = mdbx_remote_sys::MDBX_cursor_op::try_from(op)
            .map_err(|_| crate::error::Error::DecodeError)?;
        let lg = self.state.read().await;

        let env = lg.envs.get(&env).ok_or(ServerError::NOENV)?;

        // tracing::debug!("Cursor get, cur = {}, key = {:?} op = {}", cur, key, op);
        let val = if let Some(tx) = env.rwtxs.get(&tx) {
            tx.cursors
                .get(&cur)
                .ok_or(ServerError::NOCURSOR)?
                .get::<Vec<u8>, Vec<u8>>(
                    key.as_ref().map(|t| t.as_slice()),
                    data.as_ref().map(|t| t.as_slice()),
                    op,
                )?
        } else if let Some(tx) = env.rotxs.get(&tx) {
            tx.cursors
                .get(&cur)
                .ok_or(ServerError::NOCURSOR)?
                .get::<Vec<u8>, Vec<u8>>(
                    key.as_ref().map(|t| t.as_slice()),
                    data.as_ref().map(|t| t.as_slice()),
                    op,
                )?
        } else {
            return Err(ServerError::NOTX);
        };
        // tracing::debug!("Cursor get down, cur = {}, key = {:?} op = {}", cur, key, op);
        Ok(val)
    }

    async fn cur_put(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        cur: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        flags: u32,
    ) -> Result<(), ServerError> {
        // tracing::debug!("Cursor put, cur = {}, key = {:?}", cur, key);
        let op = WriteFlags::from_bits(flags).ok_or(ServerError::INCORRECTFLAG)?;
        let mut lg = self.state.write().await;

        let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;

        if let Some(tx) = env.rwtxs.get_mut(&tx) {
            tx.cursors
                .get_mut(&cur)
                .ok_or(ServerError::NOCURSOR)?
                .put(&key, &value, op)?;
        } else {
            return Err(ServerError::NOTX);
        }

        // tracing::debug!("Cursor put done, cur = {}, key = {:?}", cur, key);
        Ok(())
    }

    async fn cur_del(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        cur: u64,
        flags: u32,
    ) -> Result<(), ServerError> {
        // tracing::info!("cur_del {}", cur);
        let op = WriteFlags::from_bits(flags).ok_or(ServerError::INCORRECTFLAG)?;
        let mut lg = self.state.write().await;

        let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;

        if let Some(tx) = env.rwtxs.get_mut(&tx) {
            tx.cursors
                .get_mut(&cur)
                .ok_or(ServerError::NOCURSOR)?
                .del(op)?;
        } else {
            return Err(ServerError::NOTX);
        }
        // tracing::info!("cur_down {}", cur);
        Ok(())
    }

    async fn cur_close(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        cur: u64,
    ) -> Result<(), ServerError> {
        let (rw, ro) = {
            let mut lg = self.state.write().await;

            let env = lg.envs.get_mut(&env).ok_or(ServerError::NOENV)?;

            (
                env.rwtxs.get_mut(&tx).and_then(|t| t.cursors.remove(&cur)),
                env.rotxs.get_mut(&tx).and_then(|t| t.cursors.remove(&cur)),
            )
        };

        if let Some(rw) = rw {
            drop(rw);
        } else if let Some(ro) = ro {
            drop(ro);
        } else {
            return Err(ServerError::NOCURSOR);
        }

        Ok(())
    }

    async fn batch_cur_get_full(
        self,
        _context: tarpc::context::Context,
        env: u64,
        tx: u64,
        cur: u64,
        cnt: u64,
        buffer: u64,
        op: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ServerError> {
        let op = mdbx_remote_sys::MDBX_cursor_op::try_from(op)
            .map_err(|_| crate::error::Error::DecodeError)?;

        if !ALLOWED_GET_FULL_OPS.contains(&op) {
            return Err(ServerError::INVALIDGETULL(op));
        }
        let lg = self.state.read().await;

        let env = lg.envs.get(&env).ok_or(ServerError::NOENV)?;

        // tracing::debug!("Cursor get, cur = {}, key = {:?} op = {}", cur, key, op);
        if let Some(tx) = env.rwtxs.get(&tx) {
            let cur = tx.cursors.get(&cur).ok_or(ServerError::NOCURSOR)?;

            return Self::batch_cur_get_full_impl(cur, cnt, buffer, op);
        } else if let Some(tx) = env.rotxs.get(&tx) {
            let cur = tx.cursors.get(&cur).ok_or(ServerError::NOCURSOR)?;
            return Self::batch_cur_get_full_impl(cur, cnt, buffer, op);
        } else {
            return Err(ServerError::NOTX);
        }
    }
}

impl RemoteMDBXServer {
    fn batch_cur_get_full_impl<K: TransactionKind>(
        cur: &Cursor<K>,
        cnt: u64,
        buffer: u64,
        op: u32,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ServerError> {
        let mut out = Vec::new();
        let mut current_size = 0;
        for _ in 0..cnt {
            if current_size >= buffer {
                break;
            }

            match cur.get::<Vec<u8>, Vec<u8>>(None, None, op) {
                Ok((k, v, _)) => {
                    let key = k.ok_or(ServerError::INVALIDGETULL(op))?;
                    current_size += key.len() as u64 + v.len() as u64;
                    out.push((key, v));
                }
                Err(crate::Error::NoData | crate::Error::NotFound) => return Ok(out),
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        Ok(out)
    }
}
