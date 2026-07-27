use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use sqlx::{Database, Describe, Either, Execute, Executor, Pool, Sqlite, Transaction};
use std::future::Future;

/// 数据库操作上下文:持有连接池,以及可选的当前事务。
#[derive(Debug)]
pub struct DbContext {

    /// 数据库连接池
    pool: Pool<Sqlite>,

    /// 当前事务(未开启事务时为 None)
    tx: Option<Transaction<'static, Sqlite>>,
}

impl Drop for DbContext {
    fn drop(&mut self) {
        // Drop 里不能 await,这里只做尽力而为的提醒,真正的
        // rollback 由 sqlx::Transaction 自身的 Drop 异步补偿完成,
        // 不保证一定在进程退出前跑完——业务代码应始终显式 commit/rollback。
        if self.tx.is_some() {
            eprintln!(
                "warning: DbContext dropped while a transaction was still active; \
                 it was not explicitly committed or rolled back"
            );
        }
    }
}

impl DbContext {
    /// 用连接池创建一个新的 `DbContext`,初始不带事务。
    pub fn new(pool: Pool<Sqlite>) -> Self {
        DbContext {
            pool,
            tx: None,
        }
    }

    /// 开启一个新事务。若当前已经处于事务中(`tx` 非空),返回错误而不覆盖原事务。
    pub fn begin(&mut self) -> impl Future<Output = Result<(), sqlx::Error>> + Send {
        async move {
            if self.tx.is_some() {
                return Err(sqlx::Error::Configuration(
                    "DbContext::begin called while a transaction is already active".into(),
                ));
            }
            self.tx = Some(self.pool.begin().await?);
            Ok(())
        }
    }

    /// 提交当前事务。若当前没有事务,则什么都不做,直接返回 `Ok(())`。
    pub fn commit(&mut self) -> impl Future<Output = Result<(), sqlx::Error>> + Send {
        async move {
            //取出后 self.tx 自动变回 None,
            if let Some(tx) = self.tx.take(){
                tx.commit().await?;
            }
            Ok(())
        }
    }

    /// 回滚当前事务。若当前没有事务,则什么都不做,直接返回 `Ok(())`。
    pub fn rollback(&mut self) -> impl Future<Output = Result<(), sqlx::Error>> + Send {
        async move {
            //取出后 self.tx 自动变回 None,
            if let Some(tx) = self.tx.take(){
                tx.rollback().await?;
            }
            Ok(())
        }
    }
}

/// 让 `&mut DbContext` 本身就是一个 sqlx::Executor。
///
/// 内部把两种执行器统一委托给已有的 `Executor` 实现：
/// - `&Pool<Sqlite>` 本来就实现了 `Executor`
/// - `&mut Transaction<Sqlite>` 需要先 deref 到内部的 `SqliteConnection`
///   （sqlx 0.8 之后 `Executor` 不再直接为 `Transaction` 实现，见官方注释）
///
/// 实现之后，所有业务函数都可以直接把 `&mut DbContext` 当成普通 sqlx 执行器使用，
/// 不用再在每个函数里手动 match Pool/Tx 两个分支。
impl<'c> Executor<'c> for &'c mut DbContext {
    type Database = Sqlite;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            sqlx::Error,
        >,
    >
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        if let Some(tx) = &mut self.tx {
            tx.fetch_many(query)
        } else {
            self.pool.fetch_many(query)
        }
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, sqlx::Error>>
    where
        'c: 'e,
        E: 'q + Execute<'q, Self::Database>,
    {
        if let Some(tx) = &mut self.tx {
            tx.fetch_optional(query)
        } else {
            self.pool.fetch_optional(query)
        }
    }

    fn prepare_with<'e, 'q: 'e>(
        self,
        sql: &'q str,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement<'q>, sqlx::Error>>
    where
        'c: 'e,
    {
        if let Some(tx) = &mut self.tx {
            tx.prepare_with(sql, parameters)
        } else {
            self.pool.prepare_with(sql, parameters)
        }
    }

    fn describe<'e, 'q: 'e>(
        self,
        sql: &'q str,
    ) -> BoxFuture<'e, Result<Describe<Self::Database>, sqlx::Error>>
    where
        'c: 'e,
    {
        if let Some(tx) = &mut self.tx {
            tx.describe(sql)
        } else {
            self.pool.describe(sql)
        }
    }
}