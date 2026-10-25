use futures_core::future::BoxFuture;
use futures_core::stream::BoxStream;
use sqlx::{Database, Describe, Either, Execute, Executor, Pool, Sqlite, Transaction};
use std::future::Future;

mod write_log;

pub use serde_json;
pub use write_log::{WriteLog, log_value};

/// 数据库操作上下文:持有连接池,以及可选的当前事务。
#[derive(Debug)]
pub struct DbContext {

    /// 数据库连接池
    pool: Pool<Sqlite>,

    /// 当前事务(未开启事务时为 None)
    tx: Option<Transaction<'static, Sqlite>>,

    /// 写SQL日志(未开启写日志时为 None)
    write_log: Option<Vec<WriteLog>>,

    /// 开启事务时写日志的长度,回滚时据此丢弃事务期间记录的日志
    tx_log_start: usize,
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
            write_log: None,
            tx_log_start: 0,
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
            self.tx_log_start = self.write_log.as_ref().map_or(0, Vec::len);
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
    /// 事务期间记录的写日志会一并丢弃(开启事务之前记录的保留)。
    pub fn rollback(&mut self) -> impl Future<Output = Result<(), sqlx::Error>> + Send {
        async move {
            //取出后 self.tx 自动变回 None,
            if let Some(tx) = self.tx.take(){
                if let Some(log) = &mut self.write_log {
                    log.truncate(self.tx_log_start);
                }
                tx.rollback().await?;
            }
            Ok(())
        }
    }

    /// 开启写SQL日志。开启后 sqlx-dsl-dao 生成的写函数执行成功后会把(SQL, 参数)记录下来,
    /// 通过 [`take_write_log`](Self::take_write_log) 取出。已经开启时什么都不做。
    pub fn enable_write_log(&mut self) {
        if self.write_log.is_none() {
            self.write_log = Some(Vec::new());
        }
    }

    /// 关闭写SQL日志,并丢弃尚未取出的日志。
    pub fn disable_write_log(&mut self) {
        self.write_log = None;
    }

    /// 是否开启了写SQL日志
    #[inline]
    pub fn is_write_log_enabled(&self) -> bool {
        self.write_log.is_some()
    }

    /// 取出目前为止记录的写日志(取出后清空,写日志保持开启)。未开启时返回空列表。
    pub fn take_write_log(&mut self) -> Vec<WriteLog> {
        self.tx_log_start = 0;
        self.write_log.as_mut().map(std::mem::take).unwrap_or_default()
    }

    /// 记录一条写日志。未开启写日志时什么都不做。
    /// 生成代码会先判断 [`is_write_log_enabled`](Self::is_write_log_enabled) 再构建参数,
    /// 手写的SQL也可以调用它把自己的写操作加入日志。
    pub fn push_write_log(&mut self, sql: impl Into<String>, params: Vec<serde_json::Value>) {
        if let Some(log) = &mut self.write_log {
            log.push(WriteLog {
                sql: sql.into(),
                params,
            });
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
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    async fn memory_ctx() -> DbContext {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        DbContext::new(pool)
    }

    #[tokio::test]
    async fn write_log_disabled_by_default() {
        let mut ctx = memory_ctx().await;
        assert!(!ctx.is_write_log_enabled());
        ctx.push_write_log("insert into t values (?)", vec![json!(1)]);
        assert!(ctx.take_write_log().is_empty());
    }

    #[tokio::test]
    async fn rollback_discards_logs_written_in_transaction() {
        let mut ctx = memory_ctx().await;
        ctx.enable_write_log();
        ctx.push_write_log("before", vec![]);
        ctx.begin().await.unwrap();
        ctx.push_write_log("in tx", vec![]);
        ctx.rollback().await.unwrap();
        let logs = ctx.take_write_log();
        assert_eq!(logs.iter().map(|it| it.sql.as_str()).collect::<Vec<_>>(), ["before"]);

        ctx.begin().await.unwrap();
        ctx.push_write_log("committed", vec![json!("a")]);
        let logs = ctx.take_write_log();
        ctx.commit().await.unwrap();
        assert_eq!(logs, [WriteLog { sql: "committed".into(), params: vec![json!("a")] }]);
        assert!(ctx.is_write_log_enabled());
    }
}
