use std::fs;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite};
use sqlx_context::DbContext;

#[tokio::main]
async fn main() {
    let db: Pool<Sqlite> = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_lazy("sqlite://sqlx-context.sqlite?mode=rwc")
        .unwrap();

    let _ = sqlx::query(
        r#"
    CREATE TABLE IF NOT EXISTS  test(
        key1 VARCHAR(32) PRIMARY KEY NOT NULL,
        key2  VARCHAR(1024)         NOT NULL
    );"#,
    )
    .execute(&db)
    .await;

    let mut ctx = DbContext::new(db);
    ctx.begin().await.unwrap();
    group_insert(&mut ctx).await.unwrap();
    insert3(&mut ctx).await.unwrap();
    ctx.rollback().await.unwrap();
    group_insert(&mut ctx).await.unwrap();
    drop(ctx);
    fs::remove_file("sqlx-context.sqlite").unwrap();
    println!("Hello, world!");
}

async fn group_insert(ctx: &mut DbContext) -> Result<(), sqlx::Error> {
    insert1(ctx).await?;
    insert2(ctx).await?;
    Ok(())
}

/// 插入数据
pub async fn insert1(ctx: &mut DbContext) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO test (key1, key2) VALUES ('insert1-1', '232wefsfs')")
        .execute(&mut *ctx)
        .await?;
    sqlx::query("INSERT INTO test (key1, key2) VALUES ('insert1-2', '232wefsfs')")
        .execute(ctx)
        .await?;
    Ok(())
}

/// 插入数据
pub async fn insert2(ctx: &mut DbContext) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO test (key1, key2) VALUES ('insert2', '232wefsfs')")
        .execute(ctx)
        .await?;
    Ok(())
}

/// 插入数据
pub async fn insert3(ctx: &mut DbContext) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO test (key1, key2) VALUES ('insert3', '232wefsfs')")
        .execute(ctx)
        .await?;
    Ok(())
}
