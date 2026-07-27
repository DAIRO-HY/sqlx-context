# sqlx-context

A small `DbContext` wrapper around [`sqlx`](https://crates.io/crates/sqlx) that lets your
business code write queries against `&mut DbContext` without caring whether a transaction
is currently open — no more manually branching between `&Pool` and `&mut Transaction` in
every function.

## Why

Without this crate, code that may run inside or outside a transaction typically ends up
looking like this:

```rust,ignore
async fn insert(pool: &Pool<Sqlite>, tx: Option<&mut Transaction<'_, Sqlite>>) -> sqlx::Result<()> {
    match tx {
        Some(tx) => sqlx::query("...").execute(&mut **tx).await?,
        None => sqlx::query("...").execute(pool).await?,
    };
    Ok(())
}
```

`DbContext` implements `sqlx::Executor` directly, so the branching only happens once,
inside the wrapper:

```rust,ignore
async fn insert(ctx: &mut DbContext) -> sqlx::Result<()> {
    sqlx::query("...").execute(ctx).await?;
    Ok(())
}
```

## Install

```toml
[dependencies]
sqlx-context = "0.0.1"
sqlx = { version = "0.8", features = ["sqlite", "runtime-tokio"] }
```

`sqlx-context` only enables the `sqlite` and `runtime-tokio` features of `sqlx` itself.
If you need extra `sqlx` features (`macros`, `chrono`, `json`, ...), enable them yourself —
Cargo's feature unification will combine them automatically.

Currently only the `Sqlite` driver is supported.

## Usage

```rust,ignore
use sqlx::sqlite::SqlitePoolOptions;
use sqlx_context::DbContext;

#[tokio::main]
async fn main() -> sqlx::Result<()> {
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite://data.db")
        .await?;

    let mut ctx = DbContext::new(pool);

    ctx.begin().await?;
    insert_one(&mut ctx).await?;
    insert_two(&mut ctx).await?;
    ctx.commit().await?;

    Ok(())
}

async fn insert_one(ctx: &mut DbContext) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO items (name) VALUES ('a')")
        .execute(ctx)
        .await?;
    Ok(())
}

async fn insert_two(ctx: &mut DbContext) -> sqlx::Result<()> {
    sqlx::query("INSERT INTO items (name) VALUES ('b')")
        .execute(ctx)
        .await?;
    Ok(())
}
```

The same `insert_one` / `insert_two` functions work whether or not `ctx.begin()` was
called — if no transaction is active, queries run directly against the pool.

## API

- `DbContext::new(pool: Pool<Sqlite>) -> DbContext` — wrap a pool, no transaction open yet.
- `ctx.begin().await` — starts a transaction. Returns `sqlx::Error::Configuration` if a
  transaction is already active (nested transactions are not supported).
- `ctx.commit().await` — commits the active transaction. A no-op returning `Ok(())` if no
  transaction is open.
- `ctx.rollback().await` — rolls back the active transaction. A no-op returning `Ok(())` if
  no transaction is open.
- `&mut DbContext` implements `sqlx::Executor`, so it can be passed anywhere sqlx expects
  an executor (`.execute()`, `.fetch_one()`, `query!` macros, etc.).

## Notes

- If a `DbContext` is dropped while a transaction is still open, a warning is printed to
  stderr. `sqlx::Transaction`'s own `Drop` will still attempt a best-effort async rollback,
  but this is not guaranteed to complete before the process exits — always call `commit()`
  or `rollback()` explicitly.

## License

MIT