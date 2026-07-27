# sqlx-context

A Cargo workspace containing `sqlx-context`, a small `DbContext` wrapper around
[`sqlx`](https://crates.io/crates/sqlx) that lets business code write queries against
`&mut DbContext` without caring whether a transaction is currently open.

## Workspace layout

| Member | Description |
| --- | --- |
| [`sqlx-context`](./sqlx-context) | The library crate, published on crates.io. See its [README](./sqlx-context/README.md) for API docs and usage examples. |
| [`sqlx-context-sample`](./sqlx-context-sample) | A runnable example (`main.rs`) demonstrating `DbContext` against a SQLite database. |

## Quick start

```bash
# build everything
cargo build

# run the example (edit the connection string in sqlx-context-sample/src/main.rs first)
cargo run -p sqlx-context-sample
```

For how to add `sqlx-context` as a dependency in your own project, see the
[crate README](./sqlx-context/README.md).

## License

MIT — see [sqlx-context/LICENSE](./sqlx-context/LICENSE).