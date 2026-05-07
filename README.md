# SyncLite DB — Language-Agnostic Sync-Ready Database Server

> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.

## What is SyncLite DB?

**SyncLite DB** is a standalone, sync-enabled database server that wraps popular embedded databases — SQLite, DuckDB, Apache Derby, H2, and HyperSQL — and exposes them over HTTP as a JSON API.

Whereas [SyncLite Logger](https://github.com/syncliteio/synclite-logger-java) is an embeddable JDBC library for Java and Python, SyncLite DB is the **language-agnostic alternative**: any application written in any language (Java, Python, C++, C#, Go, Rust, Ruby, Node.js, and more) can send SQL requests as JSON over HTTP and have them executed on the embedded database and automatically synced through the SyncLite pipeline to the destination database.

```
Your App (any language)  ──HTTP/JSON──▶  SyncLite DB Server  ──▶  Staging Storage  ──▶  SyncLite Consolidator  ──▶  Destination
```

## Key Features

- **Language-agnostic** — plain HTTP + JSON; no language-specific SDK required
- **All SyncLite device types** — SQLITE, DUCKDB, DERBY, H2, HYPERSQL, STREAMING, and all APPENDER variants
- **Full transaction support** — begin / execute / commit / rollback with transaction handles
- **Result set pagination** — fetch large result sets in pages using `resultset-handle`
- **Authentication** — Bearer token auth and HMAC app-auth
- **Batch operations** — batched INSERT / UPDATE / DELETE in a single HTTP call
- **Zero schema changes** — your app sends standard SQL; SyncLite DB handles the logging

## Starting the Server

```bash
# Windows
synclite-db.bat --config synclite_db.conf

# Linux / macOS
synclite-db.sh --config synclite_db.conf
```

The server binds to `http://localhost:<configured-port>` by default.

Database files are managed by the server under its DB root directory; applications should not pass physical DB paths in API calls.

## HTTP/JSON API

### Request model (important)

- Applications send `db-name` (not `db-path`).
- SyncLite DB resolves the physical database path internally under the server DB root directory.
- `db-name` is also used internally as SyncLite Logger `device-name`.
- On `initialize`, pass logger settings as a nested JSON object in `synclite-logger-options` (or `synclite-logger-config` object alias).
- File-path based logger config in requests is deprecated.

### Initialize a database

```json
POST /synclite
{
  "db-type": "SQLITE",
  "db-name": "myapp",
  "synclite-logger-options": {
    "local-data-stage-directory": "/home/alice/synclite/job1/stageDir",
    "destination-type": "FS"
  },
  "sql": "initialize"
}
```

If `synclite-logger-options` is omitted, server default logger config is used.

### Create a table

```json
{
  "db-name": "myapp",
  "sql": "CREATE TABLE IF NOT EXISTS events(id INT, payload TEXT)"
}
```

### Batched insert

```json
{
  "db-name": "myapp",
  "sql": "INSERT INTO events VALUES(?, ?)",
  "arguments": [[1, "edge-event-1"], [2, "edge-event-2"]]
}
```

### Explicit transaction

```json
// Begin
{ "db-name": "myapp", "sql": "begin" }

// Execute inside transaction
{ "db-name": "myapp", "sql": "INSERT INTO events VALUES(?, ?)", "txn-handle": "<uuid>", "arguments": [[3, "three"]] }

// Commit
{ "db-name": "myapp", "sql": "commit", "txn-handle": "<uuid>" }
```

### Querying data — SELECT, result set handling, and pagination

#### Basic SELECT

```json
POST /synclite
{
  "db-name": "myapp",
  "sql": "SELECT id, name, score FROM players ORDER BY id",
  "resultset-include-metadata": "ON"
}
```

**Response:**

```json
{
  "result": true,
  "message": "OK",
  "column-metadata": [
    { "label": "id",    "type": "INTEGER" },
    { "label": "name",  "type": "TEXT"    },
    { "label": "score", "type": "INTEGER" }
  ],
  "resultset": [
    { "id": 1, "name": "Alice", "score": 100 },
    { "id": 2, "name": "Bob",   "score": 200 }
  ],
  "has-more": false
}
```

- **`resultset`** — array of row objects (JSON format, default). Each object is `{ columnName: value, … }`.
- **`column-metadata`** — present when `"resultset-include-metadata": "ON"`. Each entry has `label` (column name) and `type`.
- **`has-more`** — `true` when additional pages exist. A `resultset-handle` UUID is also returned to fetch them.

#### Pagination — large result sets

Use `resultset-pagination-size` in the initial request. The server returns the first page and a `resultset-handle`. Call `"request-type": "next"` with that handle to retrieve subsequent pages until `has-more` is `false`.

**Step 1 — Initial query (page size = 100):**

```json
{
  "db-name": "myapp",
  "sql": "SELECT id, name, score FROM players ORDER BY id",
  "resultset-pagination-size": 100,
  "resultset-include-metadata": "ON"
}
```

**Response (first page, more rows available):**

```json
{
  "result": true,
  "message": "OK",
  "column-metadata": [ { "label": "id", "type": "INTEGER" }, { "label": "name", "type": "TEXT" }, { "label": "score", "type": "INTEGER" } ],
  "resultset": [ { "id": 1, "name": "Alice", "score": 100 }, "…99 more rows…" ],
  "resultset-handle": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "has-more": true
}
```

**Step 2 — Fetch next page:**

```json
{
  "request-type": "next",
  "resultset-handle": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "resultset-pagination-size": 100
}
```

Repeat until `has-more` is `false`. The handle is automatically released on the last page, or if the session times out.

**Full pagination loop in Python:**

```python
r = execute_sql("myapp", None, "SELECT id, name, score FROM players ORDER BY id",
                resultset_pagination_size=100, include_metadata=True)

# Print header
print("\t".join(col["label"] for col in r.column_metadata))

current = r
while True:
    for row in current.result_set:
        print(f"{row['id']}\t{row['name']}\t{row['score']}")
    if not current.has_more or not current.resultset_handle:
        break
    current = next_page(current.resultset_handle)
    if not current.result:
        raise Exception(f"Pagination error: {current.message}")
```

#### DB data format — columnar arrays

Pass `"resultset-data-format": "DB"` (with metadata on) to receive rows as value arrays instead of `{name: value}` objects. This reduces payload size for wide tables.

```json
{
  "db-name": "myapp",
  "sql": "SELECT id, name, score FROM players ORDER BY id",
  "resultset-data-format": "DB",
  "resultset-include-metadata": "ON"
}
```

**Response:**

```json
{
  "result": true,
  "column-metadata": [ { "label": "id" }, { "label": "name" }, { "label": "score" } ],
  "resultset": [
    [1, "Alice", 100],
    [2, "Bob",   200]
  ],
  "has-more": false
}
```

Column order in each row array matches the order of `column-metadata`. The same `resultset-data-format` field can be passed to `"request-type": "next"` calls.

### Close

```json
{ "db-name": "myapp", "sql": "close" }
```

## SDK Samples

Ready-to-run client samples are in `sdk-source/` covering all core API patterns:

| Language | Directory |
|---|---|
| Java | `sdk-source/java/` |
| Python | `sdk-source/python/` |
| C# | `sdk-source/c#/` |
| C++ | `sdk-source/cpp/` |
| Go | `sdk-source/go/` |
| Rust | `sdk-source/rust/` |
| Ruby | `sdk-source/ruby/` |
| Node.js | `sdk-source/node.js/` |

See `sdk-source/GETTING_STARTED.md` for run instructions and `sdk-source/LANGUAGE_QUICKSTART.md` for per-language setup.

## Build

```bash
cd synclite-db/root/core
mvn -Drevision=oss clean install
```

Built artifact: `root/core/target/synclite-db-core-oss.jar`

## Related Components

| Component | Role |
|---|---|
| [SyncLite Logger](https://github.com/syncliteio/synclite-logger-java) | Native Java/Python JDBC driver (embedded, no HTTP overhead) |
| [SyncLite Client](https://github.com/syncliteio/synclite-client) | CLI client that can connect to SyncLite DB |
| [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) | Consumes the sync logs and replicates to destination |

## Documentation & Community

- Full documentation: https://www.synclite.io/resources/documentation
- Website: https://www.synclite.io
- Slack: https://join.slack.com/t/syncliteworkspace/shared_invite/zt-2pz945vva-uuKapsubC9Mu~uYDRKo6Jw

---

← Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)

