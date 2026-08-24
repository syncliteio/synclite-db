
# SyncLite DB — Local-First, Sync-Enabled Database Server


> Part of the [SyncLite Platform](https://github.com/syncliteio/SyncLite) — Build Anything, Sync Anywhere.


## What is SyncLite DB?

**SyncLite DB** is a local-first, sync-enabled database server that wraps popular embedded databases — SQLite, DuckDB, Apache Derby, H2, and HyperSQL — and exposes them over HTTP as a JSON API.

Optimized for running on localhost, edge devices, or user workstations, SyncLite DB enables robust, offline-capable, and sync-ready applications. While designed for local-first deployment, it can securely handle remote requests when needed (e.g., for admin, monitoring, or hybrid scenarios).

Whereas [SyncLite Logger](https://github.com/syncliteio/synclite-logger-java) is an embeddable JDBC library for Java, and [SyncLite Runtime](https://github.com/syncliteio/SyncLite/tree/main/synclite-logger-rust) serves Rust/Python/C++ embeddings, SyncLite DB is the **language-agnostic alternative**: any application written in any language (Java, Python, C++, C#, Go, Rust, Ruby, Node.js, and more) can send SQL requests as JSON over HTTP and have them executed on the embedded database and automatically synced through the SyncLite pipeline to the destination database.

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


## Starting SyncLite DB

SyncLite DB is now deployed as a web application (WAR) and provides a browser-based GUI for configuration and management.

### Quick Start (GUI/WAR Deployment)

1. **Deploy the WAR:**  
  - Copy `synclite-db-1.1.0.war` (from `root/web/target/` or your platform's `tools/synclite-db/`) into the `webapps/` directory of your Apache Tomcat server.
  - Start Tomcat (see platform or Tomcat documentation).

2. **Access the Web UI:**  
  - Open your browser and go to:  
    `http://localhost:8080/synclite-db`  
    (Adjust port if your Tomcat uses a different one.)

3. **Configure & Start:**  
  - Use the web interface to configure databases, logger options, and start/stop the SyncLite DB server.
  - All management, monitoring, and job setup is now available via the GUI.

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
    "device-stage-type": "FS"
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

## Authentication

SyncLite DB supports two independent authentication modes, configured in `synclite_db.conf`.

### Mode 1 — Global Token

A shared secret token. Every request carrying the correct token is accepted.

**Server configuration (`synclite_db.conf`):**

```properties
auth-token = change-me-to-a-long-random-value
```

**Client — send the token as an HTTP header:**

```python
import requests

headers = {"X-SyncLite-Token": "change-me-to-a-long-random-value"}
requests.post("http://localhost:5555/synclite",
              json={"db-name": "myapp", "sql": "SELECT 1"},
              headers=headers)
```

Or via curl:

```bash
curl -X POST http://localhost:5555/synclite \
  -H "Content-Type: application/json" \
  -H "X-SyncLite-Token: change-me-to-a-long-random-value" \
  -d '{"db-name": "myapp", "sql": "SELECT 1"}'
```

The environment variable `SYNCLITE_DB_AUTH_TOKEN` is the conventional way SDK samples pick up this token.

### Mode 2 — Per-App HMAC Signed Requests

Each registered application has its own `app-id` and `app-secret`. Every request is signed with HMAC-SHA256 over a canonical string that includes a timestamp, a nonce, and the SHA-256 hash of the request body. This prevents replay attacks and body tampering.

**Server configuration (`synclite_db.conf`):**

```properties
enable-app-auth = true
authorized-apps = app1,app2

app.app1.secret = replace-with-long-random-secret
app.app1.allowed-ops = initialize,begin,commit,rollback,select,next,execute,close

app.app2.secret = replace-with-another-secret
app.app2.allowed-ops = select,next,execute
```

**`allowed-ops` values:** `initialize` · `close` · `begin` · `commit` · `rollback` · `select` · `next` · `execute`

**Client — sign each request:**

```python
import requests, json, hashlib, hmac, base64, time, uuid

APP_ID     = "app1"
APP_SECRET = "replace-with-long-random-secret"
BASE_URL   = "http://localhost:5555/synclite"

def signed_post(payload: dict) -> dict:
    body      = json.dumps(payload, separators=(",", ":"))
    timestamp = str(int(time.time() * 1000))
    nonce     = str(uuid.uuid4())
    body_hash = hashlib.sha256(body.encode()).hexdigest()
    canonical = f"POST\n/\n{timestamp}\n{nonce}\n{body_hash}"
    sig       = base64.b64encode(
                    hmac.new(APP_SECRET.encode(), canonical.encode(),
                             hashlib.sha256).digest()
                ).decode()
    headers = {
        "Content-Type":         "application/json",
        "X-SyncLite-App-Id":    APP_ID,
        "X-SyncLite-Timestamp": timestamp,
        "X-SyncLite-Nonce":     nonce,
        "X-SyncLite-Signature": sig,
    }
    return requests.post(BASE_URL, data=body, headers=headers).json()

signed_post({"db-name": "myapp", "sql": "SELECT 1"})
```

The environment variables `SYNCLITE_DB_APP_ID` and `SYNCLITE_DB_APP_SECRET` are the conventional way SDK samples pick up credentials.

See [DOCUMENTATION.md](../DOCUMENTATION.md#73-authentication) for additional details including advanced tuning parameters (`app-auth-timestamp-skew-ms`, `app-auth-nonce-ttl-ms`, `app-auth-nonce-cache-max-entries`) and a Java signing example.

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
mvn -Drevision=1.1.0 clean install
```

Built artifact: `root/core/target/synclite-db-core-1.1.0.jar`

## Related Components

| Component | Role |
|---|---|
| [SyncLite Logger](https://github.com/syncliteio/synclite-logger-java) | Java JDBC logger (embedded, no HTTP overhead) |
| [SyncLite Runtime](https://github.com/syncliteio/SyncLite/tree/main/synclite-logger-rust) | Full Rust runtime (logger + consolidator), consumable from Rust, Python, and C++ |
| [SyncLite Client](https://github.com/syncliteio/synclite-client) | CLI client that can connect to SyncLite DB |
| [SyncLite Consolidator](https://github.com/syncliteio/synclite-consolidator) | Consumes the sync logs and replicates to destination |

## Documentation & Community

- Full documentation: https://github.com/syncliteio/SyncLite/blob/main/DOCUMENTATION.md
- Website: https://www.synclite.io
- Community: https://github.com/syncliteio/SyncLite/issues

---

← Back to the [SyncLite Platform README](https://github.com/syncliteio/SyncLite/blob/main/README.md)

