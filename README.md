# Flexible RAG Ingest

Lightweight, performant document ingest + RAG retrieval system.

- Watches configurable directories and indexes new/changed files (content SHA-256)
- Stores embeddings in **LanceDB**
- Tracks provenance (index configs + runs) in **SQLite** by default, or **Postgres** via `DATABASE_URL`
- Long-running **web portal** + **REST API**
- **MCP server** for Cursor / MaaS-style clients (`stdio` or HTTP at `/mcp`)

## Quickstart

```bash
uv sync
cp .env.example .env
# optional: put files in ./watch_sample
mkdir -p watch_sample
echo "Hello RAG" > watch_sample/hello.txt

uv run ingest serve
```

Open:

- Portal: http://127.0.0.1:8080/
- OpenAPI: http://127.0.0.1:8080/docs
- Health: http://127.0.0.1:8080/api/v1/health
- MCP (streamable HTTP): http://127.0.0.1:8080/mcp

## CLI

```bash
uv run ingest serve --host 0.0.0.0 --port 8080
uv run ingest mcp          # stdio MCP for Cursor
uv run ingest ingestor --server-url http://127.0.0.1:8080 --ingestor-id edge-1 --api-key <key>
```

## Multi-ingestor ingestion

Multiple remote **ingestors** can index local files and push results into one central server (metadata DB + LanceDB). The portal assigns watch paths to each ingestor by ID.

1. Start the central server: `uv run ingest serve`
2. Open **Ingestors** in the portal, create an ingestor, and copy the one-time API key
3. On **Sources**, add a directory and set **Owner** to that ingestor (path must exist on the ingestor host)
4. On the ingestor host:

```bash
uv run ingest ingestor \
  --server-url http://<central-host>:8080 \
  --ingestor-id <id> \
  --api-key <key>
```

Ingestors heartbeat their status; the dashboard/ingestors/documents pages refresh live. Sources with no ingestor owner are still watched and indexed by the local `serve` process.

## Cursor MCP (stdio)

Add to your MCP config:

```json
{
  "mcpServers": {
    "ingest": {
      "command": "uv",
      "args": ["run", "--directory", "/absolute/path/to/ingest", "ingest", "mcp"],
      "env": {
        "DATABASE_URL": "sqlite+aiosqlite:///./data/ingest.db",
        "LANCEDB_PATH": "./data/lancedb",
        "EMBEDDER_PROVIDER": "deterministic"
      }
    }
  }
}
```

### MCP tools

- `list_documents` / `get_document`
- `search`
- `list_sources`
- `reindex_document`
- `status_summary`

## REST API (selected)

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/v1/health` | Health |
| GET | `/api/v1/status` | Status counts + queue depth + ingestor counts |
| GET | `/api/v1/events` | SSE live status stream |
| GET/POST | `/api/v1/sources` | List / add watch dirs (`ingestor_id` optional) |
| POST | `/api/v1/sources/{id}/enable` | Enable/disable a source |
| DELETE | `/api/v1/sources/{id}` | Delete source and purge its docs from DB + VDB |
| GET | `/api/v1/sources/audit` | Source create/enable/disable/delete audit log |
| GET | `/api/v1/documents` | List documents |
| GET | `/api/v1/documents/{id}` | Provenance + latest run/config |
| POST | `/api/v1/documents/{id}/reindex` | Force reindex |
| GET | `/api/v1/search?q=` | Vector search |
| GET | `/api/v1/index-config/default` | Default index config (ingestors use this) |
| POST | `/api/v1/ingestors` | Create ingestor (returns API key once) |
| GET | `/api/v1/ingestors` | List ingestors |
| POST | `/api/v1/ingestors/me/heartbeat` | Ingestor heartbeat (`X-Ingestor-Id` + key) |
| GET | `/api/v1/ingestors/me/sources` | Sources assigned to this ingestor |
| POST | `/api/v1/ingestors/me/documents/upsert` | Report file discover/change/delete (atomic claim) |
| GET | `/api/v1/ingestors/me/documents/check` | Pre-flight: already indexed / claim in progress? |
| POST | `/api/v1/ingestors/me/documents/{id}/index` | Push chunks + vectors (claim owner only) |
| POST | `/api/v1/ingestors/me/documents/{id}/fail` | Report indexing error |

## Configuration

See [`.env.example`](.env.example). Important knobs:

- `DATABASE_URL` — `sqlite+aiosqlite:///./data/ingest.db` or `postgresql+asyncpg://...`
- `LANCEDB_PATH` — vector store directory
- `WATCH_PATHS` — comma-separated bootstrap directories
- `EMBEDDER_PROVIDER` — `deterministic` (default, offline), `local` (sentence-transformers), or `openai_compatible`

## Postgres (optional)

```bash
docker compose --profile postgres up -d postgres
export DATABASE_URL=postgresql+asyncpg://ingest:ingest@127.0.0.1:5432/ingest
uv run alembic upgrade head
uv run ingest serve
```

Or run app + Postgres:

```bash
docker compose --profile postgres --profile app up --build
```

## Migrations

App also calls `create_all` on startup for convenience. For production-ish Postgres:

```bash
uv run alembic upgrade head
```

## Embeddings

Default `deterministic` embedder is fast and offline (great for bring-up/tests). For quality retrieval:

```bash
EMBEDDER_PROVIDER=local
EMBEDDER_MODEL=sentence-transformers/all-MiniLM-L6-v2
EMBEDDER_DIMENSION=384
```

Changing embedder settings creates a need to reindex existing documents (use Reindex in the UI/API).

## Development

```bash
uv sync
uv run pytest
```

## Layout

```
src/ingest/
  app.py            # FastAPI + lifespan (watcher/workers/MCP)
  cli.py            # ingest serve | ingest mcp | ingest ingestor
  ingestor/         # Remote ingestor client + local watch/index
  api/              # REST (+ ingestor endpoints, SSE)
  web/              # HTMX portal
  mcp/              # MCP tools
  pipeline/         # parse → chunk → embed
  watcher/          # directory monitoring + SHA
  vectors/          # LanceDB
  db/               # SQLModel + Alembic
```
