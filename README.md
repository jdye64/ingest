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
uv run ingest serve --host 127.0.0.1 --port 8080
uv run ingest mcp          # stdio MCP for Cursor
```

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
| GET | `/api/v1/status` | Status counts + queue depth |
| GET/POST | `/api/v1/sources` | List / add watch dirs |
| GET | `/api/v1/documents` | List documents |
| GET | `/api/v1/documents/{id}` | Provenance + latest run/config |
| POST | `/api/v1/documents/{id}/reindex` | Force reindex |
| GET | `/api/v1/search?q=` | Vector search |

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
  cli.py            # ingest serve | ingest mcp
  api/              # REST
  web/              # HTMX portal
  mcp/              # MCP tools
  pipeline/         # parse → chunk → embed
  watcher/          # directory monitoring + SHA
  vectors/          # LanceDB
  db/               # SQLModel + Alembic
```
# ingest
