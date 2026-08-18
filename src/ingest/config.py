from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "ingest"
    host: str = "127.0.0.1"
    port: int = 8080
    data_dir: Path = Path("./data")

    database_url: str = "sqlite+aiosqlite:///./data/ingest.db"
    lancedb_path: Path = Path("./data/lancedb")
    lance_table: str = "chunks"

    # Comma-separated bootstrap watch paths (created as sources on startup if missing)
    watch_paths: str = ""

    worker_concurrency: int = 2
    reconcile_interval_seconds: int = 60
    queue_maxsize: int = 1000

    # Embedding defaults for the system index config
    embedder_provider: str = "deterministic"  # deterministic | local | openai_compatible
    embedder_model: str = "hash-384"
    embedder_dimension: int = 384
    openai_base_url: str = "https://api.openai.com/v1"
    openai_api_key: str = ""
    openai_model: str = "text-embedding-3-small"

    chunk_size: int = 800
    chunk_overlap: int = 100

    supported_extensions: tuple[str, ...] = (
        ".pdf",
        ".md",
        ".markdown",
        ".txt",
        ".html",
        ".htm",
    )

    @property
    def watch_path_list(self) -> list[Path]:
        if not self.watch_paths.strip():
            return []
        return [Path(p.strip()).expanduser().resolve() for p in self.watch_paths.split(",") if p.strip()]

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.lancedb_path.mkdir(parents=True, exist_ok=True)
        db_path = self._sqlite_path()
        if db_path is not None:
            db_path.parent.mkdir(parents=True, exist_ok=True)

    def _sqlite_path(self) -> Path | None:
        url = self.database_url
        prefixes = ("sqlite+aiosqlite:///", "sqlite:///")
        for prefix in prefixes:
            if url.startswith(prefix):
                raw = url[len(prefix) :]
                if raw == ":memory:":
                    return None
                return Path(raw)
        return None


@lru_cache
def get_settings() -> Settings:
    return Settings()
