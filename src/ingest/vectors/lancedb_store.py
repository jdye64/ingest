from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import lancedb
import pyarrow as pa


@dataclass
class ChunkRecord:
    chunk_id: str
    document_id: str
    run_id: str
    source_id: str
    path: str
    chunk_index: int
    text: str
    vector: list[float]
    content_sha256: str
    metadata: dict[str, Any]


class LanceStore:
    def __init__(self, path: Path, table_name: str = "chunks", dimension: int = 384) -> None:
        self.path = Path(path)
        self.table_name = table_name
        self.dimension = dimension
        self.path.mkdir(parents=True, exist_ok=True)
        self._db = lancedb.connect(str(self.path))
        self._ensure_table()

    def _schema(self) -> pa.Schema:
        return pa.schema(
            [
                ("chunk_id", pa.string()),
                ("document_id", pa.string()),
                ("run_id", pa.string()),
                ("source_id", pa.string()),
                ("path", pa.string()),
                ("chunk_index", pa.int32()),
                ("text", pa.string()),
                ("vector", pa.list_(pa.float32(), self.dimension)),
                ("content_sha256", pa.string()),
                ("metadata_json", pa.string()),
            ]
        )

    def _table_names(self) -> set[str]:
        if hasattr(self._db, "list_tables"):
            listed = self._db.list_tables()
            tables = getattr(listed, "tables", None)
            if tables is not None:
                return set(tables)
            if isinstance(listed, list):
                return set(listed)
        return set(self._db.table_names())

    def _ensure_table(self) -> None:
        names = self._table_names()
        if self.table_name not in names:
            empty = pa.Table.from_pylist([], schema=self._schema())
            self._db.create_table(self.table_name, data=empty, mode="create")

    def _table(self):
        return self._db.open_table(self.table_name)

    def delete_document(self, document_id: str) -> None:
        table = self._table()
        table.delete(f"document_id = '{document_id}'")

    def upsert_chunks(self, records: list[ChunkRecord]) -> None:
        if not records:
            return
        document_id = records[0].document_id
        self.delete_document(document_id)
        import json

        rows = [
            {
                "chunk_id": r.chunk_id,
                "document_id": r.document_id,
                "run_id": r.run_id,
                "source_id": r.source_id,
                "path": r.path,
                "chunk_index": r.chunk_index,
                "text": r.text,
                "vector": r.vector,
                "content_sha256": r.content_sha256,
                "metadata_json": json.dumps(r.metadata or {}),
            }
            for r in records
        ]
        self._table().add(rows)

    def search(
        self,
        vector: list[float],
        *,
        limit: int = 10,
        source_id: str | None = None,
        path_contains: str | None = None,
    ) -> list[dict[str, Any]]:
        import json

        query = self._table().search(vector).limit(limit)
        where_parts: list[str] = []
        if source_id:
            where_parts.append(f"source_id = '{source_id}'")
        if path_contains:
            safe = path_contains.replace("'", "''")
            where_parts.append(f"path LIKE '%{safe}%'")
        if where_parts:
            query = query.where(" AND ".join(where_parts))
        rows = query.to_list()
        results: list[dict[str, Any]] = []
        for row in rows:
            meta = row.get("metadata_json") or "{}"
            try:
                metadata = json.loads(meta)
            except Exception:
                metadata = {}
            results.append(
                {
                    "chunk_id": row.get("chunk_id"),
                    "document_id": row.get("document_id"),
                    "run_id": row.get("run_id"),
                    "source_id": row.get("source_id"),
                    "path": row.get("path"),
                    "chunk_index": row.get("chunk_index"),
                    "text": row.get("text"),
                    "content_sha256": row.get("content_sha256"),
                    "score": row.get("_distance"),
                    "metadata": metadata,
                }
            )
        return results

    def health(self) -> dict[str, Any]:
        names = self._table_names()
        count = 0
        if self.table_name in names:
            count = self._table().count_rows()
        return {
            "path": str(self.path),
            "table": self.table_name,
            "tables": list(names),
            "row_count": count,
            "ok": True,
        }


def new_chunk_id() -> str:
    return str(uuid.uuid4())
