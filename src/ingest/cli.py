from __future__ import annotations

import argparse
import asyncio
import logging

import uvicorn

from ingest.config import get_settings


def main() -> None:
    parser = argparse.ArgumentParser(prog="ingest", description="Flexible RAG ingest system")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run web portal, REST API, watcher, and MCP HTTP endpoint")
    serve.add_argument("--host", default=None)
    serve.add_argument("--port", type=int, default=None)
    serve.add_argument("--reload", action="store_true")

    mcp = sub.add_parser("mcp", help="Run MCP server over stdio (for Cursor)")
    mcp.add_argument("--verbose", action="store_true")

    args = parser.parse_args()
    settings = get_settings()

    if args.command == "serve":
        host = args.host or settings.host
        port = args.port or settings.port
        uvicorn.run(
            "ingest.app:app",
            host=host,
            port=port,
            reload=args.reload,
            factory=False,
        )
        return

    if args.command == "mcp":
        level = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(level=level)
        from ingest.mcp.server import run_mcp_stdio

        asyncio.run(run_mcp_stdio())
        return


if __name__ == "__main__":
    main()
