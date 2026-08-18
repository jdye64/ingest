from __future__ import annotations

import argparse
import asyncio
import logging
import signal

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

    ingestor = sub.add_parser("ingestor", help="Run a remote ingestor against a central server")
    ingestor.add_argument("--server-url", required=True, help="Central server base URL, e.g. http://127.0.0.1:8080")
    ingestor.add_argument("--ingestor-id", required=True, help="Unique ingestor id configured in the portal")
    ingestor.add_argument("--api-key", required=True, help="API key issued when the ingestor was created")
    ingestor.add_argument("--verbose", action="store_true")

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

    if args.command == "ingestor":
        level = logging.DEBUG if args.verbose else logging.INFO
        logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        from ingest.ingestor.client import IngestorClient
        from ingest.ingestor.service import IngestorRunner

        async def _run() -> None:
            client = IngestorClient(args.server_url, args.ingestor_id, args.api_key)
            runner = IngestorRunner(client, settings=settings)
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, runner.request_stop)
                except NotImplementedError:
                    pass
            logging.getLogger(__name__).info(
                "Starting ingestor %s against %s", args.ingestor_id, args.server_url
            )
            await runner.run()

        asyncio.run(_run())
        return


if __name__ == "__main__":
    main()
