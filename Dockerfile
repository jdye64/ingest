FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml README.md ./
COPY src ./src

RUN uv sync --no-dev

ENV HOST=0.0.0.0
ENV PORT=8080
EXPOSE 8080

CMD ["uv", "run", "ingest", "serve"]
