# 
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

# copy dependency files dulu
COPY pyproject.toml uv.lock ./

# install semua dependencies dari uv.lock
RUN uv sync --locked

# copy script setelah dependencies supaya cache efisien
COPY ingest_data.py ingest_data.py

ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]
