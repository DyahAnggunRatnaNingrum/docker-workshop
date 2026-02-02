FROM python:3.13.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/


RUN pip install pandas pyarrow

WORKDIR /app

COPY "pyproject.toml" "uv.lock" ".python-version" ./

RUN uv sync --locked


COPY pipeline.py pipeline.py

ENTRYPOINT ["uv", "run", "python", "pipeline.py"]
CMD ["12"]