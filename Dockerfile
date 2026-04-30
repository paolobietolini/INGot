# --- Builder ---
FROM  mirror.gcr.io/python:3.12-slim-trixie AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /code

COPY pyproject.toml uv.lock ./

RUN uv sync --no-dev


# --- Runtime ---
FROM python:3.12-slim-trixie


RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code
RUN mkdir -p /code/raw /code/output

COPY --from=builder /code/.venv /code/.venv

COPY . .

ENV PATH="/code/.venv/bin:$PATH"
ENV JAVA_HOME="/usr/lib/jvm/java-21-openjdk-amd64"

ENTRYPOINT ["python", "main.py"]
CMD ["--input", "raw", "--out", "output", "--format", "parquet"]