FROM oven/bun:1 AS runtime

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
 && mv /root/.local/bin/uv /usr/local/bin/uv

WORKDIR /app

COPY package.json bun.lock* ./
RUN bun install --frozen-lockfile || bun install

COPY .python-version pyproject.toml uv.lock* ./
RUN uv python install "$(cat .python-version)" \
 && uv sync --frozen --no-dev || uv sync --no-dev

COPY . .
RUN uv run dbt deps

CMD ["bun", "run", "dbt:run"]
