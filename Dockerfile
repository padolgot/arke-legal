# --- Stage 1: build dashboard ---
FROM node:22-slim AS frontend
WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json* ./
RUN npm ci
COPY dashboard/ .
RUN npm run build

# --- Stage 2: Python runtime ---
FROM python:3.12-slim
WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
COPY arke/ arke/
RUN uv pip install --system .

COPY --from=frontend /app/dashboard/dist arke/static

EXPOSE 8000
CMD ["arke", "serve"]
