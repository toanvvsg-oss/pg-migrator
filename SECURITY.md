# Security & Supply Chain Hardening

This document summarizes the **Supply Chain Attack** mitigations and infrastructure security controls applied in this project:

## 1. Minimal Dependency Footprint (Supply Chain Surface Reduction)
- **Go Backend**: Only **1 direct dependency** (`github.com/jackc/pgx/v5 v5.7.1`). All HTTP routing, AES-256-GCM encryption, JSON handling, and SSE streaming use the Go standard library (`net/http`, `crypto/*`). No third-party frameworks or ORMs.
- **Frontend (React + Vite)**:
  - Only **2 runtime dependencies** (`react@18.3.1`, `react-dom@18.3.1`) and **5 official devDependencies** (`vite@8.3.8`, `@vitejs/plugin-react@6.3.0`, `typescript@5.6.3`, `@types/*`) — **28 packages total** across the entire dependency tree, verified with `npm audit` (**0 vulnerabilities**). No bloated UI kits or charting libraries (the lag chart is rendered with pure SVG).
  - Enforces `ignore-scripts=true` in `.npmrc` and `npm ci --ignore-scripts` against `package-lock.json` in `Dockerfile` → **completely blocks npm `postinstall` malware** and guarantees deterministic package hashes.
  - Exact version pinning (no `^` or `~` ranges).
- **Python Lab Containers (`seeder`, `workload`)**:
  - Exact version pinning (`duckdb==1.1.3`, `psycopg[binary]==3.2.3`) with mandatory `--only-binary=:all:` → **prevents arbitrary code execution in `setup.py`** during source tarball builds.
- **Docker Base Images**:
  - Uses only Official Library images with pinned versions (`postgres:15.8-alpine`, `postgres:16.4-alpine`, `golang:1.23.4-alpine3.20`, `node:22.12.0-alpine3.20`, `nginx:1.27.3-alpine`, `python:3.12.8-slim`, `ghcr.io/shopify/toxiproxy:2.9.0`).
  - Run `make audit-supply-chain` to inspect `RepoDigests` (`@sha256:...`) if you want to pin images by immutable digest before production use.

## 2. Strict Network Isolation (Zero Egress / Exfiltration Blocking)
- `meta-net` (`internal: true`): Connects only `backend` ↔ `pg-meta`, **with no internet egress**.
- `migration-net` (`internal: true`): Contains only `pg-source`, `pg-target`, `pgbouncer`, `toxiproxy`, `workload-sim`, and `backend` — **no internet egress** and no PostgreSQL ports exposed to the host.
- `frontend` binds strictly to `127.0.0.1:8080` (localhost-only access).

## 3. Credential & Database Hardening
- **No hardcoded passwords**: `make init-env` generates random passwords and `APP_ENCRYPTION_KEY` via `openssl rand` and sets `chmod 600 .env`.
- **Encryption at Rest**: Source/Target database passwords stored in `pg-meta` are encrypted with **AES-256-GCM** and are never returned by the JSON API (`json:"-"`).
- **`scram-sha-256` authentication** enforced across all PostgreSQL instances (`pg-meta`, `pg-source`, `pg-target`).
- **Least Privilege (`NOSUPERUSER`)**: The `migrator` role on Target PG 16 uses the standard `GRANT pg_create_subscription` role instead of `SUPERUSER`.
- **Hardened Containers**: All application containers run as **non-root**, with `no-new-privileges:true` and `cap_drop: [ALL]`.
- **SQL Injection & Command Injection Prevention**: Uses `pgx.Identifier.Sanitize()`, parameterized queries (`$1, $2`), and invokes `pg_dump`/`psql` directly without a shell (`PGPASSWORD` passed via environment variable rather than CLI arguments).
