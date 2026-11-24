import json
import os
import re
from http.server import BaseHTTPRequestHandler, HTTPServer
import psycopg2

INI_PATH = "/etc/pgbouncer/pgbouncer.ini"
ADMIN_PASS = os.environ.get("PGBOUNCER_ADMIN_PASSWORD", "")
ADMIN_DSN = f"host=127.0.0.1 port=6432 dbname=pgbouncer user=pgbouncer_admin password={ADMIN_PASS}"
ALLOWED_HOSTS = {"pg-source", "pg-target"}


def exec_admin(sql: str):
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            try:
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            except Exception:
                return []
    finally:
        conn.close()


def current_backend():
    with open(INI_PATH, "r", encoding="utf-8") as f:
        text = f.read()
    m = re.search(r"^appdb\s*=\s*(.+)$", text, flags=re.MULTILINE)
    return m.group(1).strip() if m else ""


def update_backend(host: str, port: int, dbname: str):
    if host not in ALLOWED_HOSTS:
        raise ValueError(f"Host {host!r} not in allowed targets")
    if not re.fullmatch(r"[a-zA-Z0-9_]+", dbname):
        raise ValueError("Invalid database name")
    if not (1 <= port <= 65535):
        raise ValueError("Invalid port")

    with open(INI_PATH, "r", encoding="utf-8") as f:
        text = f.read()
    new_line = f"appdb = host={host} port={port} dbname={dbname}"
    updated, count = re.subn(r"^appdb\s*=\s*.+$", new_line, text, flags=re.MULTILINE)
    if count == 0:
        raise RuntimeError("Could not find appdb entry in pgbouncer.ini")
    with open(INI_PATH, "w", encoding="utf-8") as f:
        f.write(updated)


class Handler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        if self.path == "/status":
            try:
                pools = exec_admin("SHOW POOLS;")
                self._send(200, {"backend": current_backend(), "pools": pools})
            except Exception as e:
                self._send(500, {"error": str(e)})
            return
        self._send(404, {"error": "not found"})

    def do_POST(self):
        try:
            body = self._read_json()
            if self.path == "/pause":
                exec_admin("PAUSE appdb;")
                self._send(200, {"status": "paused", "backend": current_backend()})
            elif self.path == "/resume":
                exec_admin("RESUME appdb;")
                self._send(200, {"status": "resumed", "backend": current_backend()})
            elif self.path == "/switch":
                host = body.get("host", "pg-target")
                port = int(body.get("port", 5432))
                dbname = body.get("dbname", "targetdb")
                update_backend(host, port, dbname)
                exec_admin("RELOAD;")
                self._send(200, {"status": "switched", "backend": current_backend()})
            elif self.path == "/reset":
                update_backend("pg-source", 5432, "sourcedb")
                exec_admin("RELOAD;")
                try:
                    exec_admin("RESUME appdb;")
                except Exception:
                    pass
                exec_admin("RECONNECT appdb;")
                self._send(200, {"status": "reset", "backend": current_backend()})
            else:
                self._send(404, {"error": "not found"})
        except Exception as e:
            self._send(500, {"error": str(e)})


if __name__ == "__main__":
    port = int(os.environ.get("CONTROL_PORT", "8091"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    print(f"[pgbouncer-control] listening on :{port}", flush=True)
    server.serve_forever()
