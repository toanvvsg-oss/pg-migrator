import collections
import json
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
import psycopg

DSN = os.environ["WORKLOAD_DSN"]
TARGET_TPS = float(os.environ.get("WORKLOAD_TPS", "10"))

state_lock = threading.Lock()
recent_txs = collections.deque(maxlen=30)
acknowledged_ids = set()

state = {
    "running": os.environ.get("WORKLOAD_AUTOSTART", "true").lower() == "true",
    "target_tps": TARGET_TPS,
    "inserts": 0,
    "updates": 0,
    "deletes": 0,
    "errors": 0,
    "last_tx_id": 0,
    "last_error": "",
    "current_db": "",
    "current_pg_version": "",
    "previous_db": "",
    "cutover_detected_at": "",
    "cutover_tx_boundary": 0,
    "max_latency_ms": 0.0,
    "avg_latency_ms": 0.0,
    "actual_tps": 0.0,
    "started_at": time.time(),
}


def execute_insert(account_id: int, amount: float, payload: str, source_tag: str):
    t0 = time.perf_counter()
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.live_transactions (account_id, amount, status, payload)
                VALUES (%s, %s, 'pending', %s)
                RETURNING tx_id, current_database(), current_setting('server_version')
                """,
                (account_id, amount, payload),
            )
            tx_id, db_name, pg_ver = cur.fetchone()
    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 1)

    now_iso = time.strftime("%H:%M:%S")
    with state_lock:
        if state["current_db"] and state["current_db"] != db_name:
            state["previous_db"] = state["current_db"]
            state["cutover_detected_at"] = now_iso
            state["cutover_tx_boundary"] = tx_id
        state["current_db"] = db_name
        state["current_pg_version"] = pg_ver.split(" ")[0]
        state["inserts"] += 1
        state["last_tx_id"] = tx_id
        if elapsed_ms > state["max_latency_ms"]:
            state["max_latency_ms"] = elapsed_ms
        acknowledged_ids.add(tx_id)
        if len(acknowledged_ids) > 5000:
            # Keep memory bounded while retaining recent IDs for integrity check
            for old_id in sorted(acknowledged_ids)[:1000]:
                acknowledged_ids.discard(old_id)
        recent_txs.appendleft(
            {
                "tx_id": tx_id,
                "account_id": account_id,
                "amount": amount,
                "payload": payload,
                "op": source_tag,
                "db": db_name,
                "pg_ver": pg_ver.split(" ")[0],
                "latency_ms": elapsed_ms,
                "time": now_iso,
            }
        )
    return {
        "tx_id": tx_id,
        "db": db_name,
        "pg_ver": pg_ver.split(" ")[0],
        "latency_ms": elapsed_ms,
    }


def verify_integrity():
    with state_lock:
        sample_ids = sorted(acknowledged_ids)[-200:]
    if not sample_ids:
        return {"checked": 0, "present": 0, "missing": 0, "db": state["current_db"]}
    with psycopg.connect(DSN, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*), current_database() FROM public.live_transactions WHERE tx_id = ANY(%s)",
                (sample_ids,),
            )
            present, db_name = cur.fetchone()
    return {
        "checked": len(sample_ids),
        "present": present,
        "missing": len(sample_ids) - present,
        "db": db_name,
    }


def worker_loop():
    window_count = 0
    window_start = time.time()
    latency_sum = 0.0
    latency_cnt = 0

    while True:
        with state_lock:
            is_running = state["running"]
            tps = max(1.0, float(state["target_tps"]))

        if not is_running:
            time.sleep(0.4)
            window_count = 0
            window_start = time.time()
            continue

        t0 = time.perf_counter()
        try:
            op = random.random()
            if op < 0.72:
                res = execute_insert(
                    random.randint(1001, 5000),
                    round(random.uniform(5.0, 999.99), 2),
                    f"auto-order-{int(time.time() * 1000)}",
                    "INSERT",
                )
                elapsed_ms = res["latency_ms"]
            elif op < 0.94:
                with psycopg.connect(DSN, autocommit=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE public.live_transactions
                            SET status = 'settled', updated_at = NOW()
                            WHERE tx_id = (
                                SELECT tx_id FROM public.live_transactions
                                WHERE status = 'pending'
                                ORDER BY tx_id DESC
                                LIMIT 1
                            )
                            RETURNING current_database(), current_setting('server_version')
                            """
                        )
                        row = cur.fetchone()
                elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                with state_lock:
                    state["updates"] += 1
                    if row:
                        if state["current_db"] and state["current_db"] != row[0]:
                            state["previous_db"] = state["current_db"]
                            state["cutover_detected_at"] = time.strftime("%H:%M:%S")
                        state["current_db"] = row[0]
                        state["current_pg_version"] = row[1].split(" ")[0]
                    if elapsed_ms > state["max_latency_ms"]:
                        state["max_latency_ms"] = elapsed_ms
            else:
                with psycopg.connect(DSN, autocommit=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            DELETE FROM public.live_transactions
                            WHERE tx_id = (
                                SELECT tx_id FROM public.live_transactions
                                WHERE tx_id < (SELECT COALESCE(max(tx_id), 0) - 2000 FROM public.live_transactions)
                                ORDER BY tx_id ASC
                                LIMIT 1
                            )
                            """
                        )
                elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 1)
                with state_lock:
                    state["deletes"] += 1

            window_count += 1
            latency_sum += elapsed_ms
            latency_cnt += 1

            now = time.time()
            if now - window_start >= 2.0:
                actual = window_count / (now - window_start)
                avg_lat = latency_sum / max(1, latency_cnt)
                with state_lock:
                    state["actual_tps"] = round(actual, 1)
                    state["avg_latency_ms"] = round(avg_lat, 1)
                window_count = 0
                window_start = now
                latency_sum = 0.0
                latency_cnt = 0

        except Exception as e:
            with state_lock:
                state["errors"] += 1
                state["last_error"] = str(e)[:240]
            time.sleep(0.25)

        sleep_for = max(0.0, (1.0 / tps) - (time.perf_counter() - t0))
        if sleep_for > 0:
            time.sleep(sleep_for)


DEMO_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Example App — CloudPay Live Orders</title>
  <style>
    :root { --bg:#090e17; --card:#111928; --card2:#172236; --border:#24324c; --text:#e8eef8; --muted:#8fa2c0; --blue:#3b82f6; --green:#10b981; --amber:#f59e0b; --red:#ef4444; }
    * { box-sizing: border-box; }
    body { margin:0; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,sans-serif; background:var(--bg); color:var(--text); padding:22px; }
    .wrap { max-width:1100px; margin:0 auto; display:flex; flex-direction:column; gap:16px; }
    .header { display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px; background:var(--card); border:1px solid var(--border); border-radius:12px; padding:16px 20px; }
    .title h1 { margin:0; font-size:20px; }
    .title p { margin:4px 0 0; font-size:12px; color:var(--muted); }
    .db-pill { padding:8px 14px; border-radius:999px; font-weight:700; font-size:13px; font-family:ui-monospace,Menlo,monospace; border:1px solid; }
    .db-source { background:rgba(59,130,246,0.16); color:#60a5fa; border-color:#3b82f6; }
    .db-target { background:rgba(16,185,129,0.18); color:#34d399; border-color:#10b981; }
    .banner { background:rgba(16,185,129,0.14); border:1px solid #10b981; color:#a7f3d0; padding:12px 16px; border-radius:10px; font-size:13px; display:none; }
    .kpis { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:12px; }
    .kpi { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:14px; }
    .kpi .l { font-size:11px; color:var(--muted); text-transform:uppercase; }
    .kpi .v { font-size:22px; font-weight:700; margin-top:4px; font-variant-numeric:tabular-nums; }
    .kpi .s { font-size:11px; color:var(--muted); margin-top:2px; }
    .card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:16px; }
    .row { display:flex; align-items:center; gap:10px; flex-wrap:wrap; }
    .btn { background:var(--card2); border:1px solid var(--border); color:var(--text); padding:7px 12px; border-radius:7px; font-size:13px; cursor:pointer; }
    .btn:hover { border-color:var(--blue); }
    .btn-primary { background:var(--blue); border-color:var(--blue); color:#fff; font-weight:600; }
    .btn-green { background:var(--green); border-color:var(--green); color:#04150e; font-weight:600; }
    input { background:var(--bg); border:1px solid var(--border); color:var(--text); padding:7px 10px; border-radius:7px; font-size:13px; }
    table { width:100%; border-collapse:collapse; font-size:13px; margin-top:10px; }
    th, td { text-align:left; padding:7px 10px; border-bottom:1px solid var(--border); }
    th { color:var(--muted); font-size:11px; text-transform:uppercase; }
    .mono { font-family:ui-monospace,Menlo,monospace; }
    .tag { display:inline-block; padding:2px 7px; border-radius:999px; font-size:11px; font-weight:600; }
    .tag-src { background:rgba(59,130,246,0.16); color:#60a5fa; }
    .tag-tgt { background:rgba(16,185,129,0.16); color:#34d399; }
    .tag-man { background:rgba(245,158,11,0.18); color:#fbbf24; }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <div class="title">
        <h1>CloudPay Demo App (Connected via PgBouncer)</h1>
        <p>Continuously inserts &amp; updates live orders through <code>pgbouncer:6432/appdb</code> — open side-by-side with <a href="http://127.0.0.1:8080" target="_blank" style="color:#60a5fa;">PG Migrator UI (:8080)</a></p>
      </div>
      <div id="dbPill" class="db-pill db-source">Connecting...</div>
    </div>

    <div id="cutoverBanner" class="banner"></div>

    <div class="kpis">
      <div class="kpi"><div class="l">Live Throughput</div><div class="v" id="kTps">0 TPS</div><div class="s" id="kTargetTps">Target: 10 TPS</div></div>
      <div class="kpi"><div class="l">Latest Sequence ID</div><div class="v mono" id="kSeq">#0</div><div class="s">BIGSERIAL tx_id</div></div>
      <div class="kpi"><div class="l">Total Writes</div><div class="v" id="kWrites">0</div><div class="s" id="kOps">0 ins / 0 upd / 0 del</div></div>
      <div class="kpi"><div class="l">Failed Requests</div><div class="v" id="kErr" style="color:var(--green)">0</div><div class="s" id="kLastErr">Zero dropped writes</div></div>
      <div class="kpi"><div class="l">Max Latency Spike</div><div class="v" id="kLat">0 ms</div><div class="s" id="kAvgLat">Avg: 0 ms</div></div>
    </div>

    <div class="card">
      <div class="row" style="justify-content:space-between;">
        <div class="row">
          <button class="btn btn-primary" id="btnToggle" onclick="toggleRun()">Pause Traffic</button>
          <button class="btn" onclick="setTps(5)">5 TPS</button>
          <button class="btn" onclick="setTps(15)">15 TPS</button>
          <button class="btn" onclick="setTps(30)">30 TPS</button>
          <button class="btn" onclick="resetStats()">Reset Error/Max Latency</button>
          <button class="btn btn-green" onclick="checkIntegrity()">Verify No Lost Writes</button>
        </div>
        <div class="row">
          <input id="customNote" placeholder="Custom order note..." value="VIP-ORDER-TEST" style="width:170px;" />
          <button class="btn btn-primary" onclick="manualInsert()">+ Insert Row Now</button>
        </div>
      </div>
      <div id="integrityOut" style="margin-top:10px; font-size:12px; color:var(--muted);"></div>
    </div>

    <div class="card">
      <div class="row" style="justify-content:space-between;">
        <strong>Live Stream of Recent Writes (newest first)</strong>
        <span style="font-size:12px; color:var(--muted);">Watch the Database column flip from <code>sourcedb (PG 15)</code> to <code>targetdb (PG 16)</code> with continuous <code>tx_id</code></span>
      </div>
      <table>
        <thead>
          <tr><th>Time</th><th>tx_id (Sequence)</th><th>Type</th><th>Account</th><th>Amount</th><th>Payload</th><th>Database Written To</th><th>Latency</th></tr>
        </thead>
        <tbody id="txBody"></tbody>
      </table>
    </div>
  </div>

  <script>
    let currentRunning = true;
    let currentTargetTps = 10;

    async function poll() {
      try {
        const res = await fetch('/api/feed');
        const data = await res.json();
        const s = data.stats;
        currentRunning = s.running;
        currentTargetTps = s.target_tps;

        const pill = document.getElementById('dbPill');
        const isTarget = s.current_db === 'targetdb';
        pill.className = 'db-pill ' + (isTarget ? 'db-target' : 'db-source');
        pill.textContent = s.current_db
          ? (isTarget ? 'CLOUD B • ' : 'CLOUD A • ') + s.current_db + ' (PG ' + s.current_pg_version + ')'
          : 'Waiting for DB...';

        if (s.cutover_detected_at) {
          const b = document.getElementById('cutoverBanner');
          b.style.display = 'block';
          b.innerHTML = '<strong>ZERO-DOWNTIME CUTOVER DETECTED at ' + s.cutover_detected_at + '!</strong> App traffic transparently switched from <code>' + s.previous_db + '</code> to <code>' + s.current_db + ' (PG ' + s.current_pg_version + ')</code> at <code>tx_id #' + s.cutover_tx_boundary + '</code> with <strong>' + s.errors + ' failed requests</strong> (max pause: ' + s.max_latency_ms + ' ms).';
        }

        document.getElementById('kTps').textContent = s.running ? (s.actual_tps + ' TPS') : 'PAUSED';
        document.getElementById('kTargetTps').textContent = 'Target: ' + s.target_tps + ' TPS';
        document.getElementById('kSeq').textContent = '#' + s.last_tx_id;
        document.getElementById('kWrites').textContent = (s.inserts + s.updates + s.deletes).toLocaleString();
        document.getElementById('kOps').textContent = s.inserts + ' ins / ' + s.updates + ' upd / ' + s.deletes + ' del';
        const errEl = document.getElementById('kErr');
        errEl.textContent = s.errors;
        errEl.style.color = s.errors > 0 ? 'var(--red)' : 'var(--green)';
        document.getElementById('kLastErr').textContent = s.last_error || 'Zero dropped writes';
        document.getElementById('kLat').textContent = s.max_latency_ms + ' ms';
        document.getElementById('kAvgLat').textContent = 'Avg: ' + s.avg_latency_ms + ' ms';
        document.getElementById('btnToggle').textContent = s.running ? 'Pause Traffic' : 'Start Traffic';

        const tbody = document.getElementById('txBody');
        tbody.innerHTML = data.recent.map(r => {
          const tgt = r.db === 'targetdb';
          const opTag = r.op === 'MANUAL' ? '<span class="tag tag-man">MANUAL</span>' : '<span class="mono">AUTO</span>';
          const dbTag = '<span class="tag ' + (tgt ? 'tag-tgt' : 'tag-src') + '">' + r.db + ' (PG ' + r.pg_ver + ')</span>';
          return '<tr><td class="mono">' + r.time + '</td><td class="mono"><strong>#' + r.tx_id + '</strong></td><td>' + opTag + '</td><td class="mono">acct-' + r.account_id + '</td><td class="mono">$' + r.amount.toFixed(2) + '</td><td class="mono">' + r.payload + '</td><td>' + dbTag + '</td><td class="mono">' + r.latency_ms + ' ms</td></tr>';
        }).join('');
      } catch (e) {}
    }

    async function toggleRun() {
      await fetch('/toggle', { method: 'POST', body: JSON.stringify({ running: !currentRunning, target_tps: currentTargetTps }) });
      poll();
    }
    async function setTps(tps) {
      await fetch('/toggle', { method: 'POST', body: JSON.stringify({ running: true, target_tps: tps }) });
      poll();
    }
    async function resetStats() {
      await fetch('/reset-stats', { method: 'POST', body: '{}' });
      poll();
    }
    async function manualInsert() {
      const note = document.getElementById('customNote').value || 'manual-test';
      const res = await fetch('/api/manual-insert', { method: 'POST', body: JSON.stringify({ payload: note }) });
      const data = await res.json();
      document.getElementById('integrityOut').textContent = data.error
        ? 'Manual insert error: ' + data.error
        : 'Inserted tx_id #' + data.tx_id + ' into ' + data.db + ' (PG ' + data.pg_ver + ') in ' + data.latency_ms + ' ms';
      poll();
    }
    async function checkIntegrity() {
      const res = await fetch('/api/integrity');
      const d = await res.json();
      const el = document.getElementById('integrityOut');
      if (d.error) { el.textContent = 'Check error: ' + d.error; return; }
      el.style.color = d.missing === 0 ? '#34d399' : '#f87171';
      el.textContent = 'Integrity Check on ' + d.db + ': verified last ' + d.checked + ' acknowledged tx_ids -> ' + d.present + '/' + d.checked + ' present (' + d.missing + ' missing).';
    }

    poll();
    setInterval(poll, 1000);
  </script>
</body>
</html>
"""


def render_prometheus_metrics() -> str:
    with state_lock:
        snap = dict(state)
    active_db = snap["current_db"]
    is_source = 1 if active_db == "sourcedb" else 0
    is_target = 1 if active_db == "targetdb" else 0
    cutover_flag = 1 if snap["cutover_detected_at"] else 0
    running_flag = 1 if snap["running"] else 0
    lines = [
        "# HELP cloudpay_workload_running Whether the CloudPay workload generator is currently running (1=running, 0=paused).",
        "# TYPE cloudpay_workload_running gauge",
        f"cloudpay_workload_running {running_flag}",
        "# HELP cloudpay_workload_target_tps Configured target transactions per second.",
        "# TYPE cloudpay_workload_target_tps gauge",
        f"cloudpay_workload_target_tps {snap['target_tps']}",
        "# HELP cloudpay_workload_actual_tps Measured transactions per second over the last 2s window.",
        "# TYPE cloudpay_workload_actual_tps gauge",
        f"cloudpay_workload_actual_tps {snap['actual_tps']}",
        "# HELP cloudpay_transactions_total Total database operations executed by the CloudPay demo app.",
        "# TYPE cloudpay_transactions_total counter",
        f'cloudpay_transactions_total{{op="insert"}} {snap["inserts"]}',
        f'cloudpay_transactions_total{{op="update"}} {snap["updates"]}',
        f'cloudpay_transactions_total{{op="delete"}} {snap["deletes"]}',
        f'cloudpay_transactions_total{{op="error"}} {snap["errors"]}',
        "# HELP cloudpay_last_tx_id Most recent BIGSERIAL tx_id inserted by CloudPay.",
        "# TYPE cloudpay_last_tx_id gauge",
        f"cloudpay_last_tx_id {snap['last_tx_id']}",
        "# HELP cloudpay_query_latency_ms_avg Rolling average query latency in milliseconds.",
        "# TYPE cloudpay_query_latency_ms_avg gauge",
        f"cloudpay_query_latency_ms_avg {snap['avg_latency_ms']}",
        "# HELP cloudpay_query_latency_ms_max Maximum observed query latency spike in milliseconds.",
        "# TYPE cloudpay_query_latency_ms_max gauge",
        f"cloudpay_query_latency_ms_max {snap['max_latency_ms']}",
        "# HELP cloudpay_active_db Active PostgreSQL database serving CloudPay writes (1=active, 0=inactive).",
        "# TYPE cloudpay_active_db gauge",
        f'cloudpay_active_db{{db="sourcedb"}} {is_source}',
        f'cloudpay_active_db{{db="targetdb"}} {is_target}',
        "# HELP cloudpay_cutover_detected Whether CloudPay has observed a live database switchover (1=yes, 0=no).",
        "# TYPE cloudpay_cutover_detected gauge",
        f"cloudpay_cutover_detected {cutover_flag}",
        "",
    ]
    return "\n".join(lines)


class StatsHandler(BaseHTTPRequestHandler):
    def _send_text(self, code: int, text: str, content_type: str):
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, code: int, payload: dict):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, html: str):
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("X-Frame-Options", "DENY")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        return

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self._send_html(DEMO_HTML)
            return
        if self.path == "/metrics":
            self._send_text(200, render_prometheus_metrics(), "text/plain; version=0.0.4; charset=utf-8")
            return
        if self.path == "/stats":
            with state_lock:
                snap = dict(state)
            self._send_json(200, snap)
            return
        if self.path == "/api/feed":
            with state_lock:
                snap = dict(state)
                feed = list(recent_txs)
            self._send_json(200, {"stats": snap, "recent": feed})
            return
        if self.path == "/api/integrity":
            try:
                self._send_json(200, verify_integrity())
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return
        self._send_json(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        body = json.loads(self.rfile.read(length).decode("utf-8")) if length > 0 else {}

        if self.path == "/toggle":
            with state_lock:
                if "running" in body:
                    state["running"] = bool(body["running"])
                else:
                    state["running"] = not state["running"]
                if "target_tps" in body:
                    state["target_tps"] = max(1.0, min(100.0, float(body["target_tps"])))
                snap = dict(state)
            self._send_json(200, snap)
            return
        if self.path == "/reset-stats":
            with state_lock:
                state["inserts"] = 0
                state["updates"] = 0
                state["deletes"] = 0
                state["errors"] = 0
                state["last_error"] = ""
                state["max_latency_ms"] = 0.0
                state["avg_latency_ms"] = 0.0
                state["cutover_detected_at"] = ""
                state["cutover_tx_boundary"] = 0
                state["previous_db"] = ""
                recent_txs.clear()
                acknowledged_ids.clear()
                snap = dict(state)
            self._send_json(200, snap)
            return
        if self.path == "/api/manual-insert":
            try:
                note = str(body.get("payload", "manual-order"))[:80]
                res = execute_insert(9999, round(random.uniform(50.0, 500.0), 2), note, "MANUAL")
                self._send_json(200, res)
            except Exception as e:
                self._send_json(500, {"error": str(e)})
            return
        self._send_json(404, {"error": "not found"})


if __name__ == "__main__":
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()
    port = int(os.environ.get("STATS_PORT", "8092"))
    print(f"[demo-app] CloudPay Example App running on :{port}", flush=True)
    HTTPServer(("0.0.0.0", port), StatsHandler).serve_forever()
