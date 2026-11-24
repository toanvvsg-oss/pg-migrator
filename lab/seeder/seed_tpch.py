import io
import os
import sys
import tempfile
import time
import duckdb
import psycopg

PG_DSN = os.environ["SOURCE_DSN"]
SCALE_FACTOR = float(os.environ.get("TPCH_SCALE_FACTOR", "10"))
FORCE_RESEED = os.environ.get("FORCE_RESEED", "false").lower() == "true"

SCHEMA_DDL = """
DROP TABLE IF EXISTS public.lineitem CASCADE;
DROP TABLE IF EXISTS public.orders CASCADE;
DROP TABLE IF EXISTS public.partsupp CASCADE;
DROP TABLE IF EXISTS public.customer CASCADE;
DROP TABLE IF EXISTS public.supplier CASCADE;
DROP TABLE IF EXISTS public.part CASCADE;
DROP TABLE IF EXISTS public.nation CASCADE;
DROP TABLE IF EXISTS public.region CASCADE;

CREATE TABLE public.region (
    r_regionkey INTEGER NOT NULL,
    r_name      CHAR(25) NOT NULL,
    r_comment   VARCHAR(152)
);

CREATE TABLE public.nation (
    n_nationkey INTEGER NOT NULL,
    n_name      CHAR(25) NOT NULL,
    n_regionkey INTEGER NOT NULL,
    n_comment   VARCHAR(152)
);

CREATE TABLE public.part (
    p_partkey     INTEGER NOT NULL,
    p_name        VARCHAR(55) NOT NULL,
    p_mfgr        CHAR(25) NOT NULL,
    p_brand       CHAR(10) NOT NULL,
    p_type        VARCHAR(25) NOT NULL,
    p_size        INTEGER NOT NULL,
    p_container   CHAR(10) NOT NULL,
    p_retailprice DECIMAL(15,2) NOT NULL,
    p_comment     VARCHAR(23) NOT NULL
);

CREATE TABLE public.supplier (
    s_suppkey   INTEGER NOT NULL,
    s_name      CHAR(25) NOT NULL,
    s_address   VARCHAR(40) NOT NULL,
    s_nationkey INTEGER NOT NULL,
    s_phone     CHAR(15) NOT NULL,
    s_acctbal   DECIMAL(15,2) NOT NULL,
    s_comment   VARCHAR(101) NOT NULL
);

CREATE TABLE public.partsupp (
    ps_partkey    INTEGER NOT NULL,
    ps_suppkey    INTEGER NOT NULL,
    ps_availqty   INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment    VARCHAR(199) NOT NULL
);

CREATE TABLE public.customer (
    c_custkey    INTEGER NOT NULL,
    c_name       VARCHAR(25) NOT NULL,
    c_address    VARCHAR(40) NOT NULL,
    c_nationkey  INTEGER NOT NULL,
    c_phone      CHAR(15) NOT NULL,
    c_acctbal    DECIMAL(15,2) NOT NULL,
    c_mktsegment CHAR(10) NOT NULL,
    c_comment    VARCHAR(117) NOT NULL
);

CREATE TABLE public.orders (
    o_orderkey      BIGINT NOT NULL,
    o_custkey       INTEGER NOT NULL,
    o_orderstatus   CHAR(1) NOT NULL,
    o_totalprice    DECIMAL(15,2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority CHAR(15) NOT NULL,
    o_clerk         CHAR(15) NOT NULL,
    o_shippriority  INTEGER NOT NULL,
    o_comment       VARCHAR(79) NOT NULL
);

CREATE TABLE public.lineitem (
    l_orderkey      BIGINT NOT NULL,
    l_partkey       INTEGER NOT NULL,
    l_suppkey       INTEGER NOT NULL,
    l_linenumber    INTEGER NOT NULL,
    l_quantity      DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount      DECIMAL(15,2) NOT NULL,
    l_tax           DECIMAL(15,2) NOT NULL,
    l_returnflag    CHAR(1) NOT NULL,
    l_linestatus    CHAR(1) NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  CHAR(25) NOT NULL,
    l_shipmode      CHAR(10) NOT NULL,
    l_comment       VARCHAR(44) NOT NULL
);
"""

CONSTRAINTS_DDL = """
ALTER TABLE public.region   ADD CONSTRAINT pk_region   PRIMARY KEY (r_regionkey);
ALTER TABLE public.nation   ADD CONSTRAINT pk_nation   PRIMARY KEY (n_nationkey);
ALTER TABLE public.part     ADD CONSTRAINT pk_part     PRIMARY KEY (p_partkey);
ALTER TABLE public.supplier ADD CONSTRAINT pk_supplier PRIMARY KEY (s_suppkey);
ALTER TABLE public.partsupp ADD CONSTRAINT pk_partsupp PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE public.customer ADD CONSTRAINT pk_customer PRIMARY KEY (c_custkey);
ALTER TABLE public.orders   ADD CONSTRAINT pk_orders   PRIMARY KEY (o_orderkey);
ALTER TABLE public.lineitem ADD CONSTRAINT pk_lineitem PRIMARY KEY (l_orderkey, l_linenumber);

ALTER TABLE public.nation   ADD CONSTRAINT fk_nation_region     FOREIGN KEY (n_regionkey) REFERENCES public.region(r_regionkey);
ALTER TABLE public.supplier ADD CONSTRAINT fk_supplier_nation   FOREIGN KEY (s_nationkey) REFERENCES public.nation(n_nationkey);
ALTER TABLE public.partsupp ADD CONSTRAINT fk_partsupp_part     FOREIGN KEY (ps_partkey)  REFERENCES public.part(p_partkey);
ALTER TABLE public.partsupp ADD CONSTRAINT fk_partsupp_supplier FOREIGN KEY (ps_suppkey)  REFERENCES public.supplier(s_suppkey);
ALTER TABLE public.customer ADD CONSTRAINT fk_customer_nation   FOREIGN KEY (c_nationkey) REFERENCES public.nation(n_nationkey);
ALTER TABLE public.orders   ADD CONSTRAINT fk_orders_customer   FOREIGN KEY (o_custkey)   REFERENCES public.customer(c_custkey);
ALTER TABLE public.lineitem ADD CONSTRAINT fk_lineitem_orders   FOREIGN KEY (l_orderkey)  REFERENCES public.orders(o_orderkey);
ALTER TABLE public.lineitem ADD CONSTRAINT fk_lineitem_partsupp FOREIGN KEY (l_partkey, l_suppkey) REFERENCES public.partsupp(ps_partkey, ps_suppkey);

CREATE INDEX idx_orders_custkey   ON public.orders(o_custkey);
CREATE INDEX idx_orders_orderdate ON public.orders(o_orderdate);
CREATE INDEX idx_lineitem_part    ON public.lineitem(l_partkey);
CREATE INDEX idx_lineitem_ship    ON public.lineitem(l_shipdate);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO migrator;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO appuser;
"""

TABLES = [
    "region",
    "nation",
    "part",
    "supplier",
    "partsupp",
    "customer",
    "orders",
    "lineitem",
]


def wait_for_pg():
    for attempt in range(60):
        try:
            with psycopg.connect(PG_DSN) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return
        except Exception as e:
            print(f"[seeder] Waiting for pg-source ({attempt + 1}/60): {e}", flush=True)
            time.sleep(2)
    sys.exit("Timed out waiting for pg-source")


def already_seeded(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'lineitem'
            )
            """
        )
        if not cur.fetchone()[0]:
            return False
        cur.execute("SELECT count(*) FROM public.orders")
        rows = cur.fetchone()[0]
        return rows > 0


def main():
    t0 = time.time()
    wait_for_pg()

    with psycopg.connect(PG_DSN, autocommit=True) as conn:
        if not FORCE_RESEED and already_seeded(conn):
            with conn.cursor() as cur:
                cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                size = cur.fetchone()[0]
            print(f"[seeder] TPC-H already seeded (DB size: {size}). Set FORCE_RESEED=true to regenerate.", flush=True)
            return

    print(f"[seeder] Generating TPC-H dataset with DuckDB (SCALE_FACTOR={SCALE_FACTOR})...", flush=True)
    with tempfile.TemporaryDirectory(prefix="tpch_") as tmpdir:
        db_path = os.path.join(tmpdir, "tpch.duckdb")
        duck = duckdb.connect(db_path)
        duck.sql("SET memory_limit='2GB';")
        duck.sql("SET threads=4;")
        duck.sql("LOAD tpch;")

        gen_start = time.time()
        duck.sql(f"CALL dbgen(sf={SCALE_FACTOR});")
        print(f"[seeder] DuckDB generated TPC-H SF={SCALE_FACTOR} in {time.time() - gen_start:.1f}s", flush=True)

        with psycopg.connect(PG_DSN) as pg_conn:
            with pg_conn.cursor() as cur:
                print("[seeder] Creating TPC-H base tables...", flush=True)
                cur.execute(SCHEMA_DDL)
            pg_conn.commit()

            for idx, tbl in enumerate(TABLES, 1):
                t_tbl = time.time()
                row_count = duck.sql(f"SELECT count(*) FROM {tbl}").fetchone()[0]
                csv_path = os.path.join(tmpdir, f"{tbl}.csv")
                print(f"[seeder] [{idx}/{len(TABLES)}] Exporting & COPYing {tbl} ({row_count:,} rows)...", flush=True)

                duck.sql(f"COPY {tbl} TO '{csv_path}' (FORMAT CSV, HEADER FALSE, DELIMITER ',');")
                with pg_conn.cursor() as cur:
                    with cur.copy(f"COPY public.{tbl} FROM STDIN WITH (FORMAT CSV)") as copy:
                        with open(csv_path, "rb") as f:
                            while True:
                                chunk = f.read(4 * 1024 * 1024)
                                if not chunk:
                                    break
                                copy.write(chunk)
                pg_conn.commit()
                os.remove(csv_path)
                print(f"[seeder] [{idx}/{len(TABLES)}] Loaded {tbl} in {time.time() - t_tbl:.1f}s", flush=True)

            print("[seeder] Building Primary Keys, Foreign Keys, and Indexes (this takes a moment for SF=10)...", flush=True)
            t_idx = time.time()
            with pg_conn.cursor() as cur:
                cur.execute(CONSTRAINTS_DDL)
            pg_conn.commit()
            print(f"[seeder] Constraints & Indexes built in {time.time() - t_idx:.1f}s", flush=True)

            with pg_conn.cursor() as cur:
                cur.execute("ANALYZE;")
                cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
                size = cur.fetchone()[0]
            pg_conn.commit()

        duck.close()

    print(f"[seeder] DONE! Total time: {time.time() - t0:.1f}s | Source DB Size: {size}", flush=True)


if __name__ == "__main__":
    main()
