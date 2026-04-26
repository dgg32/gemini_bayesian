#!/usr/bin/env python3
"""
sync_ducklake.py – Bidirectional mirror between central.duckdb and DuckLake.

Typical workflows
-----------------
  Publish local work to the cloud:
    python sync.py b2c --all          # Bayesian projects → central.duckdb
    python sync_ducklake.py push      # central.duckdb    → DuckLake

  Restore from the cloud (e.g. on a new machine):
    python sync_ducklake.py pull      # DuckLake          → central.duckdb
    python sync.py c2b --all          # central.duckdb    → Bayesian projects

Why not COPY FROM DATABASE?
---------------------------
  DuckLake does not support PRIMARY KEY or UNIQUE constraints.
  COPY FROM DATABASE copies the DDL verbatim and fails when the source has
  any such constraint.  This script instead does, per table:

    DROP TABLE IF EXISTS <tbl>
    CREATE TABLE <tbl> AS SELECT * FROM <source>.main.<tbl>

  No constraints are copied, and the full table is replaced on every run.

Usage
-----
  python sync_ducklake.py push                  # central.duckdb → DuckLake
  python sync_ducklake.py pull                  # DuckLake → central.duckdb
  python sync_ducklake.py pull --dst other.duckdb
  python sync_ducklake.py status                # row counts in DuckLake

  Optional flags:
    --src  PATH   Local DuckDB to push from  (default: central.duckdb)
    --dst  PATH   Local DuckDB to pull into  (default: central.duckdb)
    --db   NAME   MotherDuck DuckLake database name (default: ducklake_bayesian)
"""

import argparse
import sys

import duckdb

DEFAULT_SRC = "central.duckdb"
DEFAULT_DB  = "ducklake_bayesian"


def _ducklake_tables(conn: duckdb.DuckDBPyConnection, db_name: str) -> list[str]:
    return [
        r[0] for r in conn.execute(
            "SELECT table_name FROM information_schema.tables"
            " WHERE table_schema = 'main' AND table_catalog = ?",
            (db_name,),
        ).fetchall()
    ]


def push(src_path: str, conn: duckdb.DuckDBPyConnection) -> None:
    """
    Full-replace mirror: for every table in src_path, drop the DuckLake copy
    and recreate it via CREATE TABLE AS SELECT.  No constraints are copied.
    """
    conn.execute(f"ATTACH '{src_path}' AS _src (READ_ONLY)")
    try:
        tables = conn.execute(
            "SELECT table_name FROM duckdb_tables()"
            " WHERE database_name = '_src' AND schema_name = 'main'"
        ).fetchall()

        if not tables:
            print(f"  No tables found in {src_path}.")
            return

        for (tbl,) in tables:
            print(f"  {tbl} … ", end="", flush=True)
            conn.execute(f'DROP TABLE IF EXISTS "{tbl}"')
            conn.execute(f'CREATE TABLE "{tbl}" AS SELECT * FROM _src.main."{tbl}"')
            n = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
            print(f"{n} rows")
    finally:
        try:
            conn.execute("DETACH _src")
        except Exception:
            pass


def pull(dst_path: str, conn: duckdb.DuckDBPyConnection, db_name: str) -> None:
    """
    Full-replace mirror: for every table in DuckLake, drop the local copy and
    recreate it via CREATE TABLE AS SELECT.  No constraints are copied.
    After this completes, run `sync.py c2b` to propagate into Bayesian DBs.
    """
    conn.execute(f"ATTACH '{dst_path}' AS _dst")
    try:
        tables = _ducklake_tables(conn, db_name)
        if not tables:
            print("  (no tables in DuckLake)")
            return
        for tbl in tables:
            print(f"  {tbl} … ", end="", flush=True)
            conn.execute(f'DROP TABLE IF EXISTS _dst.main."{tbl}"')
            conn.execute(
                f'CREATE TABLE _dst.main."{tbl}" AS SELECT * FROM "{db_name}".main."{tbl}"'
            )
            n = conn.execute(
                f'SELECT COUNT(*) FROM _dst.main."{tbl}"'
            ).fetchone()[0]
            print(f"{n} rows")
    finally:
        try:
            conn.execute("DETACH _dst")
        except Exception:
            pass


def status(conn: duckdb.DuckDBPyConnection, db_name: str) -> None:
    tables = _ducklake_tables(conn, db_name)
    if not tables:
        print(f"  (no tables in {db_name})")
        return
    for tbl in sorted(tables):
        try:
            n = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
            print(f"  {tbl}: {n} rows")
        except Exception as exc:
            print(f"  {tbl}: ERROR ({exc})")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Mirror central.duckdb → MotherDuck/DuckLake",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--db", default=DEFAULT_DB, metavar="NAME",
        help=f"MotherDuck DuckLake database name (default: {DEFAULT_DB})",
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_push = sub.add_parser("push", help="Push local DuckDB → DuckLake (full replace)")
    p_push.add_argument(
        "--src", default=DEFAULT_SRC, metavar="PATH",
        help=f"Local DuckDB to push from (default: {DEFAULT_SRC})",
    )

    p_pull = sub.add_parser("pull", help="Pull DuckLake → local DuckDB (full replace)")
    p_pull.add_argument(
        "--dst", default=DEFAULT_SRC, metavar="PATH",
        help=f"Local DuckDB to pull into (default: {DEFAULT_SRC})",
    )

    sub.add_parser("status", help="Show row counts in DuckLake")

    args = parser.parse_args()

    conn = duckdb.connect("md:")
    conn.execute("install ducklake;")
    conn.execute("load ducklake;")
    conn.sql(f"USE {args.db};")

    try:
        if args.cmd == "push":
            print(f"Pushing {args.src} → DuckLake ({args.db}) …")
            push(args.src, conn)
            print("Done.")
        elif args.cmd == "pull":
            print(f"Pulling DuckLake ({args.db}) → {args.dst} …")
            pull(args.dst, conn, args.db)
            print("Done.  Run: python sync.py c2b --all")
        else:
            print(f"\n=== DuckLake ({args.db}) ===")
            status(conn, args.db)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
