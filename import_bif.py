#!/usr/bin/env python3
"""
Import a BIF file into a DuckDB project.

Usage:
    python import_bif.py <bif_file> [project_name] [--overwrite]
    python import_bif.py <bif_file> --db <path_to.duckdb> [--overwrite]

Examples:
    python import_bif.py bif/hepar2.bif hepar2
    python import_bif.py bif/hepar2.bif hepar2 --overwrite
    python import_bif.py bif/hepar2.bif --db custom.duckdb
"""

import argparse
import re
import sys
from pathlib import Path

import duckdb
from pgmpy.readwrite import BIFReader

PROJECTS_DIR = Path("projects")
DEFAULT_PROJECT = "default"


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    # Migration: rename category column/table → label
    try:
        conn.execute("ALTER TABLE node RENAME COLUMN category TO label")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE category RENAME TO label")
    except Exception:
        pass

    conn.execute("CREATE SEQUENCE IF NOT EXISTS node_id_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS node (
            id         INTEGER DEFAULT nextval('node_id_seq') PRIMARY KEY,
            name       VARCHAR UNIQUE NOT NULL,
            states     VARCHAR[],
            label      VARCHAR DEFAULT '',
            properties VARCHAR DEFAULT '{}'
        )
    """)
    conn.execute("ALTER TABLE node ADD COLUMN IF NOT EXISTS label VARCHAR DEFAULT ''")
    conn.execute("ALTER TABLE node ADD COLUMN IF NOT EXISTS properties VARCHAR DEFAULT '{}'")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relation (
            source   INTEGER NOT NULL,
            target   INTEGER NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            label    VARCHAR DEFAULT '',
            PRIMARY KEY (source, target)
        )
    """)
    conn.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS label VARCHAR DEFAULT ''")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cpt (
            node_id     INTEGER NOT NULL,
            col_index   INTEGER NOT NULL,
            state_index INTEGER NOT NULL,
            probability DOUBLE  NOT NULL DEFAULT 0.0,
            PRIMARY KEY (node_id, col_index, state_index)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS label (
            name  VARCHAR PRIMARY KEY,
            color VARCHAR NOT NULL DEFAULT '#ffffcc'
        )
    """)


def import_bif(bif_path: str, db_path: str, overwrite: bool = False) -> None:
    print(f"Reading {bif_path} ...")
    reader = BIFReader(bif_path)
    model = reader.get_model()
    cpds = model.cpds

    print(f"  Nodes: {len(model.nodes())}, Edges: {len(model.edges())}, CPDs: {len(cpds)}")

    conn = duckdb.connect(db_path)
    try:
        _init_schema(conn)

        if overwrite:
            print("  Clearing existing data ...")
            conn.execute("DELETE FROM cpt")
            conn.execute("DELETE FROM relation")
            conn.execute("DELETE FROM node")

        # ── 1. Insert / upsert nodes ─────────────────────────────────────────────
        name_to_id: dict[str, int] = {}
        for cpd in cpds:
            var = cpd.variable
            states = list(cpd.state_names[var])
            existing = conn.execute("SELECT id FROM node WHERE name = ?", (var,)).fetchone()
            if existing:
                node_id = existing[0]
                conn.execute("UPDATE node SET states = ? WHERE id = ?", (states, node_id))
            else:
                node_id = conn.execute(
                    "INSERT INTO node (name, states) VALUES (?, ?) RETURNING id",
                    (var, states),
                ).fetchone()[0]
            name_to_id[var] = node_id

        # ── 2. Insert relations and CPT values ───────────────────────────────────
        for cpd in cpds:
            node_id = name_to_id[cpd.variable]
            # cpd.variables = [child, parent0, parent1, ...]
            parents = cpd.variables[1:]

            # BIF model guarantees each edge is unique; no need to check existence
            for pos, parent in enumerate(parents):
                conn.execute(
                    "INSERT INTO relation (source, target, position) VALUES (?, ?, ?)",
                    (name_to_id[parent], node_id, pos),
                )

            # cpd.get_values() → 2D array (n_child_states, n_parent_combos)
            # Column ordering: first parent (parents[0]) varies slowest,
            # last parent varies fastest — matches our col_index convention.
            values_2d = cpd.get_values()
            n_states, n_cols = values_2d.shape

            conn.execute("DELETE FROM cpt WHERE node_id = ?", (node_id,))
            rows = [
                (node_id, col_idx, state_idx, float(values_2d[state_idx, col_idx]))
                for state_idx in range(n_states)
                for col_idx in range(n_cols)
            ]
            conn.executemany(
                "INSERT INTO cpt (node_id, col_index, state_index, probability) VALUES (?, ?, ?, ?)",
                rows,
            )
    finally:
        conn.close()
    print(f"  Done → {db_path}")


def _validate_project_name(name: str) -> None:
    if not re.match(r'^[a-zA-Z0-9_-]+$', name):
        print(f"Error: invalid project name '{name}'. Use only letters, digits, _ or -.", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Import a BIF Bayesian network into a DuckDB project file."
    )
    parser.add_argument("bif_file", help="Path to the .bif file")
    parser.add_argument(
        "project",
        nargs="?",
        default=DEFAULT_PROJECT,
        help=f"Project name (stored as projects/<name>.duckdb). Default: '{DEFAULT_PROJECT}'",
    )
    parser.add_argument(
        "--db",
        metavar="PATH",
        help="Direct path to a .duckdb file (overrides project name)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Clear all existing data in the target DB before importing",
    )
    args = parser.parse_args()

    bif_path = Path(args.bif_file)
    if not bif_path.exists():
        print(f"Error: file not found: {bif_path}", file=sys.stderr)
        sys.exit(1)

    if args.db:
        db_path = args.db
    else:
        _validate_project_name(args.project)
        PROJECTS_DIR.mkdir(exist_ok=True)
        db_path = str(PROJECTS_DIR / f"{args.project}.duckdb")

    import_bif(str(bif_path), db_path, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
