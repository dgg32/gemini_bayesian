#!/usr/bin/env python3
"""
sync.py – Bidirectional sync between Central DuckDB and Bayesian project DBs.

Central schema
--------------
• One node table per label, named after the label itself (e.g. symptom, Disease).
  Unlabeled nodes go into the table "node".
  Columns: id VARCHAR PK, name VARCHAR, label VARCHAR,
           properties VARCHAR (JSON), bayesian_network VARCHAR[]

• One edge table per label, named after the label itself (e.g. causes, treats).
  Unlabeled edges go into the table "edge".
  Columns: from_id VARCHAR, to_id VARCHAR, label VARCHAR,
           properties VARCHAR (JSON), bayesian_network VARCHAR[]

  Nodes/edges with an empty bayesian_network list are knowledge-graph-only
  (e.g. an anatomical "Lung" node that never appears in any BN).

• label table: node label → color mapping (shared across all projects).

• project_node table: per-project node states.
  Columns: project VARCHAR, node_name VARCHAR (lowercase), states VARCHAR[]

• relation table: per-project parent ordering (needed to reconstruct CPT columns).
  Columns: project VARCHAR, source VARCHAR, target VARCHAR, position INTEGER,
           label VARCHAR, properties VARCHAR (JSON)

• cpt table: per-project conditional probability values.
  Columns: project VARCHAR, node_name VARCHAR (lowercase),
           col_index INTEGER, state_index INTEGER, probability DOUBLE

  Together these four tables let a second user fully reproduce all Bayesian
  Networks from Central without needing the original project DB files.

Usage
-----
  python sync.py status                    # counts in Central + all Bayesian DBs
  python sync.py b2c <project>             # Bayesian project → Central
  python sync.py b2c --all                 # all Bayesian projects → Central
  python sync.py c2b <project>             # Central → Bayesian project
  python sync.py c2b <project> --prune     # also remove nodes/edges absent in Central
  python sync.py c2b --all                 # every BN referenced in Central → its DB
  python sync.py c2b --all --prune         # same, with deletion sync

  Optional flags:
    --central PATH   Path to Central DuckDB  (default: central.duckdb)
    --projects DIR   Directory of project DBs (default: projects/)

Deletion behaviour (--prune)
----------------------------
  Without --prune (default): c2b is additive-only.  Nodes/edges that exist in
  the Bayesian DB but are absent (or de-tagged) in Central are left untouched.

  With --prune: c2b also removes orphaned rows:
    • Orphaned node  → its CPT + all its relations are deleted; CPTs of child
                       nodes are reset to zeros with the correct new dimensions
                       (user must re-enter the probabilities).
                       Central project_node / relation / cpt rows for the
                       pruned node+project are also removed.
    • Orphaned edge  → the parent-child relation is deleted; the child's CPT is
                       reset to zeros with the correct new dimensions.
                       Central relation row for the pruned edge+project is also removed.
    • Label colours  → stored in the Bayesian `label` table, not per-node;
                       unused entries become harmless orphans (not deleted,
                       so no data loss if the label is re-used later).

Notes
-----
  b2c opens Bayesian DBs **read-only** — safe while the FastAPI server runs.
  c2b *writes* to a Bayesian DB; stop the server before running it.
"""

import argparse
import re
import sys
from pathlib import Path

import duckdb

PROJECTS_DIR   = Path("projects")
CENTRAL_DB     = Path("central.duckdb")
DEFAULT_STATES = ["Yes", "No"]
PROPERTY_GRAPH_NAME = "bayesian_kg"


# ── Naming helpers ─────────────────────────────────────────────────────────────

def _safe(label: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9_]", "_", label).strip("_")
    return s or "general"


def _label_table(label: str) -> str:
    return _safe(label) if label else "node"


def _edge_table(label: str) -> str:
    return _safe(label) if label else "edge"


# ── Central schema ─────────────────────────────────────────────────────────────

def _ensure_label_table(conn: duckdb.DuckDBPyConnection, label: str) -> str:
    tbl     = _label_table(label)
    lbl_sql = label.replace("'", "''")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{tbl}" (
            id               VARCHAR PRIMARY KEY,
            name             VARCHAR NOT NULL,
            label            VARCHAR DEFAULT '{lbl_sql}',
            properties       VARCHAR DEFAULT '{{}}',
            bayesian_network VARCHAR[] DEFAULT []
        )
    """)
    return tbl


def _ensure_edge_table(conn: duckdb.DuckDBPyConnection, label: str) -> str:
    tbl     = _edge_table(label)
    lbl_sql = label.replace("'", "''")
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS "{tbl}" (
            from_id          VARCHAR NOT NULL,
            to_id            VARCHAR NOT NULL,
            label            VARCHAR DEFAULT '{lbl_sql}',
            properties       VARCHAR DEFAULT '{{}}',
            bayesian_network VARCHAR[] DEFAULT [],
            PRIMARY KEY (from_id, to_id)
        )
    """)
    return tbl


def _node_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Tables that have a 'bayesian_network' column but NOT a 'from_id' column (node shape)."""
    rows = conn.execute("""
        SELECT DISTINCT c1.table_name
        FROM information_schema.columns c1
        WHERE c1.table_schema = 'main'
          AND c1.column_name = 'bayesian_network'
          AND c1.table_name NOT IN (
              SELECT table_name FROM information_schema.columns
              WHERE table_schema = 'main' AND column_name = 'from_id'
          )
    """).fetchall()
    return [r[0] for r in rows]


def _edge_tables(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Tables that have a 'from_id' column (edge shape)."""
    rows = conn.execute("""
        SELECT DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND column_name = 'from_id'
    """).fetchall()
    return [r[0] for r in rows]


def _project_edge_metadata(
    conn: duckdb.DuckDBPyConnection,
    project: str,
) -> dict[tuple[str, str], tuple[str, str]]:
    metadata: dict[tuple[str, str], tuple[str, str]] = {}
    for etbl in _edge_tables(conn):
        try:
            rows = conn.execute(
                f'SELECT from_id, to_id, label, properties FROM "{etbl}" '
                'WHERE list_contains(bayesian_network, ?)',
                (project,),
            ).fetchall()
        except Exception:
            try:
                raw_rows = conn.execute(
                    f'SELECT from_id, to_id, label FROM "{etbl}" '
                    'WHERE list_contains(bayesian_network, ?)',
                    (project,),
                ).fetchall()
            except Exception:
                continue
            rows = [(source, target, label, "{}") for source, target, label in raw_rows]

        for source, target, label, properties in rows:
            key = (source, target)
            lbl_key = label or ""
            props_json = properties or "{}"
            prev = metadata.get(key)
            if not prev:
                metadata[key] = (lbl_key, props_json)
                continue

            prev_label, prev_props = prev
            metadata[key] = (
                prev_label or lbl_key,
                prev_props if prev_props not in {"", "{}"} else props_json,
            )

    return metadata


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _edge_endpoint_table(
    conn: duckdb.DuckDBPyConnection,
    edge_table: str,
    endpoint_col: str,
    node_tables: list[str],
) -> str | None:
    counts: list[tuple[int, str]] = []
    for node_table in node_tables:
        try:
            match_count = conn.execute(
                f'''
                SELECT COUNT(*)
                FROM {_quote_ident(edge_table)} e
                JOIN {_quote_ident(node_table)} n
                  ON e.{endpoint_col} = n.id
                '''
            ).fetchone()[0]
        except Exception:
            match_count = 0
        if match_count > 0:
            counts.append((match_count, node_table))

    if not counts:
        return None

    counts.sort(key=lambda item: (-item[0], item[1]))
    return counts[0][1]


def _refresh_property_graph(conn: duckdb.DuckDBPyConnection) -> None:
    try:
        conn.execute("INSTALL duckpgq FROM community")
        conn.execute("LOAD duckpgq")
    except Exception as exc:
        print(
            f"Warning: could not load DuckPGQ; property graph '{PROPERTY_GRAPH_NAME}' was not refreshed: {exc}",
            file=sys.stderr,
        )
        return

    node_tables = sorted(_node_tables(conn))
    edge_tables = sorted(_edge_tables(conn))

    if not node_tables or not edge_tables:
        print(
            f"Warning: skipped property graph refresh for '{PROPERTY_GRAPH_NAME}' because "
            f"the central schema does not yet contain both vertex and edge tables.",
            file=sys.stderr,
        )
        return

    vertex_sql = ",\n    ".join(_quote_ident(tbl) for tbl in node_tables)
    edge_defs: list[str] = []
    skipped_edges: list[str] = []
    for edge_table in edge_tables:
        src_table = _edge_endpoint_table(conn, edge_table, "from_id", node_tables)
        dst_table = _edge_endpoint_table(conn, edge_table, "to_id", node_tables)
        if not src_table or not dst_table:
            skipped_edges.append(edge_table)
            continue
        edge_defs.append(
            f"{_quote_ident(edge_table)} SOURCE KEY ('from_id') REFERENCES {_quote_ident(src_table)} (id) "
            f"DESTINATION KEY ('to_id') REFERENCES {_quote_ident(dst_table)} (id) LABEL {_quote_ident(edge_table)}"
        )

    if not edge_defs:
        print(
            f"Warning: skipped property graph refresh for '{PROPERTY_GRAPH_NAME}' because "
            f"no edge tables could be mapped to vertex tables.",
            file=sys.stderr,
        )
        return

    if skipped_edges:
        print(
            f"Warning: skipped edge tables during property graph refresh for '{PROPERTY_GRAPH_NAME}': "
            f"{', '.join(sorted(skipped_edges))}",
            file=sys.stderr,
        )

    edge_sql = ",\n  ".join(edge_defs)

    try:
        conn.execute(f"DROP PROPERTY GRAPH IF EXISTS {_quote_ident(PROPERTY_GRAPH_NAME)}")
        conn.execute(
            f"""
            CREATE PROPERTY GRAPH {_quote_ident(PROPERTY_GRAPH_NAME)}
              VERTEX TABLES (
                {vertex_sql}
              )
            EDGE TABLES (
              {edge_sql}
            )
            """
        )
    except Exception as exc:
        print(
            f"Warning: failed to refresh property graph '{PROPERTY_GRAPH_NAME}': {exc}",
            file=sys.stderr,
        )


def _ensure_central_base(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS node (
            id               VARCHAR PRIMARY KEY,
            name             VARCHAR NOT NULL,
            label            VARCHAR DEFAULT '',
            properties       VARCHAR DEFAULT '{}',
            bayesian_network VARCHAR[] DEFAULT []
        )
    """)
    _ensure_edge_table(conn, "")   # create base unlabeled edge table
    # Migrations: rename old column names if they still exist
    for old, new in [("source_id", "from_id"), ("target_id", "to_id"), ("edge_type", "label")]:
        try:
            conn.execute(f"ALTER TABLE edge RENAME COLUMN {old} TO {new}")
        except Exception:
            pass
    # Bayesian-specific tables (shared across projects)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS label (
            name  VARCHAR PRIMARY KEY,
            color VARCHAR NOT NULL DEFAULT '#ffffcc'
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS project_node (
            project   VARCHAR NOT NULL,
            node_name VARCHAR NOT NULL,
            states    VARCHAR[] NOT NULL DEFAULT [],
            PRIMARY KEY (project, node_name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS relation (
            project  VARCHAR NOT NULL,
            source   VARCHAR NOT NULL,
            target   VARCHAR NOT NULL,
            position INTEGER NOT NULL DEFAULT 0,
            label    VARCHAR DEFAULT '',
            properties VARCHAR DEFAULT '{}',
            PRIMARY KEY (project, source, target)
        )
    """)
    conn.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS label VARCHAR DEFAULT ''")
    conn.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS properties VARCHAR DEFAULT '{}' ")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cpt (
            project     VARCHAR NOT NULL,
            node_name   VARCHAR NOT NULL,
            col_index   INTEGER NOT NULL,
            state_index INTEGER NOT NULL,
            probability DOUBLE NOT NULL DEFAULT 0.0,
            PRIMARY KEY (project, node_name, col_index, state_index)
        )
    """)
    # Migration: move any labeled rows out of base edge table into per-label tables
    try:
        labeled = conn.execute(
            "SELECT from_id, to_id, label, properties, bayesian_network"
            " FROM edge WHERE label != ''"
        ).fetchall()
        for from_id, to_id, lbl, props, bn in labeled:
            tbl = _ensure_edge_table(conn, lbl)
            try:
                conn.execute(
                    f'INSERT INTO "{tbl}" (from_id, to_id, label, properties, bayesian_network)'
                    " VALUES (?, ?, ?, ?, ?)",
                    (from_id, to_id, lbl, props, bn),
                )
            except Exception:
                pass
        if labeled:
            conn.execute("DELETE FROM edge WHERE label != ''")
    except Exception:
        pass


# ── Bayesian DB helpers ────────────────────────────────────────────────────────

def _open_bayesian(project: str, projects_dir: Path,
                   read_only: bool = False) -> duckdb.DuckDBPyConnection:
    projects_dir.mkdir(exist_ok=True)
    path = str(projects_dir / f"{project}.duckdb")
    conn = duckdb.connect(path, read_only=read_only)
    if not read_only:
        conn.execute("DROP TABLE IF EXISTS nodes")
        for stmt in (
            "ALTER TABLE node RENAME COLUMN category TO label",
            "ALTER TABLE category RENAME TO label",
        ):
            try:
                conn.execute(stmt)
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
        conn.execute("ALTER TABLE node ADD COLUMN IF NOT EXISTS label      VARCHAR DEFAULT ''")
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
        conn.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS label      VARCHAR DEFAULT ''")
        conn.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS properties VARCHAR DEFAULT '{}'")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS cpt (
                node_id     INTEGER NOT NULL,
                col_index   INTEGER NOT NULL,
                state_index INTEGER NOT NULL,
                probability DOUBLE NOT NULL DEFAULT 0.0,
                PRIMARY KEY (node_id, col_index, state_index)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS label (
                name  VARCHAR PRIMARY KEY,
                color VARCHAR NOT NULL DEFAULT '#ffffcc'
            )
        """)
    return conn


# ── CPT reset helper ───────────────────────────────────────────────────────────

def _reset_child_cpt(b: duckdb.DuckDBPyConnection, child_int_id: int) -> None:
    """
    After a parent was removed, recompute CPT dimensions for a child node and
    fill them with zeros.  The user must re-enter the probabilities in the UI.
    """
    child_row = b.execute(
        "SELECT states FROM node WHERE id = ?", (child_int_id,)
    ).fetchone()
    if not child_row:
        return
    states = list(child_row[0])

    parent_ids = [
        r[0] for r in b.execute(
            "SELECT source FROM relation WHERE target = ? ORDER BY position",
            (child_int_id,),
        ).fetchall()
    ]
    n_cols = 1
    for pid in parent_ids:
        p_row = b.execute("SELECT states FROM node WHERE id = ?", (pid,)).fetchone()
        if p_row:
            n_cols *= len(list(p_row[0]))

    b.execute("DELETE FROM cpt WHERE node_id = ?", (child_int_id,))
    rows = [
        (child_int_id, c, r, 0.0)
        for r in range(len(states))
        for c in range(n_cols)
    ]
    if rows:
        b.executemany(
            "INSERT INTO cpt (node_id, col_index, state_index, probability)"
            " VALUES (?, ?, ?, ?)",
            rows,
        )


# ── Sync: Bayesian → Central ───────────────────────────────────────────────────

def sync_b2c(project: str, central: duckdb.DuckDBPyConnection,
             projects_dir: Path) -> None:
    """
    Push every node, edge, CPT, and label color from a Bayesian project DB into Central.
    Safe to run while the FastAPI server is running (Bayesian DB opened read-only).
    """
    b = _open_bayesian(project, projects_dir, read_only=True)
    try:
        nodes = b.execute("SELECT name, label, properties, states FROM node").fetchall()
        try:
            edges = b.execute("""
                SELECT n1.name, n2.name, r.label, r.position, r.properties
                FROM relation r
                JOIN node n1 ON r.source = n1.id
                JOIN node n2 ON r.target = n2.id
            """).fetchall()
        except Exception:
            # Older DB without properties column — treat all edge properties as empty
            raw = b.execute("""
                SELECT n1.name, n2.name, r.label, r.position
                FROM relation r
                JOIN node n1 ON r.source = n1.id
                JOIN node n2 ON r.target = n2.id
            """).fetchall()
            edges = [(s, t, l, p, '{}') for s, t, l, p in raw]
        cpt_rows = b.execute("""
            SELECT n.name, c.col_index, c.state_index, c.probability
            FROM cpt c JOIN node n ON c.node_id = n.id
        """).fetchall()
        label_rows = b.execute("SELECT name, color FROM label").fetchall()
    finally:
        b.close()

    n_new = n_upd = e_new = e_upd = 0
    current_node_tables = {
        name.lower(): _label_table(label or "")
        for name, label, _, _ in nodes
    }
    current_edge_keys = {
        (source.lower(), target.lower(), edge_lbl or "")
        for source, target, edge_lbl, _, _ in edges
    }

    for etbl in _edge_tables(central):
        try:
            tagged_rows = central.execute(
                f'SELECT from_id, to_id, label, bayesian_network FROM "{etbl}" '
                'WHERE list_contains(bayesian_network, ?)',
                (project,),
            ).fetchall()
        except Exception:
            continue

        for from_id, to_id, label, bayesian_network in tagged_rows:
            if (from_id, to_id, label or "") in current_edge_keys:
                continue
            remaining_bn = [bn for bn in list(bayesian_network or []) if bn != project]
            if remaining_bn:
                central.execute(
                    f'UPDATE "{etbl}" SET bayesian_network = ? WHERE from_id = ? AND to_id = ?',
                    (remaining_bn, from_id, to_id),
                )
            else:
                central.execute(
                    f'DELETE FROM "{etbl}" WHERE from_id = ? AND to_id = ?',
                    (from_id, to_id),
                )

    for ntbl in _node_tables(central):
        try:
            tagged_rows = central.execute(
                f'SELECT id, bayesian_network FROM "{ntbl}" WHERE list_contains(bayesian_network, ?)',
                (project,),
            ).fetchall()
        except Exception:
            continue

        for node_id, bayesian_network in tagged_rows:
            if current_node_tables.get(node_id) == ntbl:
                continue
            remaining_bn = [bn for bn in list(bayesian_network or []) if bn != project]
            if remaining_bn:
                central.execute(
                    f'UPDATE "{ntbl}" SET bayesian_network = ? WHERE id = ?',
                    (remaining_bn, node_id),
                )
            else:
                central.execute(f'DELETE FROM "{ntbl}" WHERE id = ?', (node_id,))

    central.execute("DELETE FROM project_node WHERE project = ?", (project,))
    central.execute("DELETE FROM relation WHERE project = ?", (project,))
    central.execute("DELETE FROM cpt WHERE project = ?", (project,))

    # ── Node tables + project_node (states) ──────────────────────────────────
    for name, label, props_json, states_arr in nodes:
        name_key   = name.lower()
        tbl        = _ensure_label_table(central, label) if label else "node"
        props_json = props_json or "{}"
        states     = list(states_arr or [])

        existing = central.execute(
            f'SELECT bayesian_network FROM "{tbl}" WHERE id = ?', (name_key,)
        ).fetchone()
        if existing:
            bn = list(existing[0] or [])
            if project not in bn:
                bn.append(project)
            central.execute(
                f'UPDATE "{tbl}" SET properties = ?, bayesian_network = ? WHERE id = ?',
                (props_json, bn, name_key),
            )
            n_upd += 1
        else:
            central.execute(
                f'INSERT INTO "{tbl}" (id, name, label, properties, bayesian_network)'
                f' VALUES (?, ?, ?, ?, ?)',
                (name_key, name, label or "", props_json, [project]),
            )
            n_new += 1

        # Upsert per-project states
        if central.execute(
            "SELECT 1 FROM project_node WHERE project = ? AND node_name = ?",
            (project, name_key),
        ).fetchone():
            central.execute(
                "UPDATE project_node SET states = ? WHERE project = ? AND node_name = ?",
                (states, project, name_key),
            )
        else:
            central.execute(
                "INSERT INTO project_node (project, node_name, states) VALUES (?, ?, ?)",
                (project, name_key, states),
            )

    # ── Edge tables + Central relation table (position ordering) ─────────────
    for source, target, edge_lbl, position, edge_props_json in edges:
        src_key = source.lower()
        tgt_key = target.lower()
        lbl_key = edge_lbl or ""
        props_json = edge_props_json or "{}"

        # KG edge table (for visualization)
        tbl = _ensure_edge_table(central, lbl_key)
        existing = central.execute(
            f'SELECT bayesian_network FROM "{tbl}" WHERE from_id = ? AND to_id = ?',
            (src_key, tgt_key),
        ).fetchone()
        if existing:
            bn = list(existing[0] or [])
            if project not in bn:
                bn.append(project)
            central.execute(
                f'UPDATE "{tbl}" SET properties = ?, bayesian_network = ? WHERE from_id = ? AND to_id = ?',
                (props_json, bn, src_key, tgt_key),
            )
            e_upd += 1
        else:
            central.execute(
                f'INSERT INTO "{tbl}" (from_id, to_id, label, properties, bayesian_network) VALUES (?, ?, ?, ?, ?)',
                (src_key, tgt_key, lbl_key, props_json, [project]),
            )
            e_new += 1

        # Central relation table (parent ordering for CPT reconstruction)
        if central.execute(
            "SELECT 1 FROM relation WHERE project = ? AND source = ? AND target = ?",
            (project, src_key, tgt_key),
        ).fetchone():
            central.execute(
                "UPDATE relation SET label = ?, position = ?, properties = ? "
                "WHERE project = ? AND source = ? AND target = ?",
                (lbl_key, position, props_json, project, src_key, tgt_key),
            )
        else:
            central.execute(
                "INSERT INTO relation (project, source, target, position, label, properties) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (project, src_key, tgt_key, position, lbl_key, props_json),
            )

    # ── CPT ───────────────────────────────────────────────────────────────────
    for node_name, col_idx, state_idx, prob in cpt_rows:
        name_key = node_name.lower()
        if central.execute(
            "SELECT 1 FROM cpt WHERE project = ? AND node_name = ?"
            " AND col_index = ? AND state_index = ?",
            (project, name_key, col_idx, state_idx),
        ).fetchone():
            central.execute(
                "UPDATE cpt SET probability = ? WHERE project = ? AND node_name = ?"
                " AND col_index = ? AND state_index = ?",
                (prob, project, name_key, col_idx, state_idx),
            )
        else:
            central.execute(
                "INSERT INTO cpt (project, node_name, col_index, state_index, probability)"
                " VALUES (?, ?, ?, ?, ?)",
                (project, name_key, col_idx, state_idx, prob),
            )

    # ── Label colors ──────────────────────────────────────────────────────────
    for lbl_name, color in label_rows:
        if central.execute("SELECT 1 FROM label WHERE name = ?", (lbl_name,)).fetchone():
            central.execute("UPDATE label SET color = ? WHERE name = ?", (color, lbl_name))
        else:
            central.execute(
                "INSERT INTO label (name, color) VALUES (?, ?)", (lbl_name, color)
            )

    print(
        f"  [{project}] → Central: "
        f"{n_new} new nodes, {n_upd} updated nodes, "
        f"{e_new} new edges, {e_upd} updated edges"
    )


# ── Sync: Central → Bayesian ───────────────────────────────────────────────────

def sync_c2b(project: str, central: duckdb.DuckDBPyConnection,
             projects_dir: Path, prune: bool = False) -> None:
    """
    Pull all nodes/edges tagged with `project` from Central into the Bayesian DB.

    Without --prune (default): additive only — nothing is deleted from Bayesian.
    With    --prune:            also removes nodes/edges absent in Central and
                                resets affected CPTs.

    ⚠️  Requires the FastAPI server to be stopped (writes to the project DB).
    """
    b = _open_bayesian(project, projects_dir, read_only=False)
    try:
        n_new = n_upd = e_new = 0
        n_pruned_nodes = n_pruned_edges = 0

        # ── 1. Upsert nodes (states from Central project_node) ───────────────
        for tbl in _node_tables(central):
            try:
                rows = central.execute(
                    f'SELECT id, name, label, properties FROM "{tbl}"'
                    f' WHERE list_contains(bayesian_network, ?)',
                    (project,),
                ).fetchall()
            except Exception as exc:
                print(f"    Warning: could not read {tbl}: {exc}", file=sys.stderr)
                continue

            for node_id, name, label, props_json in rows:
                props_json = props_json or "{}"
                # States: prefer Central project_node; fall back to existing Bayesian states
                states_row = central.execute(
                    "SELECT states FROM project_node WHERE project = ? AND node_name = ?",
                    (project, node_id),
                ).fetchone()
                existing = b.execute(
                    "SELECT id FROM node WHERE LOWER(name) = ?", (node_id,)
                ).fetchone()
                states = list(states_row[0]) if states_row else (
                    list(b.execute("SELECT states FROM node WHERE id = ?", (existing[0],)).fetchone()[0])
                    if existing else DEFAULT_STATES
                )

                if existing:
                    b.execute(
                        "UPDATE node SET states = ?, label = ?, properties = ? WHERE id = ?",
                        (states, label or "", props_json, existing[0]),
                    )
                    n_upd += 1
                else:
                    b.execute(
                        "INSERT INTO node (name, states, label, properties)"
                        " VALUES (?, ?, ?, ?)",
                        (name, states, label or "", props_json),
                    )
                    n_new += 1

        # ── 2. Upsert relations from Central relation table (correct position) ─
        edge_metadata = _project_edge_metadata(central, project)

        try:
            central_relation_rows = central.execute(
                "SELECT source, target, position, label, properties FROM relation WHERE project = ?",
                (project,),
            ).fetchall()
        except Exception:
            raw_relation_rows = central.execute(
                "SELECT source, target, position, label FROM relation WHERE project = ?",
                (project,),
            ).fetchall()
            central_relation_rows = [
                (source, target, position, edge_lbl, "{}")
                for source, target, position, edge_lbl in raw_relation_rows
            ]
        central_edges: set[tuple[str, str]] = set()

        for source, target, position, edge_lbl, edge_props_json in central_relation_rows:
            central_edges.add((source, target))
            s = b.execute("SELECT id FROM node WHERE LOWER(name) = ?", (source,)).fetchone()
            t = b.execute("SELECT id FROM node WHERE LOWER(name) = ?", (target,)).fetchone()
            if not s or not t:
                continue
            fallback_label, fallback_props = edge_metadata.get((source, target), ("", "{}"))
            lbl_key = edge_lbl or fallback_label or ""
            props_json = edge_props_json if edge_props_json not in (None, "", "{}") else fallback_props
            already = b.execute(
                "SELECT 1 FROM relation WHERE source = ? AND target = ?",
                (s[0], t[0]),
            ).fetchone()
            if not already:
                b.execute(
                    "INSERT INTO relation (source, target, position, label, properties) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (s[0], t[0], position, lbl_key, props_json),
                )
                e_new += 1
            else:
                b.execute(
                    "UPDATE relation SET position = ?, label = ?, properties = ? "
                    "WHERE source = ? AND target = ?",
                    (position, lbl_key, props_json, s[0], t[0]),
                )

        # Also catch any edge-table edges not yet in Central relation table
        for (source, target), (lbl_key, props_json) in edge_metadata.items():
            if (source, target) in central_edges:
                continue   # already handled above
            central_edges.add((source, target))
            s = b.execute("SELECT id FROM node WHERE LOWER(name) = ?", (source,)).fetchone()
            t = b.execute("SELECT id FROM node WHERE LOWER(name) = ?", (target,)).fetchone()
            if not s or not t:
                continue
            already = b.execute(
                "SELECT 1 FROM relation WHERE source = ? AND target = ?",
                (s[0], t[0]),
            ).fetchone()
            if not already:
                pos = b.execute(
                    "SELECT COUNT(*) FROM relation WHERE target = ?", (t[0],)
                ).fetchone()[0]
                b.execute(
                    "INSERT INTO relation (source, target, position, label, properties) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (s[0], t[0], pos, lbl_key, props_json),
                )
                e_new += 1
            else:
                b.execute(
                    "UPDATE relation SET label = ?, properties = ? WHERE source = ? AND target = ?",
                    (lbl_key, props_json, s[0], t[0]),
                )

        # ── 3. Sync CPT from Central ──────────────────────────────────────────
        cpt_rows = central.execute(
            "SELECT node_name, col_index, state_index, probability FROM cpt WHERE project = ?",
            (project,),
        ).fetchall()
        for node_name, col_idx, state_idx, prob in cpt_rows:
            node_row = b.execute(
                "SELECT id FROM node WHERE LOWER(name) = ?", (node_name,)
            ).fetchone()
            if not node_row:
                continue
            node_int_id = node_row[0]
            if b.execute(
                "SELECT 1 FROM cpt WHERE node_id = ? AND col_index = ? AND state_index = ?",
                (node_int_id, col_idx, state_idx),
            ).fetchone():
                b.execute(
                    "UPDATE cpt SET probability = ? WHERE node_id = ? AND col_index = ? AND state_index = ?",
                    (prob, node_int_id, col_idx, state_idx),
                )
            else:
                b.execute(
                    "INSERT INTO cpt (node_id, col_index, state_index, probability) VALUES (?, ?, ?, ?)",
                    (node_int_id, col_idx, state_idx, prob),
                )

        # ── 4. Sync label colors from Central ─────────────────────────────────
        for lbl_name, color in central.execute("SELECT name, color FROM label").fetchall():
            if b.execute("SELECT 1 FROM label WHERE name = ?", (lbl_name,)).fetchone():
                b.execute("UPDATE label SET color = ? WHERE name = ?", (color, lbl_name))
            else:
                b.execute("INSERT INTO label (name, color) VALUES (?, ?)", (lbl_name, color))

        # ── 5. Prune orphaned nodes ───────────────────────────────────────────
        if prune:
            # Collect every node name that Central tags with this project
            central_names: set[str] = set()
            for tbl in _node_tables(central):
                try:
                    for (nid,) in central.execute(
                        f'SELECT id FROM "{tbl}" WHERE list_contains(bayesian_network, ?)',
                        (project,),
                    ).fetchall():
                        central_names.add(nid)
                except Exception:
                    pass

            bayesian_nodes = b.execute("SELECT id, name FROM node").fetchall()
            for int_id, name in bayesian_nodes:
                if name.lower() in central_names:   # central_names contains lowercase ids
                    continue  # still tagged in Central → keep

                # Find children before deleting, so we can reset their CPTs
                child_ids = [
                    r[0] for r in b.execute(
                        "SELECT target FROM relation WHERE source = ?", (int_id,)
                    ).fetchall()
                ]
                b.execute("DELETE FROM cpt      WHERE node_id = ?",                (int_id,))
                b.execute("DELETE FROM relation WHERE source = ? OR target = ?", (int_id, int_id))
                b.execute("DELETE FROM node     WHERE id = ?",                   (int_id,))

                for child_id in child_ids:
                    _reset_child_cpt(b, child_id)

                # Also clean up Central auxiliary tables for this node+project
                name_key = name.lower()
                central.execute(
                    "DELETE FROM project_node WHERE project = ? AND node_name = ?",
                    (project, name_key),
                )
                central.execute(
                    "DELETE FROM cpt WHERE project = ? AND node_name = ?",
                    (project, name_key),
                )
                central.execute(
                    "DELETE FROM relation WHERE project = ? AND (source = ? OR target = ?)",
                    (project, name_key, name_key),
                )

                n_pruned_nodes += 1

        # ── 6. Prune orphaned edges ───────────────────────────────────────────
        if prune:
            bayesian_edges = b.execute("""
                SELECT n1.name, n2.name, r.source, r.target
                FROM relation r
                JOIN node n1 ON r.source = n1.id
                JOIN node n2 ON r.target = n2.id
            """).fetchall()

            for src_name, tgt_name, src_id, tgt_id in bayesian_edges:
                if (src_name.lower(), tgt_name.lower()) in central_edges:
                    continue  # still tagged in Central → keep


                b.execute(
                    "DELETE FROM relation WHERE source = ? AND target = ?",
                    (src_id, tgt_id),
                )
                _reset_child_cpt(b, tgt_id)

                # Also remove this relation from Central for this project
                central.execute(
                    "DELETE FROM relation WHERE project = ? AND source = ? AND target = ?",
                    (project, src_name.lower(), tgt_name.lower()),
                )

                n_pruned_edges += 1

        # ── Report ────────────────────────────────────────────────────────────
        msg = (
            f"  Central → [{project}]: "
            f"{n_new} new nodes, {n_upd} updated, {e_new} new edges"
        )
        if prune:
            msg += (
                f" | pruned {n_pruned_nodes} nodes, {n_pruned_edges} edges"
                f" (affected CPTs reset to zero)"
            )
        print(msg)

    finally:
        b.close()


# ── Status report ──────────────────────────────────────────────────────────────

def status(central: duckdb.DuckDBPyConnection, projects_dir: Path) -> None:
    print(f"\n=== Central DB ({CENTRAL_DB}) ===")
    for tbl in sorted(_node_tables(central)):
        n = central.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
        bn_counts: dict[str, int] = {}
        try:
            for (name,) in central.execute(
                f'SELECT DISTINCT unnest(bayesian_network) FROM "{tbl}"'
            ).fetchall():
                if name:
                    bn_counts[name] = bn_counts.get(name, 0) + 1
        except Exception:
            pass
        bn_str = ", ".join(f"{k}:{v}" for k, v in sorted(bn_counts.items()))
        print(f"  {tbl}: {n} nodes" + (f"  [{bn_str}]" if bn_str else ""))

    for etbl in sorted(_edge_tables(central)):
        e = central.execute(f'SELECT COUNT(*) FROM "{etbl}"').fetchone()[0]
        bn_counts: dict[str, int] = {}
        try:
            for (name,) in central.execute(
                f'SELECT DISTINCT unnest(bayesian_network) FROM "{etbl}"'
            ).fetchall():
                if name:
                    bn_counts[name] = bn_counts.get(name, 0) + 1
        except Exception:
            pass
        bn_str = ", ".join(f"{k}:{v}" for k, v in sorted(bn_counts.items()))
        print(f"  {etbl}: {e} edges" + (f"  [{bn_str}]" if bn_str else ""))

    try:
        lbl_n = central.execute("SELECT COUNT(*) FROM label").fetchone()[0]
        print(f"  label: {lbl_n} colors")
    except Exception:
        pass
    try:
        pn = central.execute("SELECT COUNT(DISTINCT project) FROM project_node").fetchone()[0]
        nn = central.execute("SELECT COUNT(*) FROM project_node").fetchone()[0]
        print(f"  project_node: {nn} rows across {pn} project(s)")
    except Exception:
        pass
    try:
        rn = central.execute("SELECT COUNT(*) FROM relation").fetchone()[0]
        print(f"  relation: {rn} rows")
    except Exception:
        pass
    try:
        cn = central.execute("SELECT COUNT(*) FROM cpt").fetchone()[0]
        print(f"  cpt: {cn} rows")
    except Exception:
        pass

    print(f"\n=== Bayesian projects ({projects_dir}/) ===")
    for path in sorted(projects_dir.glob("*.duckdb")):
        proj = path.stem
        try:
            b = _open_bayesian(proj, projects_dir, read_only=True)
            n = b.execute("SELECT COUNT(*) FROM node").fetchone()[0]
            r = b.execute("SELECT COUNT(*) FROM relation").fetchone()[0]
            b.close()
            print(f"  {proj}: {n} nodes, {r} edges")
        except Exception as exc:
            print(f"  {proj}: ERROR ({exc})")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync Central DuckDB ↔ Bayesian project DBs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--central", default=str(CENTRAL_DB), metavar="PATH",
        help=f"Central DuckDB file (default: {CENTRAL_DB})",
    )
    parser.add_argument(
        "--projects", default=str(PROJECTS_DIR), metavar="DIR",
        help=f"Bayesian project directory (default: {PROJECTS_DIR})",
    )

    sub = parser.add_subparsers(dest="cmd", required=True)

    p_b2c = sub.add_parser("b2c", help="Push Bayesian project(s) → Central")
    p_b2c.add_argument("project", nargs="?", help="Project name")
    p_b2c.add_argument("--all", action="store_true", help="All projects")

    p_c2b = sub.add_parser("c2b", help="Pull Central → Bayesian project(s)")
    p_c2b.add_argument("project", nargs="?", help="Project name")
    p_c2b.add_argument("--all", action="store_true",
                       help="All BN names referenced in Central")
    p_c2b.add_argument(
        "--prune", action="store_true",
        help=(
            "Also remove nodes/edges from the Bayesian DB that are absent "
            "(or de-tagged) in Central. Affected CPTs are reset to zero."
        ),
    )

    sub.add_parser("status", help="Print node/edge counts for all sources")

    args    = parser.parse_args()
    proj_d  = Path(args.projects)
    central = duckdb.connect(args.central)
    _ensure_central_base(central)

    try:
        if args.cmd == "status":
            status(central, proj_d)
            return

        if not args.project and not args.all:
            parser.error("Specify a project name or --all")

        if args.cmd == "b2c":
            projects = (
                [p.stem for p in sorted(proj_d.glob("*.duckdb"))]
                if args.all else [args.project]
            )
            for proj in projects:
                try:
                    sync_b2c(proj, central, proj_d)
                except Exception as exc:
                    print(f"  [{proj}] ERROR: {exc}", file=sys.stderr)

        else:  # c2b
            if args.all:
                bn_set: set[str] = set()
                for tbl in _node_tables(central):
                    try:
                        for (name,) in central.execute(
                            f'SELECT DISTINCT unnest(bayesian_network) FROM "{tbl}"'
                        ).fetchall():
                            if name:
                                bn_set.add(name)
                    except Exception:
                        pass
                for etbl in _edge_tables(central):
                    try:
                        for (name,) in central.execute(
                            f'SELECT DISTINCT unnest(bayesian_network) FROM "{etbl}"'
                        ).fetchall():
                            if name:
                                bn_set.add(name)
                    except Exception:
                        pass

                projects = list(bn_set)
            else:
                projects = [args.project]

            print(
                "⚠️  c2b writes to Bayesian DBs — ensure the FastAPI server is stopped first.\n"
            )
            for proj in projects:
                try:
                    sync_c2b(proj, central, proj_d, prune=getattr(args, "prune", False))
                except Exception as exc:
                    print(f"  [{proj}] ERROR: {exc}", file=sys.stderr)

        if args.cmd in {"b2c", "c2b"}:
            _refresh_property_graph(central)

        if args.cmd == "b2c":
            try:
                central.execute("VACUUM")
            except Exception as exc:
                print(f"Warning: VACUUM failed: {exc}", file=sys.stderr)

    finally:
        central.close()


if __name__ == "__main__":
    main()
