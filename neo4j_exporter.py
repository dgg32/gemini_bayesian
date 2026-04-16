"""
Export node and edge (relation) tables from a Bayesian Visual Editor project
to CSV files grouped by label, suitable for Neo4j import.

Nodes with the same label are written to <label>.csv (node file).
Edges with the same label are written to <label>.csv (edge file) inside an
"edges/" sub-folder so names never clash with node files.
Unlabelled nodes/edges go to _unlabeled.csv in their respective locations.

Usage:
    python neo4j_exporter.py <project_name> <output_folder>

Example:
    python neo4j_exporter.py default ./neo4j_export
"""

import csv
import sys
from collections import defaultdict
from pathlib import Path

import duckdb

NODE_HEADERS = ["id", "name", "states", "label", "properties"]
EDGE_HEADERS = ["source", "target", "position", "label", "properties"]
UNLABELED = "_unlabeled"


def _write_csv(path: Path, headers: list[str], rows: list[tuple]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)


def export(project_name: str, output_folder: str) -> None:
    projects_dir = Path("projects")
    db_path = projects_dir / f"{project_name}.duckdb"

    if not db_path.exists():
        print(f"Error: project '{project_name}' not found at {db_path}")
        sys.exit(1)

    out_dir = Path(output_folder)
    nodes_dir = out_dir / "nodes"
    edges_dir = out_dir / "edges"

    conn = duckdb.connect(str(db_path), read_only=True)

    # ── Nodes grouped by label ────────────────────────────────────────────────
    nodes = conn.execute(
        "SELECT id, name, states, label, properties FROM node ORDER BY id"
    ).fetchall()

    by_label: dict[str, list[tuple]] = defaultdict(list)
    for row in nodes:
        lbl = (row[3] or "").strip() or UNLABELED
        by_label[lbl].append(row)

    for lbl, rows in sorted(by_label.items()):
        path = nodes_dir / f"{lbl}.csv"
        _write_csv(path, NODE_HEADERS, rows)
        print(f"Exported {len(rows)} node(s) [{lbl}] → {path}")

    # ── Edges grouped by label ────────────────────────────────────────────────
    edges = conn.execute("""
        SELECT n1.name AS source, n2.name AS target, r.position, r.label, r.properties
        FROM relation r
        JOIN node n1 ON r.source = n1.id
        JOIN node n2 ON r.target = n2.id
        ORDER BY n2.name, r.position
    """).fetchall()

    by_edge_label: dict[str, list[tuple]] = defaultdict(list)
    for row in edges:
        lbl = (row[3] or "").strip() or UNLABELED
        by_edge_label[lbl].append(row)

    for lbl, rows in sorted(by_edge_label.items()):
        path = edges_dir / f"{lbl}.csv"
        _write_csv(path, EDGE_HEADERS, rows)
        print(f"Exported {len(rows)} edge(s) [{lbl}] → {path}")

    conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python to_neo4j.py <project_name> <output_folder>")
        sys.exit(1)

    export(sys.argv[1], sys.argv[2])
