import json
import re
from contextlib import asynccontextmanager
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pgmpy.factors.discrete import TabularCPD
from pgmpy.inference import VariableElimination
from pgmpy.models import DiscreteBayesianNetwork
from pydantic import BaseModel

# ── Project storage ──────────────────────────────────────────────────────────

PROJECTS_DIR = Path("projects")
PROJECTS_DIR.mkdir(exist_ok=True)

_connections: dict[str, duckdb.DuckDBPyConnection] = {}
DEFAULT_PROJECT = "default"


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables in a freshly opened (or existing) project DB."""
    # Migration: drop legacy single-table schema if present
    conn.execute("DROP TABLE IF EXISTS nodes")

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
    conn.execute("ALTER TABLE relation ADD COLUMN IF NOT EXISTS properties VARCHAR DEFAULT '{}'")
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


def _validate_project(name: str) -> None:
    if not re.match(r'^[a-zA-Z0-9_-]+$', name):
        raise HTTPException(status_code=400, detail=f"Invalid project name: '{name}'")


def _get_conn(project: str) -> duckdb.DuckDBPyConnection:
    _validate_project(project)
    if project not in _connections:
        db_path = PROJECTS_DIR / f"{project}.duckdb"
        conn = duckdb.connect(str(db_path))
        _init_schema(conn)
        _connections[project] = conn
    return _connections[project]


# ── App lifecycle ────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(_app: FastAPI):
    _get_conn(DEFAULT_PROJECT)    # ensure at least one project exists on startup
    yield
    for c in _connections.values():
        c.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def normalize_cpt(cpt: list, n_child: int) -> list:
    """Normalize each column to sum to 1; zero columns become uniform."""
    if not cpt or not cpt[0]:
        return cpt
    result = [row[:] for row in cpt]
    for c in range(len(result[0])):
        col_sum = sum(result[r][c] for r in range(n_child))
        if col_sum <= 0:
            for r in range(n_child):
                result[r][c] = 1.0 / n_child
        elif abs(col_sum - 1.0) > 1e-9:
            for r in range(n_child):
                result[r][c] /= col_sum
    return result


def _load_graph(conn: duckdb.DuckDBPyConnection) -> tuple[dict, list]:
    """
    Read all nodes, relations, and CPT rows from a project connection.

    Returns:
        node_dict: {name -> {states, parents, cpt, label, properties}} where cpt is a 2D list.
                   CPT dimensions are derived from current parent cardinalities;
                   missing entries are zero-filled (preserves what fits).
        relations_raw: list of (source_name, target_name) tuples.
    """
    nodes_raw = conn.execute("SELECT id, name, states, label, properties FROM node").fetchall()
    # ORDER BY position preserves the parent ordering used when CPT was created
    relations_raw = conn.execute("""
        SELECT n1.name, n2.name, r.label, r.properties
        FROM relation r
        JOIN node n1 ON r.source = n1.id
        JOIN node n2 ON r.target = n2.id
        ORDER BY r.target, r.position
    """).fetchall()
    cpt_raw = conn.execute("""
        SELECT n.name, c.col_index, c.state_index, c.probability
        FROM cpt c
        JOIN node n ON c.node_id = n.id
    """).fetchall()

    node_states = {row[1]: list(row[2]) for row in nodes_raw}

    edge_labels: dict[tuple[str, str], str] = {}
    edge_props: dict[tuple[str, str], dict] = {}
    parent_map: dict[str, list[str]] = {}
    for source, target, edge_lbl, edge_props_json in relations_raw:
        parent_map.setdefault(target, []).append(source)
        edge_labels[(source, target)] = edge_lbl or ""
        edge_props[(source, target)] = json.loads(edge_props_json or '{}')

    cpt_map: dict[str, dict[tuple, float]] = {}
    for name, col_idx, state_idx, prob in cpt_raw:
        cpt_map.setdefault(name, {})[(col_idx, state_idx)] = prob

    node_dict: dict[str, dict] = {}
    for _, name, states, lbl, properties_json in nodes_raw:
        states = list(states)
        parents = parent_map.get(name, [])

        n_cols = 1
        for p in parents:
            card = len(node_states.get(p, []))
            n_cols *= card if card > 0 else 1

        stored = cpt_map.get(name, {})
        cpt = [
            [stored.get((c, r), 0.0) for c in range(n_cols)]
            for r in range(len(states))
        ]
        props = json.loads(properties_json or '{}')
        node_dict[name] = {
            "states": states,
            "parents": parents,
            "cpt": cpt,
            "label": lbl or "",
            "properties": props,
        }

    return node_dict, relations_raw, edge_labels, edge_props


# ── Pydantic models ──────────────────────────────────────────────────────────

class NodeData(BaseModel):
    id: str
    states: list[str]
    parents: list[str]
    cpt: list[list[float]]
    label: str = ""
    properties: dict = {}


class InferenceRequest(BaseModel):
    evidence: dict[str, str]


class ProjectCreate(BaseModel):
    name: str


class LabelData(BaseModel):
    name: str
    color: str


class EdgeData(BaseModel):
    from_id: str
    to_id: str
    label: str = ""
    properties: dict = {}


# ── Project endpoints ────────────────────────────────────────────────────────

@app.get("/projects")
def list_projects():
    """Return all project names (one per .duckdb file in projects/)."""
    names = sorted(p.stem for p in PROJECTS_DIR.glob("*.duckdb"))
    if not names:
        names = [DEFAULT_PROJECT]
    return names


@app.post("/projects")
def create_project(body: ProjectCreate):
    _get_conn(body.name)   # creates the file + schema if new
    return {"status": "created", "name": body.name}


@app.delete("/projects/{name}")
def delete_project(name: str):
    _validate_project(name)
    if name == DEFAULT_PROJECT:
        raise HTTPException(status_code=400, detail="Cannot delete the default project")
    if name in _connections:
        _connections[name].close()
        del _connections[name]
    db_path = PROJECTS_DIR / f"{name}.duckdb"
    db_path.unlink(missing_ok=True)
    return {"status": "deleted", "name": name}


# ── Network endpoints ────────────────────────────────────────────────────────

@app.get("/labels")
def get_labels(project: str = Query(DEFAULT_PROJECT)):
    conn = _get_conn(project)
    return dict(conn.execute("SELECT name, color FROM label").fetchall())


@app.post("/label")
def save_label(lbl: LabelData, project: str = Query(DEFAULT_PROJECT)):
    conn = _get_conn(project)
    conn.execute(
        "INSERT INTO label (name, color) VALUES (?, ?)"
        " ON CONFLICT (name) DO UPDATE SET color = EXCLUDED.color",
        (lbl.name, lbl.color),
    )
    return {"status": "success"}


@app.post("/edge")
def save_edge(edge: EdgeData, project: str = Query(DEFAULT_PROJECT)):
    conn = _get_conn(project)
    src = conn.execute("SELECT id FROM node WHERE name = ?", (edge.from_id,)).fetchone()
    tgt = conn.execute("SELECT id FROM node WHERE name = ?", (edge.to_id,)).fetchone()
    if not src or not tgt:
        raise HTTPException(status_code=404, detail="Node not found")
    conn.execute(
        "UPDATE relation SET label = ?, properties = ? WHERE source = ? AND target = ?",
        (edge.label, json.dumps(edge.properties), src[0], tgt[0]),
    )
    return {"status": "success"}


@app.get("/network")
def get_network(project: str = Query(DEFAULT_PROJECT)):
    conn = _get_conn(project)
    node_dict, relations_raw, edge_labels, edge_props = _load_graph(conn)
    label_colors = dict(conn.execute("SELECT name, color FROM label").fetchall())
    nodes, edges = [], []

    for name, data in node_dict.items():
        lbl = data["label"]
        node_color = label_colors.get(lbl, "#ffffcc") if lbl else "#ffffcc"
        nodes.append({
            "id": name,
            "states": data["states"],
            "parents": data["parents"],
            "cpt": data["cpt"],
            "label": lbl,
            "properties": data["properties"],
            "shape": "box",
            "color": node_color,
            "font": {"align": "left", "face": "monospace"},
        })

    for source, target, *_ in relations_raw:
        edge_lbl = edge_labels.get((source, target), "")
        edges.append({
            "from": source,
            "to": target,
            "arrows": "to",
            "title": f"{source} ➔ {target}" + (f" [{edge_lbl}]" if edge_lbl else ""),
            "edgeLabel": edge_lbl,
            "properties": edge_props.get((source, target), {}),
        })

    return {"nodes": nodes, "edges": edges, "labels": label_colors}


@app.post("/node")
def save_node(node: NodeData, project: str = Query(DEFAULT_PROJECT)):
    conn = _get_conn(project)

    existing = conn.execute("SELECT id FROM node WHERE name = ?", (node.id,)).fetchone()
    if existing:
        node_int_id = existing[0]
        conn.execute(
            "UPDATE node SET states = ?, label = ?, properties = ? WHERE id = ?",
            (node.states, node.label, json.dumps(node.properties), node_int_id),
        )
    else:
        # Reject if a case-variant already exists (names are unique case-insensitively)
        conflict = conn.execute(
            "SELECT name FROM node WHERE LOWER(name) = LOWER(?) AND name != ?",
            (node.id, node.id),
        ).fetchone()
        if conflict:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Node name '{node.id}' conflicts with existing node '{conflict[0]}'. "
                    "Node names must be unique case-insensitively."
                ),
            )
        node_int_id = conn.execute(
            "INSERT INTO node (name, states, label, properties) VALUES (?, ?, ?, ?) RETURNING id",
            (node.id, node.states, node.label, json.dumps(node.properties)),
        ).fetchone()[0]

    conn.execute("DELETE FROM relation WHERE target = ?", (node_int_id,))
    if node.parents:
        placeholders = ", ".join("?" * len(node.parents))
        parent_id_map = {
            name: pid
            for name, pid in conn.execute(
                f"SELECT name, id FROM node WHERE name IN ({placeholders})",
                node.parents,
            ).fetchall()
        }
        conn.executemany(
            "INSERT INTO relation (source, target, position) VALUES (?, ?, ?)",
            [
                (parent_id_map[p], node_int_id, pos)
                for pos, p in enumerate(node.parents)
                if p in parent_id_map
            ],
        )

    conn.execute("DELETE FROM cpt WHERE node_id = ?", (node_int_id,))
    rows = [
        (node_int_id, c, r, float(prob))
        for r, row in enumerate(node.cpt)
        for c, prob in enumerate(row)
    ]
    conn.executemany(
        "INSERT INTO cpt (node_id, col_index, state_index, probability) VALUES (?, ?, ?, ?)",
        rows,
    )

    return {"status": "success"}


@app.delete("/node/{node_id}")
def delete_node(node_id: str, project: str = Query(DEFAULT_PROJECT)):
    conn = _get_conn(project)
    int_id_row = conn.execute("SELECT id FROM node WHERE name = ?", (node_id,)).fetchone()
    if not int_id_row:
        return {"status": "not found"}
    int_id = int_id_row[0]

    conn.execute("DELETE FROM cpt WHERE node_id = ?", (int_id,))
    conn.execute("DELETE FROM relation WHERE source = ? OR target = ?", (int_id, int_id))
    conn.execute("DELETE FROM node WHERE id = ?", (int_id,))

    return {"status": "success"}


@app.post("/inference")
def run_inference(req: InferenceRequest, project: str = Query(DEFAULT_PROJECT)):
    try:
        conn = _get_conn(project)
        node_dict, *_ = _load_graph(conn)
        model = DiscreteBayesianNetwork()

        for node_id, data in node_dict.items():
            if not data["parents"]:
                model.add_node(node_id)
            for p in data["parents"]:
                model.add_edge(p, node_id)

        for node_id, data in node_dict.items():
            parent_cards = [len(node_dict[p]["states"]) for p in data["parents"]]
            safe_cpt = normalize_cpt(data["cpt"], len(data["states"]))
            cpd = TabularCPD(
                variable=node_id,
                variable_card=len(data["states"]),
                values=safe_cpt,
                evidence=data["parents"] if data["parents"] else None,
                evidence_card=parent_cards if data["parents"] else None,
                state_names={
                    node_id: data["states"],
                    **{p: node_dict[p]["states"] for p in data["parents"]},
                },
            )
            model.add_cpds(cpd)

        valid_evidence = {k: v for k, v in req.evidence.items() if k in node_dict}
        infer = VariableElimination(model)
        results = {}

        for node_id, data in node_dict.items():
            if node_id in valid_evidence:
                results[node_id] = {
                    s: (1.0 if s == valid_evidence[node_id] else 0.0)
                    for s in data["states"]
                }
            else:
                res = infer.query(variables=[node_id], evidence=valid_evidence)
                results[node_id] = {
                    data["states"][i]: round(float(res.values[i]), 4)
                    for i in range(len(data["states"]))
                }

        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
