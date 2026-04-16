#!/usr/bin/env python3
"""Generate a PuppyGraph schema.json from central.duckdb.

The script reads the table metadata from [`central.duckdb`](central.duckdb), uses
[`example/schema_example.json`](example/schema_example.json) as the base template,
replaces only the [`graph`](example/schema_example.json:13) section, and writes the
result to a user-specified output path.

Included tables:
- vertex tables: tables with a [`bayesian_network`](sync.py:135) column and no [`from_id`](sync.py:150) column
- edge tables: tables with a [`from_id`](sync.py:150) column

Excluded tables:
- [`cpt`](main.py:62)
- [`label`](main.py:71)
- [`project_node`](sync.py)
- [`relation`](main.py:51)

Assumptions:
- vertex tables expose an [`id`](neo4j_exporter.py:53) column
- edge tables expose [`from_id`](sync.py:150) and [`to_id`](sync.py:150) columns
- all remaining columns are exported as PuppyGraph attributes
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

EXCLUDED_TABLES = {"cpt", "label", "project_node", "relation"}
DEFAULT_TEMPLATE = Path("example/schema_example.json")
DEFAULT_DB = Path("central.duckdb")
DEFAULT_CATALOG = "ddt"
DEFAULT_SCHEMA = "main"
DEFAULT_JDBC_URI = "jdbc:duckdb:/home/share/central.duckdb"
DEFAULT_DRIVER_CLASS = "org.duckdb.DuckDBDriver"
DEFAULT_DRIVER_URL = (
    "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.4.1.0/"
    "duckdb_jdbc-1.4.1.0.jar"
)


def _run_duckdb_query(db_path: Path, sql: str) -> list[dict[str, Any]]:
    """Run SQL through the Python duckdb package and return rows as dictionaries."""
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Python package `duckdb` is not installed in the active environment. Run this script with your uv environment, for example `uv run python3 puppygraph_schema_exporter.py ...`."
        ) from exc

    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        cursor = conn.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        conn.close()


def _load_template(template_path: Path) -> dict[str, Any]:
    with template_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _list_columns(db_path: Path) -> dict[str, list[dict[str, str]]]:
    sql = """
    SELECT table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'main'
    ORDER BY table_name, ordinal_position
    """
    rows = _run_duckdb_query(db_path, sql)
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        table_name = row["table_name"]
        grouped.setdefault(table_name, []).append(
            {
                "column_name": row["column_name"],
                "data_type": row["data_type"],
            }
        )
    return grouped


def _is_vertex_table(columns: list[dict[str, str]]) -> bool:
    names = {col["column_name"] for col in columns}
    return "bayesian_network" in names and "from_id" not in names


def _is_edge_table(columns: list[dict[str, str]]) -> bool:
    names = {col["column_name"] for col in columns}
    return "from_id" in names and "to_id" in names


def _needs_string_cast(duckdb_type: str) -> bool:
    normalized = duckdb_type.upper()
    return "[]" in normalized or normalized == "JSON"



def _puppygraph_type(duckdb_type: str) -> str:
    normalized = duckdb_type.upper()
    if _needs_string_cast(normalized):
        return "String"
    if any(token in normalized for token in ["BOOL"]):
        return "Boolean"
    if any(token in normalized for token in ["TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UBIGINT"]):
        return "Long"
    if any(token in normalized for token in ["DECIMAL", "DOUBLE", "FLOAT", "REAL"]):
        return "Double"
    return "String"



def _attribute_field(column_name: str, duckdb_type: str) -> str:
    if _needs_string_cast(duckdb_type):
        return f"CAST({column_name} AS VARCHAR)"
    return column_name


def _build_vertex(table_name: str, columns: list[dict[str, str]], catalog: str, schema: str) -> dict[str, Any]:
    names = [col["column_name"] for col in columns]
    if "id" not in names:
        raise ValueError(f"Vertex table '{table_name}' does not contain an 'id' column")

    id_column = next(col for col in columns if col["column_name"] == "id")
    attributes = [
        {
            "type": _puppygraph_type(col["data_type"]),
            "field": _attribute_field(col["column_name"], col["data_type"]),
            "alias": col["column_name"],
        }
        for col in columns
    ]

    return {
        "label": table_name,
        "oneToOne": {
            "tableSource": {
                "catalog": catalog,
                "schema": schema,
                "table": table_name,
            },
            "id": {
                "fields": [
                    {
                        "type": _puppygraph_type(id_column["data_type"]),
                        "field": "id",
                        "alias": "ID",
                    }
                ]
            },
            "attributes": attributes,
        },
        "cacheConfig": {},
    }


def _build_edge(
    table_name: str,
    columns: list[dict[str, str]],
    vertex_tables: list[str],
    catalog: str,
    schema: str,
) -> dict[str, Any]:
    names = {col["column_name"] for col in columns}
    if "from_id" not in names or "to_id" not in names:
        raise ValueError(f"Edge table '{table_name}' must contain 'from_id' and 'to_id'")

    if len(vertex_tables) == 1:
        from_vertex = vertex_tables[0]
        to_vertex = vertex_tables[0]
    else:
        from_vertex = vertex_tables[0] if vertex_tables else table_name
        to_vertex = vertex_tables[0] if vertex_tables else table_name

    from_col = next(col for col in columns if col["column_name"] == "from_id")
    to_col = next(col for col in columns if col["column_name"] == "to_id")

    return {
        "label": table_name,
        "fromVertex": from_vertex,
        "toVertex": to_vertex,
        "tableSource": {
            "catalog": catalog,
            "schema": schema,
            "table": table_name,
        },
        "fromId": {
            "fields": [
                {
                    "type": _puppygraph_type(from_col["data_type"]),
                    "field": "from_id",
                    "alias": "from_id",
                }
            ]
        },
        "toId": {
            "fields": [
                {
                    "type": _puppygraph_type(to_col["data_type"]),
                    "field": "to_id",
                    "alias": "to_id",
                }
            ]
        },
        "cacheConfig": {},
    }


def build_graph(db_path: Path, catalog: str, schema: str) -> dict[str, Any]:
    columns_by_table = _list_columns(db_path)
    filtered = {
        table_name: columns
        for table_name, columns in columns_by_table.items()
        if table_name not in EXCLUDED_TABLES
    }

    vertex_tables = sorted(
        table_name for table_name, columns in filtered.items() if _is_vertex_table(columns)
    )
    edge_tables = sorted(
        table_name for table_name, columns in filtered.items() if _is_edge_table(columns)
    )

    vertices = [
        _build_vertex(table_name, filtered[table_name], catalog, schema)
        for table_name in vertex_tables
    ]
    edges = [
        _build_edge(table_name, filtered[table_name], vertex_tables, catalog, schema)
        for table_name in edge_tables
    ]

    return {
        "vertices": vertices,
        "edges": edges,
    }


def update_catalog(template: dict[str, Any], jdbc_uri: str, catalog_name: str) -> None:
    catalogs = template.get("catalogs", [])
    if not catalogs:
        template["catalogs"] = [
            {
                "name": catalog_name,
                "type": "duckdb",
                "jdbc": {
                    "jdbcUri": jdbc_uri,
                    "driverClass": DEFAULT_DRIVER_CLASS,
                    "driverUrl": DEFAULT_DRIVER_URL,
                },
            }
        ]
        return

    catalogs[0]["name"] = catalog_name
    catalogs[0]["type"] = "duckdb"
    catalogs[0]["jdbc"] = {
        "jdbcUri": jdbc_uri,
        "driverClass": DEFAULT_DRIVER_CLASS,
        "driverUrl": DEFAULT_DRIVER_URL,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate PuppyGraph schema.json from central.duckdb"
    )
    parser.add_argument(
        "output_path",
        help="Path where the generated schema.json will be written",
    )
    parser.add_argument(
        "--db",
        default=str(DEFAULT_DB),
        help="Path to the source DuckDB database (default: central.duckdb)",
    )
    parser.add_argument(
        "--template",
        default=str(DEFAULT_TEMPLATE),
        help="Path to the PuppyGraph template JSON (default: example/schema_example.json)",
    )
    parser.add_argument(
        "--catalog",
        default=DEFAULT_CATALOG,
        help="Catalog name to use in the generated graph section",
    )
    parser.add_argument(
        "--schema-name",
        default=DEFAULT_SCHEMA,
        help="Schema name to use in the generated graph section",
    )
    parser.add_argument(
        "--jdbc-uri",
        default=DEFAULT_JDBC_URI,
        help="JDBC URI to write into the catalogs section",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)
    template_path = Path(args.template)
    output_path = Path(args.output_path)

    if not db_path.exists():
        print(f"Error: database not found: {db_path}", file=sys.stderr)
        return 1
    if not template_path.exists():
        print(f"Error: template not found: {template_path}", file=sys.stderr)
        return 1

    template = _load_template(template_path)
    update_catalog(template, args.jdbc_uri, args.catalog)
    template["graph"] = build_graph(db_path, args.catalog, args.schema_name)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(template, f, indent=2)
        f.write("\n")

    print(f"Wrote PuppyGraph schema to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
