# Bayesian Network Editor

A browser-based visual editor for building, editing, and querying **Bayesian networks**. Draw nodes and edges interactively, fill in Conditional Probability Tables (CPTs), run Variable Elimination inference, and export your networks to Neo4j or PuppyGraph. It is described in my article [DuckBay: The Bayesian Knowledge Graph App](https://medium.com/@dgg32/duckbay-the-bayesian-knowledge-graph-app-398a152879f0).


## Features

- **Visual graph editor** — drag-and-drop canvas powered by [vis-network](https://visjs.github.io/vis-network/docs/network/)
- **CPT editor** — auto-generated table for each node's conditional probability distribution
- **Bayesian inference** — set evidence on any node and instantly see updated marginal probabilities across the whole network (Variable Elimination via [pgmpy](https://pgmpy.org/))
- **Multi-project support** — create, switch between, and delete named projects; each is stored in its own DuckDB file
- **Labels & colors** — tag nodes by category with custom colors
- **Node & edge properties** — attach arbitrary typed key/value metadata (VARCHAR, INTEGER, BOOLEAN, FLOAT, TIMESTAMP)
- **BIF import** — load standard `.bif` Bayesian Interchange Format files
- **Neo4j export** — export nodes and edges to CSV files grouped by label for Neo4j bulk import
- **PuppyGraph export** — generate a PuppyGraph-compatible schema JSON from your network
- **DuckLake sync** — optional synchronization with a DuckLake catalog

## Tech Stack

| Layer | Technology |
|---|---|
| Backend API | FastAPI + Uvicorn |
| Storage | DuckDB (one `.duckdb` file per project) |
| Inference | pgmpy (VariableElimination) |
| Frontend | Vanilla HTML/CSS/JS |
| Graph rendering | vis-network |

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Start the API server

```bash
uvicorn main:app --reload
```

### 3. Open the editor

Open `index.html` directly in your browser (no build step needed), or serve it from any static file server.

> The API defaults to `http://localhost:8000`. If you run it on a different host or port, update the `API` constant at the top of `js/app.js`.

## Usage

### Edit Mode

1. Click **+ Add Node** to create a node — give it an ID and a list of comma-separated states (e.g. `Yes, No`).
2. Draw edges by hovering over a node until the drag handle appears, then drag to another node.
3. Select a node to edit its states, parents, label, properties, and CPT.
4. Select an edge to add a label or typed properties.
5. Click **Save Node** / **Save Edge** to persist changes.

### Inference Mode

1. Switch to the **Inference** tab.
2. Click any node and select a state to set it as evidence.
3. The right panel shows ranked marginal probabilities for all nodes, updated in real time.
4. Click a node again to clear its evidence.

### Importing a BIF File

```bash
python import_bif.py <path/to/file.bif> [project_name]
```

If `project_name` is omitted, the network is imported into the `default` project.

### Exporting to Neo4j

```bash
python neo4j_exporter.py <project_name> <output_folder>
```

Produces `<label>.csv` node files and `edges/<label>.csv` edge files ready for `neo4j-admin import`.

### Exporting a PuppyGraph Schema

```bash
python puppygraph_schema_exporter.py <project_name>
```

Prints (or writes) a PuppyGraph-compatible JSON schema derived from your node labels and edge labels.

## Project Structure

```
.
├── main.py                     # FastAPI backend
├── index.html                  # Frontend entry point
├── css/style.css               # UI styles
├── js/                         # Frontend JavaScript
├── import_bif.py               # BIF file importer
├── neo4j_exporter.py           # Neo4j CSV exporter
├── puppygraph_schema_exporter.py
├── sync.py                     # DuckDB sync utility
├── sync_ducklake.py            # DuckLake catalog sync
├── ducklake_setup.ipynb        # DuckLake setup notebook
├── duckpgq.ipynb               # DuckPGQ exploration notebook
├── bif/                        # Example BIF network files
├── projects/                   # Per-project DuckDB databases (auto-created)
└── requirements.txt
```

## API Reference

| Method | Endpoint | Description |
|---|---|---|
| GET | `/projects` | List all projects |
| POST | `/projects` | Create a new project |
| DELETE | `/projects/{name}` | Delete a project |
| GET | `/network?project=` | Get all nodes and edges |
| POST | `/node?project=` | Create or update a node |
| DELETE | `/node/{id}?project=` | Delete a node |
| POST | `/edge?project=` | Update an edge label/properties |
| DELETE | `/edge?project=` | Delete an edge |
| GET | `/labels?project=` | List all labels and colors |
| POST | `/label?project=` | Create or update a label |
| POST | `/inference?project=` | Run Variable Elimination inference |

## License

This project is licensed under the [MIT License](LICENSE).

```
MIT License

Copyright (c) 2025 Sixing Huang

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
