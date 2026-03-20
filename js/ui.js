// ── vis-network initialisation ───────────────────────────────────────────────
const container = document.getElementById('network-container');
network = new vis.Network(container, { nodes: nodesSet, edges: edgesSet }, {
    physics: { enabled: false },
    interaction: { hover: true },
    nodes: {
        chosen: {
            node: function (values, _id, selected, _hovering) {
                if (selected) {
                    values.borderColor = '#4fc3f7';
                    values.borderWidth = 3;
                    values.shadowColor = 'rgba(79, 195, 247, 0.5)';
                    values.shadowSize = 8;
                }
            }
        }
    },
    edges: {
        chosen: {
            edge: function (values, _id, selected, _hovering) {
                if (selected) {
                    values.color = '#4fc3f7';
                    values.width = 3;
                    values.shadowColor = 'rgba(79, 195, 247, 0.5)';
                    values.shadowSize = 8;
                }
            }
        }
    }
});

// ── Canvas click handler ─────────────────────────────────────────────────────
network.on("click", function (params) {
    const isInferenceMode = document.getElementById('mode-inference').style.display !== 'none';

    if (isInferenceMode) {
        if (params.nodes.length === 0) return;
        const nodeId = params.nodes[0];
        const nodeData = globalNodeData[nodeId];
        document.getElementById('ev-node').value = nodeId;
        document.getElementById('ev-state').innerHTML = nodeData.states.map(s => `<option value="${s}">${s}</option>`).join('');
        return;
    }

    // Edit mode: edge click → show edge editor only, hide node panel
    if (params.edges.length > 0 && params.nodes.length === 0) {
        const edge = edgesSet.get(params.edges[0]);

        currentEdge = { from: edge.from, to: edge.to, edgeLabel: edge.edgeLabel || '', properties: edge.properties || {} };
        currentEdgeProperties = { ...currentEdge.properties };
        document.getElementById('edge-title').textContent = `Edge: ${edge.from} → ${edge.to}`;
        document.getElementById('edge-label-input').value = currentEdge.edgeLabel;
        document.getElementById('edge-section').style.display = 'block';
        document.getElementById('node-section').style.display = 'none';
        renderEdgePropertiesList();
        clearEdgeDirty();
        return;
    }

    // Edit mode: node click → load node properties
    if (params.nodes.length > 0) {
        currentEdge = null;
        document.getElementById('edge-section').style.display = 'none';
        document.getElementById('node-section').style.display = 'block';
        const nodeId = params.nodes[0];
        const nodeData = globalNodeData[nodeId];

        document.getElementById('edit-title').textContent = 'Add / Edit Node';
        document.getElementById('n-id').value = nodeId;
        document.getElementById('n-states').value = nodeData.states.join(', ');
        document.getElementById('n-parents').value = nodeData.parents.join(', ');
        populateLabelFields(nodeData.nodeLabel);
        currentNodeId = nodeId;
        currentProperties = { ...(nodeData.properties || {}) };
        renderPropertiesList();
        document.getElementById('cpt-label').textContent = nodeData.parents.length > 0 ? 'Joint CPT Matrix' : 'CPT Matrix';
        generateCPTTable(nodeData.cpt);
        clearNodeDirty();
    }
});

// ── Resizable sidebar ────────────────────────────────────────────────────────
const resizeHandle = document.getElementById('resize-handle');
const panel = document.getElementById('panel');
let isResizing = false;

resizeHandle.addEventListener('mousedown', e => {
    isResizing = true;
    resizeHandle.classList.add('dragging');
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    e.preventDefault();
});

document.addEventListener('mousemove', e => {
    if (!isResizing) return;
    const newWidth = window.innerWidth - e.clientX;
    if (newWidth >= 200 && newWidth <= window.innerWidth - 200) {
        panel.style.width = newWidth + 'px';
    }
});

document.addEventListener('mouseup', () => {
    if (!isResizing) return;
    isResizing = false;
    resizeHandle.classList.remove('dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
});

// ── Dirty-state listeners ─────────────────────────────────────────────────────
['n-id', 'n-states', 'n-parents', 'n-label', 'n-label-color'].forEach(id => {
    document.getElementById(id).addEventListener('input', markNodeDirty);
});
// CPT inputs are rendered dynamically — delegate from their container
document.getElementById('cpt-table-container').addEventListener('input', markNodeDirty);
// Auto-fill buttons (OR/AND gate) also change CPT
document.querySelectorAll('.cpt-autofill button').forEach(btn => {
    btn.addEventListener('click', markNodeDirty);
});
document.getElementById('edge-label-input').addEventListener('input', markEdgeDirty);

// ── Bootstrap ────────────────────────────────────────────────────────────────
async function init() {
    document.getElementById('tab-edit').classList.add('active');
    await loadProjects();
    await loadNetwork();
}
init();
