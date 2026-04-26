// ── Unsaved-changes indicators ────────────────────────────────────────────────

function markNodeDirty()  { document.getElementById('node-dirty-badge').style.display = 'inline-block'; }
function clearNodeDirty() { document.getElementById('node-dirty-badge').style.display = 'none'; }
function markEdgeDirty()  { document.getElementById('edge-dirty-badge').style.display = 'inline-block'; }
function clearEdgeDirty() { document.getElementById('edge-dirty-badge').style.display = 'none'; }

function populateLabelFields(lbl) {
    lbl = lbl || '';
    document.getElementById('n-label').value = lbl;
    document.getElementById('n-label-color').value = (lbl && labelColors[lbl]) ? labelColors[lbl] : '#ffffcc';
}

function onLabelInput() {
    const lbl = document.getElementById('n-label').value.trim();
    if (lbl && labelColors[lbl]) {
        document.getElementById('n-label-color').value = labelColors[lbl];
    } else if (!lbl) {
        document.getElementById('n-label-color').value = '#ffffcc';
    }
}

// ── Property editor ──────────────────────────────────────────────────────────

function buildPropsTable(entries, removeFn) {
    if (!entries.length)
        return '<i style="font-size:11px; color:#999;">(none)</i>';
    return `<table class="props-table">
        <thead><tr><th>Key</th><th>Value</th><th>Type</th><th></th></tr></thead>
        <tbody>` +
        entries.map(([key, {value, type}]) =>
            `<tr>
                <td class="props-td-key" title="${escHtml(key)}">${escHtml(key)}</td>
                <td class="props-td-val" title="${escHtml(value)}">${escHtml(value)}</td>
                <td class="props-td-type">${escHtml(type)}</td>
                <td><button class="prop-remove" data-key="${escHtml(key)}" onclick="${removeFn}(this.dataset.key)">×</button></td>
            </tr>`
        ).join('') +
        `</tbody></table>`;
}

function renderPropertiesList() {
    document.getElementById('properties-list').innerHTML =
        buildPropsTable(Object.entries(currentProperties), 'removeProperty');
}

function addProperty() {
    const key   = document.getElementById('prop-key').value.trim();
    const value = document.getElementById('prop-value').value.trim();
    const type  = document.getElementById('prop-type').value;
    if (!key) { alert('Property key is required.'); return; }
    currentProperties[key] = { value, type };
    if (currentNodeId && globalNodeData[currentNodeId])
        globalNodeData[currentNodeId].properties = { ...currentProperties };
    renderPropertiesList();
    document.getElementById('prop-key').value   = '';
    document.getElementById('prop-value').value = '';
    markNodeDirty();
}

function removeProperty(key) {
    delete currentProperties[key];
    if (currentNodeId && globalNodeData[currentNodeId])
        globalNodeData[currentNodeId].properties = { ...currentProperties };
    renderPropertiesList();
    markNodeDirty();
}

// ── Edge property editor ──────────────────────────────────────────────────────

function renderEdgePropertiesList() {
    document.getElementById('edge-properties-list').innerHTML =
        buildPropsTable(Object.entries(currentEdgeProperties), 'removeEdgeProperty');
}

function addEdgeProperty() {
    const key   = document.getElementById('edge-prop-key').value.trim();
    const value = document.getElementById('edge-prop-value').value.trim();
    const type  = document.getElementById('edge-prop-type').value;
    if (!key) { alert('Property key is required.'); return; }
    currentEdgeProperties[key] = { value, type };
    renderEdgePropertiesList();
    document.getElementById('edge-prop-key').value   = '';
    document.getElementById('edge-prop-value').value = '';
    markEdgeDirty();
}

function removeEdgeProperty(key) {
    delete currentEdgeProperties[key];
    renderEdgePropertiesList();
    markEdgeDirty();
}

async function saveEdgeLabel() {
    if (!currentEdge) return;
    const label      = document.getElementById('edge-label-input').value.trim();
    const properties = { ...currentEdgeProperties };
    await fetch(`http://localhost:8000/edge?project=${encodeURIComponent(currentProject)}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ from_id: currentEdge.from, to_id: currentEdge.to, label, properties })
    });
    currentEdge.edgeLabel   = label;
    currentEdge.properties  = properties;
    // Update the vis-network edge label and tooltip
    const edgeArr = edgesSet.get({ filter: e => e.from === currentEdge.from && e.to === currentEdge.to });
    if (edgeArr.length > 0) {
        edgesSet.update({
            id: edgeArr[0].id,
            label,
            edgeLabel: label,
            properties,
            title: `${currentEdge.from} ➔ ${currentEdge.to}` + (label ? ` [${label}]` : ''),
        });
    }
    clearEdgeDirty();
}

async function deleteEdge() {
    if (!currentEdge) return;
    const { from: fromId, to: toId } = currentEdge;
    if (!confirm(`Delete edge "${fromId} → ${toId}"?`)) return;

    // Remove from backend
    await fetch(
        `http://localhost:8000/edge?from_id=${encodeURIComponent(fromId)}&to_id=${encodeURIComponent(toId)}&project=${encodeURIComponent(currentProject)}`,
        { method: 'DELETE' }
    );

    // Remove from vis-network
    const edgeArr = edgesSet.get({ filter: e => e.from === fromId && e.to === toId });
    edgeArr.forEach(e => edgesSet.remove(e.id));

    // Downstream: update the child node (toId) — remove fromId from its parents, resize CPT
    const child = globalNodeData[toId];
    if (child && child.parents.includes(fromId)) {
        const newParents = child.parents.filter(p => p !== fromId);
        const newCols = newParents.length > 0
            ? newParents.reduce((prod, p) => prod * (globalNodeData[p]?.states.length ?? 1), 1)
            : 1;
        const newCpt = child.states.map(() => Array(newCols).fill(0));
        globalNodeData[toId] = { ...child, parents: newParents, cpt: newCpt };

        await fetch(`http://localhost:8000/node?project=${encodeURIComponent(currentProject)}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                id: toId, states: child.states, parents: newParents, cpt: newCpt,
                label: child.nodeLabel || '', properties: child.properties || {}
            })
        });
        nodesSet.update({ id: toId, title: formatTooltip(globalNodeData[toId]) });
    }

    // Reset edge panel
    currentEdge = null;
    document.getElementById('edge-section').style.display = 'none';
}

async function addNewNode() {
    currentEdge = null;
    document.getElementById('edge-section').style.display = 'none';
    document.getElementById('node-section').style.display = 'block';
    const raw = prompt('Node ID:');
    if (!raw) return;
    const id = raw.trim();
    if (!id) return;
    if (globalNodeData[id]) {
        alert(`Node "${id}" already exists. Click it on the canvas to edit it.`);
        return;
    }

    const states = ['Yes', 'No'];
    const cpt = [[0.5], [0.5]];

    await fetch(`http://localhost:8000/node?project=${encodeURIComponent(currentProject)}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id, states, parents: [], cpt, label: '', properties: {} })
    });

    globalNodeData[id] = { id, states, parents: [], cpt, nodeLabel: '', properties: {} };
    nodesSet.add({
        id, states, parents: [], cpt,
        label: `${id}\n` + states.map(s => `${s}: ?`).join('\n'),
        title: formatTooltip(globalNodeData[id]),
        shape: 'box', color: '#ffffcc', font: { align: 'left', face: 'monospace' }
    });

    // Load the new node into the editor panel
    document.getElementById('edit-title').textContent = 'Add / Edit Node';
    const nIdField = document.getElementById('n-id');
    nIdField.value = id;
    nIdField.readOnly = false;
    document.getElementById('n-states').value = states.join(', ');
    document.getElementById('n-parents').value = '';
    populateLabelFields('');
    currentNodeId = id;
    currentProperties = {};
    renderPropertiesList();
    document.getElementById('cpt-label').textContent = 'CPT Matrix';
    generateCPTTable(cpt);
}

async function saveNode() {
    const nodeId = document.getElementById('n-id').value.trim();
    const states = document.getElementById('n-states').value.split(',').map(s => s.trim()).filter(s => s);
    const parents = document.getElementById('n-parents').value
        ? document.getElementById('n-parents').value.split(',').map(s => s.trim()).filter(s => s)
        : [];

    if (wouldCreateCycle(nodeId, parents)) {
        alert(`Cannot save: adding ${parents.length === 1 ? `"${parents[0]}"` : 'these parents'} as parent(s) of "${nodeId}" would create a cycle in the Bayesian Network.`);
        return;
    }

    const cols = document.querySelectorAll('.cpt-input[data-r="0"]').length;

    let cpt;
    if (cols > 0) {
        cpt = [];
        for (let r = 0; r < states.length; r++) {
            let rowArr = [];
            for (let c = 0; c < cols; c++) {
                const input = document.querySelector(`.cpt-input[data-r="${r}"][data-c="${c}"]`);
                rowArr.push(parseFloat(input.value) || 0);
            }
            cpt.push(rowArr);
        }
    } else {
        // No CPT inputs visible; preserve existing CPT from memory
        cpt = globalNodeData[nodeId]?.cpt ?? [];
    }

    // Warn if any CPT column doesn't sum to 1 (backend will normalize silently)
    if (cols > 0) {
        const badCols = [];
        for (let c = 0; c < cols; c++) {
            const sum = cpt.reduce((acc, row) => acc + (row[c] ?? 0), 0);
            if (Math.abs(sum - 1.0) > 0.001) badCols.push(c + 1);
        }
        if (badCols.length > 0) {
            const colList = badCols.length === 1
                ? `column ${badCols[0]}`
                : `columns ${badCols.join(', ')}`;
            if (!confirm(`${badCols.length === 1 ? 'Column' : 'Columns'} ${badCols.join(', ')} of the CPT ${badCols.length === 1 ? 'does' : 'do'} not sum to 1.\nThe backend will normalize ${badCols.length === 1 ? 'it' : 'them'} automatically. Save anyway?`)) return;
        }
    }

    const nodeLabel  = document.getElementById('n-label').value.trim();
    const labelColor = document.getElementById('n-label-color').value;
    const properties = { ...currentProperties };

    if (nodeLabel) {
        labelColors[nodeLabel] = labelColor;
        await fetch(`http://localhost:8000/label?project=${encodeURIComponent(currentProject)}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: nodeLabel, color: labelColor })
        });
    }

    await fetch(`http://localhost:8000/node?project=${encodeURIComponent(currentProject)}`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: nodeId, states, parents, cpt, label: nodeLabel, properties })
    });

    const nodeColor = (nodeLabel && labelColors[nodeLabel]) ? labelColors[nodeLabel] : '#ffffcc';

    // Update in-place: avoid full canvas redraw
    const oldParents = globalNodeData[nodeId]?.parents || [];
    globalNodeData[nodeId] = { ...globalNodeData[nodeId], id: nodeId, states, parents, cpt, nodeLabel, properties };

    // Update just this node's tooltip and color (canvas position preserved)
    if (nodesSet.get(nodeId)) {
        nodesSet.update({ id: nodeId, color: nodeColor, title: formatTooltip(globalNodeData[nodeId]) });
    } else {
        // New node: add it
        nodesSet.add({
            id: nodeId, states, parents, cpt,
            label: `${nodeId}\n` + states.map(s => `${s}: ?`).join('\n'),
            title: formatTooltip(globalNodeData[nodeId]),
            shape: 'box', color: nodeColor, font: { align: 'left', face: 'monospace' }
        });
    }

    // Propagate new label color to all other canvas nodes in the same label
    if (nodeLabel) {
        const siblings = Object.values(globalNodeData).filter(n => n.nodeLabel === nodeLabel && n.id !== nodeId);
        if (siblings.length > 0)
            nodesSet.update(siblings.map(n => ({ id: n.id, color: nodeColor })));
    }

    // Sync edges: remove deleted parents, add new ones
    const removedParents = oldParents.filter(p => !parents.includes(p));
    const addedParents = parents.filter(p => !oldParents.includes(p));

    removedParents.forEach(p => {
        const edge = edgesSet.get({ filter: e => e.from === p && e.to === nodeId })[0];
        if (edge) edgesSet.remove(edge.id);
    });
    addedParents.forEach(p => {
        edgesSet.add({ from: p, to: nodeId, arrows: 'to', title: `${p} ➔ ${nodeId}`, label: '', edgeLabel: '' });
    });

    document.getElementById('cpt-table-container').innerHTML = '<i>Node saved.</i>';
    document.getElementById('edit-title').textContent = 'Add / Edit Node';
    document.getElementById('cpt-label').textContent = 'CPT Matrix';
    renderPropertiesList();
    clearNodeDirty();
}

async function deleteNode() {
    const nodeId = document.getElementById('n-id').value.trim();
    if (!nodeId || !globalNodeData[nodeId]) {
        alert('Select an existing node first.');
        return;
    }

    if (!confirm(`Delete node "${nodeId}" and all its connected edges?`)) return;

    // Delete from backend
    await fetch(`http://localhost:8000/node/${encodeURIComponent(nodeId)}?project=${encodeURIComponent(currentProject)}`, { method: 'DELETE' });

    // Remove all edges connected to this node
    edgesSet.get({ filter: e => e.from === nodeId || e.to === nodeId })
            .forEach(e => edgesSet.remove(e.id));

    // Remove the node from the canvas
    nodesSet.remove(nodeId);

    // Update any child nodes that listed this node as a parent
    const children = Object.values(globalNodeData).filter(n => n.parents.includes(nodeId));
    for (const child of children) {
        const newParents = child.parents.filter(p => p !== nodeId);
        // Recompute CPT dimensions (reset to zeros)
        const newCols = newParents.length > 0
            ? newParents.reduce((prod, p) => prod * (globalNodeData[p]?.states.length ?? 1), 1)
            : 1;
        const newCpt = child.states.map(() => Array(newCols).fill(0));
        globalNodeData[child.id] = { ...child, parents: newParents, cpt: newCpt };

        await fetch(`http://localhost:8000/node?project=${encodeURIComponent(currentProject)}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                id: child.id, states: child.states, parents: newParents, cpt: newCpt,
                label: child.nodeLabel || '', properties: child.properties || {}
            })
        });
        nodesSet.update({ id: child.id, title: formatTooltip(globalNodeData[child.id]) });
    }

    delete globalNodeData[nodeId];

    // Reset panel
    resetEditorPanel();
    document.getElementById('cpt-table-container').innerHTML = '<i>Node deleted.</i>';
}

function resetEditorPanel() {
    currentNodeId = null;
    currentEdge = null;
    currentProperties = {};
    currentEdgeProperties = {};

    const nIdField = document.getElementById('n-id');
    nIdField.value = '';
    nIdField.readOnly = false;
    document.getElementById('n-states').value = '';
    document.getElementById('n-parents').value = '';
    populateLabelFields('');
    document.getElementById('cpt-table-container').innerHTML = '';
    document.getElementById('edit-title').textContent = 'Add / Edit Node';
    document.getElementById('cpt-label').textContent = 'CPT Matrix';
    renderPropertiesList();

    document.getElementById('edge-label-input').value = '';
    document.getElementById('edge-section').style.display = 'none';
    renderEdgePropertiesList();

    clearNodeDirty();
    clearEdgeDirty();
}
