// Generate DOM element for node tooltips (must be a DOM node for vis-network to render HTML)
function formatTooltip(node) {
    const states  = node.states;
    const parents = node.parents;
    const cpt     = node.cpt;

    const thS  = 'border:1px solid #ccc; padding:3px 6px; background:#e0e0e0;';
    const pthS = 'border:1px solid #ccc; padding:3px 6px; background:#c8d8e8; text-align:left; font-style:italic;';
    const tdS  = 'border:1px solid #ccc; padding:3px 6px; text-align:right;';
    const stS  = 'border:1px solid #ccc; padding:3px 6px; font-weight:bold;';
    const tblS = 'border-collapse:collapse; font-size:12px; margin-top:5px;';

    let html = `<strong>${node.id}</strong>`;
    if (parents.length > 0)
        html += `<br><span style="color:#555; font-size:12px;">Parents: ${parents.join(', ')}</span>`;
    html += '<br>';

    if (parents.length === 0) {
        // Root node: simple prior table
        html += `<table style="${tblS}">
            <tr><th style="${thS}">State</th><th style="${thS}">P</th></tr>`;
        states.forEach((s, r) => {
            const val = cpt[r]?.[0] ?? 0;
            html += `<tr><td style="${stS}">${s}</td><td style="${tdS}">${(+val).toFixed(4)}</td></tr>`;
        });
        html += '</table>';
    } else {
        // Conditional node: full joint CPT table with hierarchical parent headers
        const parentStatesList = parents.map(p => globalNodeData[p]?.states || ['?']);
        const cardinalities    = parentStatesList.map(ps => ps.length);
        const nCombos          = cardinalities.reduce((a, b) => a * b, 1);

        html += `<table style="${tblS}">`;

        // One header row per parent (same colspan logic as the edit panel)
        parents.forEach((parent, i) => {
            let span = 1;
            for (let j = i + 1; j < parents.length; j++) span *= cardinalities[j];
            const repeats = nCombos / (cardinalities[i] * span);
            html += `<tr><th style="${pthS}">${parent}</th>`;
            for (let rep = 0; rep < repeats; rep++) {
                parentStatesList[i].forEach(s => {
                    html += `<th colspan="${span}" style="${thS}">${s}</th>`;
                });
            }
            html += '</tr>';
        });

        // One data row per child state
        states.forEach((state, r) => {
            html += `<tr><td style="${stS}">${state}</td>`;
            for (let c = 0; c < nCombos; c++) {
                const val = cpt[r]?.[c] ?? 0;
                html += `<td style="${tdS}">${(+val).toFixed(4)}</td>`;
            }
            html += '</tr>';
        });
        html += '</table>';
    }

    const div = document.createElement('div');
    div.style.cssText = 'background:white; border:1px solid #ccc; padding:8px; border-radius:4px; font-size:13px; overflow-x:auto;';
    div.innerHTML = html;
    return div;
}

async function loadNetwork() {
    const res = await fetch(`http://localhost:8000/network?project=${encodeURIComponent(currentProject)}`);
    const data = await res.json();

    nodesSet.clear(); edgesSet.clear(); globalNodeData = {};
    labelColors = data.labels || {};

    // First pass: populate globalNodeData so formatTooltip can resolve parent states.
    // Save backend `label` (the category-like field) as `nodeLabel` before vis-network
    // overwrites the `label` property with the canvas display text.
    data.nodes.forEach(n => {
        n.nodeLabel = n.label;
        n.properties = n.properties || {};
        globalNodeData[n.id] = n;
    });

    // Second pass: build display labels and tooltips now that all parents are available
    data.nodes.forEach(n => {
        n.label = `${n.id}\n` + n.states.map(s => `${s}: ?`).join('\n');
        n.title = formatTooltip(n);
        nodesSet.add(n);
    });
    data.edges.forEach(e => {
        e.label      = e.edgeLabel || '';   // vis-network displays this on the edge line
        e.properties = e.properties || {};
        if (e.label) e.font = { align: 'middle', size: 11 };
    });
    edgesSet.add(data.edges);

    // Run force layout until stable, then freeze so nodes can be dragged freely
    if (data.nodes.length > 0) {
        network.setOptions({
            physics: {
                enabled: true,
                barnesHut: {
                    gravitationalConstant: -5000,
                    centralGravity: 0.1,
                    springLength: 180,
                    springConstant: 0.03,
                    damping: 0.12,
                },
                stabilization: { iterations: 400, fit: true },
            },
        });
        network.once('stabilized', () => {
            network.setOptions({ physics: { enabled: false } });
            network.fit();
        });
    }
}

function switchMode(mode) {
    document.getElementById('mode-edit').style.display = mode === 'edit' ? 'block' : 'none';
    document.getElementById('mode-inference').style.display = mode === 'inference' ? 'block' : 'none';
    document.getElementById('tab-edit').classList.toggle('active', mode === 'edit');
    document.getElementById('tab-inference').classList.toggle('active', mode === 'inference');
    if (mode === 'inference') {
        runInference();
    } else {
        // Restore original labels and label colors when returning to edit mode
        nodesSet.update(Object.values(globalNodeData).map(n => {
            const lbl = n.nodeLabel;
            return {
                id: n.id,
                label: `${n.id}\n` + n.states.map(s => `${s}: ?`).join('\n'),
                color: (lbl && labelColors[lbl]) ? labelColors[lbl] : '#ffffcc'
            };
        }));
    }
}
