// For presence/absence nodes always show the positive state;
// for nominal nodes (e.g. age ranges, lab values) show most likely.
const POSITIVE_STATES = ['present', 'yes', 'true', 'active', 'high',
                         'positive', 'abnormal', 'increased', 'elevated'];

function buildChildrenMap() {
    const map = {};
    Object.values(globalNodeData).forEach(n => {
        n.parents.forEach(p => { if (!map[p]) map[p] = []; map[p].push(n.id); });
    });
    return map;
}

// Returns Map<nodeId, minHops> for all ancestors (BFS preserves shortest path)
function getAncestors(nodeId) {
    const distances = new Map();
    const queue = [];
    for (const p of (globalNodeData[nodeId]?.parents || []))
        if (!distances.has(p)) { distances.set(p, 1); queue.push([p, 1]); }
    let head = 0;
    while (head < queue.length) {
        const [n, d] = queue[head++];
        for (const p of (globalNodeData[n]?.parents || []))
            if (!distances.has(p)) { distances.set(p, d + 1); queue.push([p, d + 1]); }
    }
    return distances;
}

// Returns Map<nodeId, minHops> for all descendants
function getDescendants(nodeId, childrenMap) {
    const distances = new Map();
    const queue = [];
    for (const c of (childrenMap[nodeId] || []))
        if (!distances.has(c)) { distances.set(c, 1); queue.push([c, 1]); }
    let head = 0;
    while (head < queue.length) {
        const [n, d] = queue[head++];
        for (const c of (childrenMap[n] || []))
            if (!distances.has(c)) { distances.set(c, d + 1); queue.push([c, d + 1]); }
    }
    return distances;
}

function toDisplayEntry(nodeId, probs) {
    const posState = Object.keys(probs).find(s => POSITIVE_STATES.includes(s.toLowerCase()));
    if (posState) return { nodeId, topState: posState, displayProb: probs[posState] };
    // Nominal: fall back to most likely state
    const maxProb = Object.values(probs).reduce((m, p) => Math.max(m, p), 0);
    const topState = Object.entries(probs).find(([, p]) => p === maxProb)?.[0] || '';
    return { nodeId, topState, displayProb: maxProb };
}

function renderRanked(list, barClass) {
    if (!list.length) return '<i style="font-size:11px; color:#888">none</i>';
    let html = '';
    let lastDist = null;
    for (const { nodeId, topState, displayProb, distance } of list) {
        if (distance !== lastDist) {
            lastDist = distance;
            html += `<div class="rank-depth">depth ${distance}</div>`;
        }
        html += `<div class="rank-row">
            <span class="rank-name" title="${escHtml(nodeId)}">${escHtml(nodeId)}</span>
            <div class="rank-bar-bg"><div class="rank-bar-fill ${barClass}" style="width:${(displayProb * 100).toFixed(1)}%"></div></div>
            <span class="rank-state">${escHtml(topState)}</span>
            <span class="rank-pct">${(displayProb * 100).toFixed(1)}%</span>
        </div>`;
    }
    return html;
}

async function addEvidence() {
    const node = document.getElementById('ev-node').value;
    const state = document.getElementById('ev-state').value;
    if (node && state) currentEvidence[node] = state;
    document.getElementById('evidence-log').innerText = formatEvidence(currentEvidence);
    runInference();
}

async function clearEvidence() {
    currentEvidence = {};
    document.getElementById('evidence-log').innerText = '(none)';
    runInference();
}

async function runInference() {
    const hintsEl = document.getElementById('inference-hints');
    const logEl   = document.getElementById('evidence-log');
    try {
        const res = await fetch(`http://localhost:8000/inference?project=${encodeURIComponent(currentProject)}`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ evidence: currentEvidence })
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({ detail: res.statusText }));
            logEl.innerText = formatEvidence(currentEvidence) + '\n\n⚠️ Inference error:\n' + (err.detail || res.statusText);
            hintsEl.innerHTML = '<i class="hint-error">⚠️ Inference failed.</i>';
            return;
        }

        const posteriors = await res.json();
        const updates = Object.keys(posteriors).map(nodeId => {
            const probs = posteriors[nodeId];
            const isEv = currentEvidence[nodeId] !== undefined;
            const maxLen = Object.keys(probs).reduce((m, s) => Math.max(m, s.length), 0);
            const lines = Object.entries(probs).map(([s, p]) =>
                `${s.padEnd(maxLen)} ${makeBar(p)} ${(p * 100).toFixed(1)}%`
            );
            const lbl = globalNodeData[nodeId]?.nodeLabel;
            const nodeColor = (lbl && labelColors[lbl]) ? labelColors[lbl] : '#ffffcc';
            return {
                id: nodeId,
                label: `${nodeId}\n${lines.join('\n')}`,
                color: isEv ? '#b0b0b0' : nodeColor
            };
        });
        nodesSet.update(updates);

        // Build ranked parent / child panels
        const childrenMap = buildChildrenMap();

        if (Object.keys(currentEvidence).length === 0) {
            hintsEl.innerHTML = '<i class="hint-text">(set evidence to see rankings)</i>';
        } else {
            // Collect min-hop distances to ancestors/descendants across all evidence nodes
            const ancestorDists = new Map();
            const descendantDists = new Map();
            for (const evNode of Object.keys(currentEvidence)) {
                for (const [node, dist] of getAncestors(evNode))
                    if (!ancestorDists.has(node) || ancestorDists.get(node) > dist)
                        ancestorDists.set(node, dist);
                for (const [node, dist] of getDescendants(evNode, childrenMap))
                    if (!descendantDists.has(node) || descendantDists.get(node) > dist)
                        descendantDists.set(node, dist);
            }

            const parentRanked = [], childRanked = [];
            for (const [nodeId, probs] of Object.entries(posteriors)) {
                if (nodeId in currentEvidence) continue;
                const entry = toDisplayEntry(nodeId, probs);
                if (ancestorDists.has(nodeId))
                    parentRanked.push({ ...entry, distance: ancestorDists.get(nodeId) });
                else if (descendantDists.has(nodeId))
                    childRanked.push({ ...entry, distance: descendantDists.get(nodeId) });
            }
            // Sort: closest first, then by probability descending within same depth
            parentRanked.sort((a, b) => a.distance - b.distance || b.displayProb - a.displayProb);
            childRanked.sort((a, b) => a.distance - b.distance || b.displayProb - a.displayProb);

            hintsEl.innerHTML = `
                <div class="rank-section">
                    <h5>↑ Ranked Parents (causes)</h5>
                    ${renderRanked(parentRanked, 'rank-bar-parent')}
                </div>
                <div class="rank-section">
                    <h5>↓ Ranked Children (effects)</h5>
                    ${renderRanked(childRanked, 'rank-bar-child')}
                </div>`;
        }

    } catch (e) {
        logEl.innerText = formatEvidence(currentEvidence) + '\n\n⚠️ Network error:\n' + e.message;
        hintsEl.innerHTML = '<i style="color:red">⚠️ Inference failed.</i>';
    }
}
