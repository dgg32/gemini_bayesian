// XSS helper
function escHtml(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Cartesian product for parent state combinations
const cartesian = (...a) => a.reduce((a, b) => a.flatMap(d => b.map(e => [d, e].flat())));

// Bar chart helper
function makeBar(prob, width = 10) {
    const filled = Math.round(prob * width);
    return '█'.repeat(filled) + '░'.repeat(width - filled);
}

function formatEvidence(ev) {
    const entries = Object.entries(ev);
    return entries.length ? entries.map(([k, v]) => `${k}: ${v}`).join('\n') : '(none)';
}

// ── DAG cycle guard ──────────────────────────────────────────────────────────
// Returns true if assigning `newParents` to `nodeId` would introduce a cycle.
function wouldCreateCycle(nodeId, newParents) {
    if (newParents.includes(nodeId)) return true;   // self-loop

    // Build children map from the current canvas edges.
    // Exclude edges pointing TO nodeId (its current parents, which are being replaced).
    const children = {};
    edgesSet.get().forEach(e => {
        if (e.to === nodeId) return;
        if (!children[e.from]) children[e.from] = [];
        children[e.from].push(e.to);
    });

    // A cycle forms if any proposed parent is already a descendant of nodeId.
    const visited = new Set();
    const stack = [nodeId];
    while (stack.length > 0) {
        const curr = stack.pop();
        if (visited.has(curr)) continue;
        visited.add(curr);
        for (const child of (children[curr] || [])) {
            if (newParents.includes(child)) return true;
            stack.push(child);
        }
    }
    return false;
}
