function generateCPTTable(existingCpt = null) {
    const statesRaw = document.getElementById('n-states').value;
    const parentsRaw = document.getElementById('n-parents').value;
    if (!statesRaw.trim()) {
        document.getElementById('cpt-table-container').innerHTML = "<i>Enter states...</i>";
        return;
    }

    const states = statesRaw.split(',').map(s => s.trim()).filter(s => s);
    const parents = parentsRaw ? parentsRaw.split(',').map(s => s.trim()).filter(s => s) : [];
    const parentStatesList = parents.map(p => globalNodeData[p] ? globalNodeData[p].states : ['?']);
    const combos = parents.length > 0 ? cartesian(...parentStatesList) : [[]];
    const nCombos = combos.length;

    let html = '<table>';

    if (parents.length === 0) {
        // No parents: simple prior
        html += '<tr><th>State</th><th>(Prior)</th></tr>';
    } else {
        // One header row per parent, with colspan grouping
        const cardinalities = parentStatesList.map(s => s.length);
        parents.forEach((parent, i) => {
            // span = product of cardinalities of all parents after i
            let span = 1;
            for (let j = i + 1; j < parents.length; j++) span *= cardinalities[j];
            // repeats = how many times the pattern for this parent cycles
            const repeats = nCombos / (cardinalities[i] * span);

            html += `<tr><th style="text-align:left; background:#c8d8e8; font-style:italic;">${parent}</th>`;
            for (let rep = 0; rep < repeats; rep++) {
                parentStatesList[i].forEach(s => {
                    html += `<th colspan="${span}">${s}</th>`;
                });
            }
            html += '</tr>';
        });
    }

    // Data rows: one row per child state
    states.forEach((state, r) => {
        html += `<tr><td><b>${state}</b></td>`;
        combos.forEach((_, c) => {
            const val = (existingCpt && existingCpt[r] && existingCpt[r][c] !== undefined) ? existingCpt[r][c] : '';
            html += `<td><input type="number" step="0.01" class="cpt-input" data-r="${r}" data-c="${c}" value="${val}" oninput="updateCPTSums()"></td>`;
        });
        html += '</tr>';
    });

    // Σ footer row — shows each column's current sum, colour-coded
    html += '<tr><td style="font-style:italic; color:#777; font-size:11px;">Σ</td>';
    for (let c = 0; c < nCombos; c++) {
        let sum = 0;
        if (existingCpt) {
            for (let r = 0; r < states.length; r++) sum += (existingCpt[r]?.[c] ?? 0);
        }
        const ok  = existingCpt ? Math.abs(sum - 1.0) < 0.001 : null;
        const txt = existingCpt ? sum.toFixed(3) : '—';
        const col = ok === null ? '#aaa' : (ok ? '#2a7' : '#c00');
        const fw  = ok === false ? 'bold' : 'normal';
        html += `<td id="cpt-sum-${c}" style="font-size:11px; color:${col}; font-weight:${fw}">${txt}</td>`;
    }
    html += '</tr>';

    html += '</table>';
    document.getElementById('cpt-table-container').innerHTML = html;
}

function updateCPTSums() {
    const inputs0 = document.querySelectorAll('.cpt-input[data-r="0"]');
    inputs0.forEach((_, c) => {
        let sum = 0, r = 0, inp;
        while ((inp = document.querySelector(`.cpt-input[data-r="${r}"][data-c="${c}"]`))) {
            sum += parseFloat(inp.value) || 0;
            r++;
        }
        const cell = document.getElementById(`cpt-sum-${c}`);
        if (!cell) return;
        const ok = Math.abs(sum - 1.0) < 0.001;
        cell.textContent      = sum.toFixed(3);
        cell.style.color      = ok ? '#2a7' : '#c00';
        cell.style.fontWeight = ok ? 'normal' : 'bold';
    });
}

function autoFillCPT(gateType) {
    const statesRaw = document.getElementById('n-states').value;
    const parentsRaw = document.getElementById('n-parents').value;
    const states = statesRaw.split(',').map(s => s.trim()).filter(s => s);
    const parents = parentsRaw ? parentsRaw.split(',').map(s => s.trim()).filter(s => s) : [];

    if (states.length !== 2) {
        alert('Auto-fill currently only supports binary (2-state) child nodes.');
        return;
    }
    if (parents.length === 0) {
        alert('Auto-fill requires at least one parent.');
        return;
    }

    const parentStatesList = parents.map(p => globalNodeData[p] ? globalNodeData[p].states : []);
    if (parentStatesList.some(ps => ps.length === 0)) {
        alert('One or more parent nodes not found. Save the parent nodes first.');
        return;
    }

    const combos = cartesian(...parentStatesList);

    combos.forEach((combo, c) => {
        const flatCombo = Array.isArray(combo) ? combo : [combo];

        // For each parent: first state = "positive" (True/Yes), last state = "negative" (False/No)
        let childPositive;
        if (gateType === 'OR') {
            // Child first-state if at least one parent is in its first (positive) state
            childPositive = flatCombo.some((s, i) => s === parentStatesList[i][0]);
        } else {
            // AND: child first-state only if ALL parents are in their first (positive) state
            childPositive = flatCombo.every((s, i) => s === parentStatesList[i][0]);
        }

        const r0 = document.querySelector(`.cpt-input[data-r="0"][data-c="${c}"]`);
        const r1 = document.querySelector(`.cpt-input[data-r="1"][data-c="${c}"]`);
        if (r0) r0.value = childPositive ? 1 : 0;
        if (r1) r1.value = childPositive ? 0 : 1;
    });
    updateCPTSums();
}
