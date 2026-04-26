async function loadProjects() {
    const res = await fetch('http://localhost:8000/projects');
    const projects = await res.json();
    const select = document.getElementById('project-select');
    select.innerHTML = projects.map(p =>
        `<option value="${p}"${p === currentProject ? ' selected' : ''}>${p}</option>`
    ).join('');
    if (!projects.includes(currentProject)) {
        currentProject = projects[0] || 'default';
        select.value = currentProject;
    }
}

async function switchProject() {
    currentProject = document.getElementById('project-select').value;
    currentEvidence = {};
    labelColors = {};
    document.getElementById('evidence-log').innerText = '(none)';
    document.getElementById('inference-hints').innerHTML = '<i class="hint-text">(set evidence to see rankings)</i>';
    resetEditorPanel();
    await loadNetwork();
}

async function createProject() {
    const name = document.getElementById('new-project-name').value.trim();
    if (!name) { alert('Enter a project name first.'); return; }
    if (!/^[a-zA-Z0-9_-]+$/.test(name)) {
        alert('Project name may only contain letters, digits, _ or -.');
        return;
    }
    await fetch('http://localhost:8000/projects', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
    });
    document.getElementById('new-project-name').value = '';
    currentProject = name;
    await loadProjects();
    await loadNetwork();
}

async function deleteProject() {
    if (currentProject === 'default') { alert('Cannot delete the default project.'); return; }
    if (!confirm(`Delete project "${currentProject}" and all its data?`)) return;
    await fetch(`http://localhost:8000/projects/${encodeURIComponent(currentProject)}`, { method: 'DELETE' });
    currentProject = 'default';
    await loadProjects();
    await loadNetwork();
}
