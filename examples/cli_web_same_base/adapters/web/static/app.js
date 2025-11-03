document.addEventListener('DOMContentLoaded', () => {
  let currentPath = null;
  const sid = document.body.dataset.sid;
  if (!sid) return;

  const out = document.getElementById('job-output');
  const statusEl = document.getElementById('run-status');
  const treeEl = document.getElementById('file-tree');
  const viewer = document.getElementById('file-viewer');

  const badgeEl = document.getElementById('updated-badge');
  let badgeTimer = null;

  function flashUpdated() {
    // retrigger the CSS animation by toggling the class
    viewer.classList.remove('pulse');
    // force reflow to restart animation
    void viewer.offsetWidth;
    viewer.classList.add('pulse');

    // show the badge briefly
    badgeEl.hidden = false;
    badgeEl.classList.remove('updated');
    // force reflow so the animation restarts
    void badgeEl.offsetWidth;
    badgeEl.classList.add('updated');

    clearTimeout(badgeTimer);
    badgeTimer = setTimeout(() => {
        badgeEl.hidden = true;
        badgeEl.classList.remove('updated');
    }, 1800); // slightly longer than badge-fade
  }


  function appendPanel(title, body, color) {
    const wrapper = document.createElement('section');
    wrapper.className = `panel ${color ? 'panel-' + color : ''}`;
    wrapper.innerHTML = `
      <div class="panel-header">${title}</div>
      <div class="panel-body"><pre>${escapeHtml(body)}</pre></div>
    `;
    out.appendChild(wrapper);
    out.scrollTop = out.scrollHeight;
  }

  function appendCard(html) {
    const el = document.createElement('section');
    el.className = 'card';
    el.innerHTML = html;
    out.appendChild(el);
    out.scrollTop = out.scrollHeight;
  }

  function setStatus(text, cls) {
    statusEl.textContent = text;
    statusEl.className = `pill ${cls || ''}`;
  }

  function escapeHtml(s) {
    return s.replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
  }

  // SSE for live events
  const es = new EventSource(`/events/${sid}`);
  es.onmessage = (evt) => {
    const e = JSON.parse(evt.data);
    switch (e.type) {
      case 'run_started':
        setStatus('running', 'accent');
        appendCard(`<h3>Workspace</h3><pre>${e.workspace}</pre>`);
        break;
      case 'step_announced':
        appendCard(`<h3>Solving Step ${e.index}</h3><pre class="prompt">${escapeHtml(e.prompt.trim())}</pre>`);
        break;
      case 'step_result':
        appendPanel(e.title, e.body, 'orange');
        break;
      case 'run_summary_requested':
        appendCard(`<em>Summary requested (CLI renders Rich timing). Web summary TBD.</em>`);
        break;
      case 'run_completed':
        setStatus('done', 'success');
        break;
      case 'run_failed':
        setStatus('error', 'danger');
        appendPanel('Error', e.error, 'red');
        break;
      case 'tree_changed':
        loadTree();  // refresh the left pane
        break;

      case 'fs_modified':
        if (currentPath && e.path === currentPath) {
            const p = currentPath;
            const codeExts = ['.py','.json','.md','.txt','.js','.ts','.toml','.yaml','.yml','.sh','.cfg','.ini','.csv'];
            const wantsHtml = codeExts.some(ext => p.toLowerCase().endsWith(ext));
            fetch(`/api/file/${sid}?path=${encodeURIComponent(p)}&format=${wantsHtml ? 'html' : 'text'}`)
            .then(r => r.text())
            .then(body => {
                if (wantsHtml) viewer.innerHTML = body; else viewer.textContent = body;
                flashUpdated();           // 👈 pulse & badge
            });
        }
        break;

      case 'fs_created':
      case 'fs_deleted':
      case 'fs_moved':
        loadTree();  // conservative: refresh on structure changes
        break;    
    }
  };

  // Files pane: simple polling every 2s (can swap to SSE later)
  async function loadTree() {
    const res = await fetch(`/api/tree/${sid}`);
    const data = await res.json();
    treeEl.innerHTML = renderTree(data.children, '');
    // click handling
    treeEl.querySelectorAll('[data-path]').forEach(node => {
        node.addEventListener('click', async (ev) => {
            const p = ev.currentTarget.getAttribute('data-path');
            currentPath = p;  // remember selection

            const codeExts = ['.py','.json','.md','.txt','.js','.ts','.toml','.yaml','.yml','.sh','.cfg','.ini','.csv'];
            const wantsHtml = codeExts.some(ext => p.toLowerCase().endsWith(ext));
            const res = await fetch(`/api/file/${sid}?path=${encodeURIComponent(p)}&format=${wantsHtml ? 'html' : 'text'}`);
            const body = await res.text();
            if (wantsHtml) viewer.innerHTML = body; else viewer.textContent = body;
        });
    });


  }

  function renderTree(nodes, prefix) {
    if (!nodes || !nodes.length) return '<div class="muted">No files yet</div>';
    return '<ul class="tree">' + nodes.map(n => {
      if (n.type === 'dir') {
        return `<li><span class="dir">${n.name}</span>${renderTree(n.children, prefix + n.name + '/')}</li>`;
      } else {
        return `<li><button class="file" data-path="${prefix + n.name}">${n.name}</button></li>`;
      }
    }).join('') + '</ul>';
  }

  loadTree();
  // i dont think this is needed anymore
  // setInterval(loadTree, 2000);

  // Control pane: hooks for future actions
  document.getElementById('action-cancel')?.addEventListener('click', () => {
    alert('Cancel not implemented yet (engine needs a cancel hook).');
  });
});
