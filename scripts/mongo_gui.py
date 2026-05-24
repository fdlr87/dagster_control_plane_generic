#!/usr/bin/env python3
"""
MongoDB GUI — Standalone web interface for managing collections and documents.
Runs outside Docker, connects to MongoDB on localhost:27017.

Usage:
    pip install flask pymongo
    python scripts/mongo_gui.py
    Open: http://localhost:5050
"""

import json
import os
from datetime import datetime

from bson import ObjectId
from flask import Flask, jsonify, render_template_string, request
from pymongo import MongoClient

MONGO_URI = os.environ.get(
    "MONGODB_URI",
    "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true",
)
DATABASE = os.environ.get("MONGODB_DATABASE", "app_db")
PORT = int(os.environ.get("GUI_PORT", "5050"))

client = MongoClient(MONGO_URI)
db = client[DATABASE]

app = Flask(__name__)

# ── JSON serializer for MongoDB docs ──────────────────────────────────────────

def _serialize(doc):
    """Convert MongoDB document to JSON-safe dict."""
    if doc is None:
        return None
    result = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            result[k] = str(v)
        elif isinstance(v, datetime):
            result[k] = v.isoformat()
        elif isinstance(v, dict):
            result[k] = _serialize(v)
        elif isinstance(v, list):
            result[k] = [_serialize(i) if isinstance(i, dict) else str(i) if isinstance(i, ObjectId) else i for i in v]
        else:
            result[k] = v
    return result


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/collections")
def list_collections():
    names = db.list_collection_names()
    result = []
    for name in sorted(names):
        result.append({"name": name, "count": db[name].count_documents({})})
    return jsonify(result)


@app.post("/api/collections")
def create_collection():
    data = request.json
    name = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Name required"}), 400
    if name in db.list_collection_names():
        return jsonify({"error": f"Collection '{name}' already exists"}), 409
    db.create_collection(name)
    return jsonify({"ok": True, "name": name})


@app.delete("/api/collections/<name>")
def drop_collection(name):
    db.drop_collection(name)
    return jsonify({"ok": True})


@app.get("/api/collections/<name>/documents")
def list_documents(name):
    limit = int(request.args.get("limit", 50))
    skip = int(request.args.get("skip", 0))
    query_str = request.args.get("query", "{}")
    try:
        query = json.loads(query_str)
    except Exception:
        query = {}
    total = db[name].count_documents(query)
    docs = [_serialize(d) for d in db[name].find(query).skip(skip).limit(limit)]
    return jsonify({"total": total, "docs": docs})


@app.post("/api/collections/<name>/documents")
def insert_document(name):
    data = request.json
    if not data:
        return jsonify({"error": "Body required"}), 400
    # Remove _id if empty so Mongo generates it
    data.pop("_id", None)
    result = db[name].insert_one(data)
    return jsonify({"ok": True, "inserted_id": str(result.inserted_id)})


@app.put("/api/collections/<name>/documents/<doc_id>")
def update_document(name, doc_id):
    data = request.json
    data.pop("_id", None)
    result = db[name].replace_one({"_id": ObjectId(doc_id)}, data)
    return jsonify({"ok": True, "matched": result.matched_count})


@app.delete("/api/collections/<name>/documents/<doc_id>")
def delete_document(name, doc_id):
    db[name].delete_one({"_id": ObjectId(doc_id)})
    return jsonify({"ok": True})


# ── HTML frontend ─────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MongoDB GUI — Data Control Plane</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

  :root {
    --bg: #0d1117; --surface: #161b22; --surface2: #1c2128; --surface3: #22272e;
    --border: #30363d; --text: #e6edf3; --muted: #7d8590; --green: #3fb950;
    --blue: #58a6ff; --orange: #f0883e; --red: #f85149; --purple: #bc8cff;
    --yellow: #e3b341;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Inter', sans-serif; background: var(--bg); color: var(--text); display: flex; height: 100vh; overflow: hidden; }

  /* Sidebar */
  #sidebar { width: 280px; min-width: 280px; background: var(--surface); border-right: 1px solid var(--border); display: flex; flex-direction: column; }
  #sidebar-header { padding: 20px; border-bottom: 1px solid var(--border); }
  #sidebar-header h1 { font-size: 14px; font-weight: 700; color: var(--green); letter-spacing: 0.5px; }
  #sidebar-header p { font-size: 11px; color: var(--muted); margin-top: 4px; }
  #db-badge { display: inline-block; margin-top: 8px; background: var(--surface3); border: 1px solid var(--border); border-radius: 4px; padding: 2px 8px; font-size: 11px; color: var(--blue); font-family: 'JetBrains Mono', monospace; }

  #coll-list { flex: 1; overflow-y: auto; padding: 8px; }
  .coll-item { display: flex; align-items: center; justify-content: space-between; padding: 8px 10px; border-radius: 6px; cursor: pointer; transition: background 0.15s; margin-bottom: 2px; }
  .coll-item:hover { background: var(--surface2); }
  .coll-item.active { background: var(--surface3); border: 1px solid var(--border); }
  .coll-name { font-size: 13px; font-weight: 500; }
  .coll-count { font-size: 11px; color: var(--muted); background: var(--surface3); padding: 1px 6px; border-radius: 10px; }
  .coll-del { opacity: 0; font-size: 11px; color: var(--red); cursor: pointer; padding: 2px 6px; border-radius: 4px; }
  .coll-item:hover .coll-del { opacity: 1; }

  #new-coll-form { padding: 12px; border-top: 1px solid var(--border); display: flex; gap: 6px; }
  #new-coll-form input { flex: 1; background: var(--surface2); border: 1px solid var(--border); border-radius: 6px; padding: 7px 10px; color: var(--text); font-size: 12px; outline: none; }
  #new-coll-form input:focus { border-color: var(--blue); }
  .btn { background: var(--blue); color: #000; border: none; border-radius: 6px; padding: 7px 14px; font-size: 12px; font-weight: 600; cursor: pointer; transition: opacity 0.15s; white-space: nowrap; }
  .btn:hover { opacity: 0.85; }
  .btn-green { background: var(--green); }
  .btn-red { background: var(--red); }
  .btn-ghost { background: var(--surface3); color: var(--text); border: 1px solid var(--border); }

  /* Main panel */
  #main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
  #toolbar { padding: 16px 20px; border-bottom: 1px solid var(--border); display: flex; align-items: center; gap: 12px; background: var(--surface); }
  #toolbar h2 { font-size: 15px; font-weight: 600; flex: 1; }
  #toolbar span { font-size: 12px; color: var(--muted); }
  #filter-bar { padding: 10px 20px; background: var(--surface2); border-bottom: 1px solid var(--border); display: flex; gap: 8px; }
  #filter-bar input { flex: 1; background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: 7px 12px; color: var(--text); font-size: 12px; font-family: 'JetBrains Mono', monospace; outline: none; }
  #filter-bar input:focus { border-color: var(--blue); }

  #doc-list { flex: 1; overflow-y: auto; padding: 12px 20px; }
  .doc-card { background: var(--surface); border: 1px solid var(--border); border-radius: 8px; margin-bottom: 8px; overflow: hidden; transition: border-color 0.15s; }
  .doc-card:hover { border-color: var(--blue); }
  .doc-header { padding: 10px 14px; display: flex; align-items: center; gap: 10px; cursor: pointer; background: var(--surface2); }
  .doc-id { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: var(--purple); flex: 1; }
  .doc-preview { font-size: 11px; color: var(--muted); flex: 2; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .doc-actions { display: flex; gap: 6px; }
  .doc-body { padding: 14px; display: none; }
  .doc-body.open { display: block; }
  .doc-body textarea { width: 100%; background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: 10px; color: var(--text); font-family: 'JetBrains Mono', monospace; font-size: 12px; min-height: 120px; resize: vertical; outline: none; line-height: 1.5; }
  .doc-body textarea:focus { border-color: var(--blue); }
  .doc-body-actions { display: flex; gap: 8px; margin-top: 8px; justify-content: flex-end; }

  /* Empty state */
  #empty-state { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 12px; color: var(--muted); }
  #empty-state svg { opacity: 0.3; }

  /* Insert panel */
  #insert-panel { border-top: 1px solid var(--border); padding: 16px 20px; background: var(--surface); }
  #insert-panel summary { cursor: pointer; font-size: 13px; font-weight: 600; color: var(--green); user-select: none; }
  #insert-panel summary:hover { color: var(--text); }
  #insert-panel .insert-body { padding-top: 12px; display: flex; gap: 10px; }
  #insert-panel textarea { flex: 1; background: var(--bg); border: 1px solid var(--border); border-radius: 6px; padding: 10px; color: var(--text); font-family: 'JetBrains Mono', monospace; font-size: 12px; height: 100px; resize: none; outline: none; }
  #insert-panel textarea:focus { border-color: var(--green); }

  /* Toast */
  #toast { position: fixed; bottom: 24px; right: 24px; padding: 10px 18px; border-radius: 8px; font-size: 13px; font-weight: 500; opacity: 0; transition: opacity 0.3s; z-index: 999; pointer-events: none; }
  #toast.show { opacity: 1; }
  #toast.ok { background: var(--green); color: #000; }
  #toast.err { background: var(--red); color: #fff; }

  /* Pagination */
  #pagination { padding: 10px 20px; border-top: 1px solid var(--border); display: flex; align-items: center; gap: 10px; background: var(--surface2); }
  #pagination span { font-size: 12px; color: var(--muted); flex: 1; }

  ::-webkit-scrollbar { width: 6px; } ::-webkit-scrollbar-track { background: transparent; } ::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }
</style>
</head>
<body>

<!-- Sidebar -->
<div id="sidebar">
  <div id="sidebar-header">
    <h1>🍃 MongoDB GUI</h1>
    <p>Data Control Plane</p>
    <div id="db-badge">{{ DATABASE }}</div>
  </div>
  <div id="coll-list" id="coll-list"></div>
  <div id="new-coll-form">
    <input id="new-coll-input" placeholder="Nueva colección..." onkeydown="if(event.key==='Enter')createCollection()">
    <button class="btn btn-green" onclick="createCollection()">＋</button>
  </div>
</div>

<!-- Main -->
<div id="main">
  <div id="toolbar">
    <h2 id="coll-title">← Selecciona una colección</h2>
    <span id="doc-count"></span>
    <button class="btn btn-ghost" onclick="refreshDocs()" title="Actualizar">↺ Refresh</button>
  </div>

  <div id="filter-bar">
    <input id="filter-input" placeholder='Filtro MongoDB JSON, ej: {"status": "active"}' onkeydown="if(event.key==='Enter')applyFilter()">
    <button class="btn btn-ghost" onclick="applyFilter()">Filtrar</button>
    <button class="btn btn-ghost" onclick="document.getElementById('filter-input').value='';applyFilter()">✕</button>
  </div>

  <div id="doc-list">
    <div id="empty-state">
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5v14a9 3 0 0018 0V5"/><path d="M3 12a9 3 0 0018 0"/></svg>
      <p>Selecciona una colección para ver documentos</p>
    </div>
  </div>

  <div id="pagination" style="display:none">
    <button class="btn btn-ghost" id="btn-prev" onclick="changePage(-1)">← Anterior</button>
    <span id="page-info"></span>
    <button class="btn btn-ghost" id="btn-next" onclick="changePage(1)">Siguiente →</button>
  </div>

  <details id="insert-panel" style="display:none">
    <summary>＋ Insertar documento</summary>
    <div class="insert-body">
      <textarea id="insert-json" placeholder='{"campo": "valor", "numero": 42}'>{}</textarea>
      <button class="btn btn-green" onclick="insertDocument()" style="align-self:flex-end">Insertar</button>
    </div>
  </details>
</div>

<div id="toast"></div>

<script>
const DATABASE = "{{ DATABASE }}";
let currentColl = null;
let currentPage = 0;
const PAGE_SIZE = 50;
let currentTotal = 0;

// ── Toast ──────────────────────────────────────────────────────────────────
function toast(msg, type='ok') {
  const t = document.getElementById('toast');
  t.textContent = msg; t.className = 'show ' + type;
  setTimeout(() => t.className = '', 2500);
}

// ── Collections ────────────────────────────────────────────────────────────
async function loadCollections() {
  const res = await fetch('/api/collections');
  const cols = await res.json();
  const el = document.getElementById('coll-list');
  el.innerHTML = '';
  cols.forEach(c => {
    const div = document.createElement('div');
    div.className = 'coll-item' + (c.name === currentColl ? ' active' : '');
    div.innerHTML = `
      <span class="coll-name">${c.name}</span>
      <span class="coll-count">${c.count.toLocaleString()}</span>
      <span class="coll-del" onclick="event.stopPropagation();dropCollection('${c.name}')" title="Eliminar colección">🗑</span>
    `;
    div.onclick = () => selectCollection(c.name);
    el.appendChild(div);
  });
}

async function createCollection() {
  const input = document.getElementById('new-coll-input');
  const name = input.value.trim();
  if (!name) return;
  const res = await fetch('/api/collections', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({name}) });
  const data = await res.json();
  if (data.error) { toast(data.error, 'err'); return; }
  input.value = '';
  toast(`Colección '${name}' creada ✓`);
  await loadCollections();
  selectCollection(name);
}

async function dropCollection(name) {
  if (!confirm(`¿Eliminar colección '${name}'? Esta acción no se puede deshacer.`)) return;
  await fetch(`/api/collections/${name}`, { method: 'DELETE' });
  toast(`Colección '${name}' eliminada`, 'err');
  if (currentColl === name) { currentColl = null; document.getElementById('doc-list').innerHTML = '<div id="empty-state" style="flex:1;display:flex;flex-direction:column;align-items:center;justify-content:center;gap:12px;color:var(--muted);padding:60px">Colección eliminada</div>'; }
  await loadCollections();
}

// ── Documents ──────────────────────────────────────────────────────────────
function selectCollection(name) {
  currentColl = name; currentPage = 0;
  document.getElementById('coll-title').textContent = name;
  document.getElementById('insert-panel').style.display = '';
  document.getElementById('filter-input').value = '';
  loadCollections();
  refreshDocs();
}

async function refreshDocs() {
  if (!currentColl) return;
  const filterStr = document.getElementById('filter-input').value.trim() || '{}';
  let query = {};
  try { query = JSON.parse(filterStr); } catch(e) { toast('JSON de filtro inválido', 'err'); return; }
  const skip = currentPage * PAGE_SIZE;
  const res = await fetch(`/api/collections/${currentColl}/documents?limit=${PAGE_SIZE}&skip=${skip}&query=${encodeURIComponent(JSON.stringify(query))}`);
  const data = await res.json();
  currentTotal = data.total;
  renderDocs(data.docs, data.total);
}

function applyFilter() { currentPage = 0; refreshDocs(); }

function renderDocs(docs, total) {
  const el = document.getElementById('doc-list');
  document.getElementById('doc-count').textContent = `${total.toLocaleString()} documentos`;
  const pag = document.getElementById('pagination');

  if (docs.length === 0) {
    el.innerHTML = '<div style="text-align:center;padding:60px;color:var(--muted)">No hay documentos. Inserta uno abajo ↓</div>';
    pag.style.display = 'none';
    return;
  }

  el.innerHTML = '';
  docs.forEach(doc => {
    const id = doc._id;
    const preview = Object.entries(doc).filter(([k]) => k !== '_id').slice(0,3).map(([k,v]) => `${k}: ${JSON.stringify(v)}`).join(' · ');
    const card = document.createElement('div');
    card.className = 'doc-card';
    card.innerHTML = `
      <div class="doc-header" onclick="toggleDoc('body-${id}', this)">
        <span class="doc-id">${id}</span>
        <span class="doc-preview">${preview}</span>
        <div class="doc-actions">
          <button class="btn btn-ghost" style="padding:3px 8px;font-size:11px" onclick="event.stopPropagation();editDoc('${id}')">✎ Editar</button>
          <button class="btn btn-red" style="padding:3px 8px;font-size:11px" onclick="event.stopPropagation();deleteDoc('${id}')">🗑</button>
        </div>
      </div>
      <div class="doc-body" id="body-${id}">
        <textarea id="ta-${id}">${JSON.stringify(doc, null, 2)}</textarea>
        <div class="doc-body-actions">
          <button class="btn btn-ghost" onclick="toggleDoc('body-${id}')">Cerrar</button>
          <button class="btn" onclick="saveDoc('${id}')">Guardar cambios</button>
        </div>
      </div>
    `;
    el.appendChild(card);
  });

  // Pagination
  const totalPages = Math.ceil(total / PAGE_SIZE);
  pag.style.display = totalPages > 1 ? '' : 'none';
  document.getElementById('page-info').textContent = `Página ${currentPage+1} de ${totalPages} · ${total.toLocaleString()} docs`;
  document.getElementById('btn-prev').disabled = currentPage === 0;
  document.getElementById('btn-next').disabled = currentPage >= totalPages - 1;
}

function toggleDoc(id) {
  const el = document.getElementById(id);
  el.classList.toggle('open');
}

function editDoc(id) {
  const el = document.getElementById('body-' + id);
  el.classList.add('open');
  document.getElementById('ta-' + id).focus();
}

async function saveDoc(id) {
  let data;
  try { data = JSON.parse(document.getElementById('ta-' + id).value); } catch(e) { toast('JSON inválido', 'err'); return; }
  const res = await fetch(`/api/collections/${currentColl}/documents/${id}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body: JSON.stringify(data) });
  const r = await res.json();
  if (r.ok) { toast('Documento actualizado ✓'); refreshDocs(); }
  else toast('Error al guardar', 'err');
}

async function deleteDoc(id) {
  if (!confirm('¿Eliminar este documento?')) return;
  await fetch(`/api/collections/${currentColl}/documents/${id}`, { method: 'DELETE' });
  toast('Documento eliminado', 'err');
  refreshDocs(); loadCollections();
}

async function insertDocument() {
  let data;
  try { data = JSON.parse(document.getElementById('insert-json').value); } catch(e) { toast('JSON inválido', 'err'); return; }
  const res = await fetch(`/api/collections/${currentColl}/documents`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(data) });
  const r = await res.json();
  if (r.ok) {
    toast(`Insertado: ${r.inserted_id} ✓`);
    document.getElementById('insert-json').value = '{}';
    refreshDocs(); loadCollections();
  } else toast(r.error || 'Error', 'err');
}

function changePage(delta) {
  const totalPages = Math.ceil(currentTotal / PAGE_SIZE);
  currentPage = Math.max(0, Math.min(totalPages - 1, currentPage + delta));
  refreshDocs();
}

// ── Init ───────────────────────────────────────────────────────────────────
loadCollections();
setInterval(loadCollections, 10000); // Refresh sidebar counts every 10s
</script>
</body>
</html>
"""


@app.get("/")
def index():
    return render_template_string(HTML, DATABASE=DATABASE)


if __name__ == "__main__":
    print(f"\n🍃 MongoDB GUI arrancando...")
    print(f"   DB:   {DATABASE}")
    print(f"   URI:  {MONGO_URI[:40]}...")
    print(f"   URL:  http://localhost:{PORT}\n")
    app.run(host="0.0.0.0", port=PORT, debug=False)
