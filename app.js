/* LinkedIn Radar web app.
 *
 * A single page that reads the radar's cloud database (Turso) straight from the browser. There is no
 * server: the database address and key are entered once and stay in this browser's localStorage.
 * The page never writes the radar's own tables; buttons and edits go to `actions` and `search_specs`,
 * which the next radar run applies (the same way Telegram buttons work).
 */
"use strict";

const CONN_KEY = "radar.connection.v1";
const GH_KEY = "radar.github.v1";
const PREFS_KEY = "radar.prefs.v1";
const PAGE = 30;

// ── small helpers ────────────────────────────────────────────────────────────

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const json = (s, fallback) => {
  if (s == null || s === "") return fallback;
  if (typeof s !== "string") return s;
  try { return JSON.parse(s); } catch { return fallback; }
};
const num = (n) => (n == null ? "–" : Number(n).toLocaleString("en-US"));
const clip = (s, n) => { s = String(s ?? "").replace(/\s+/g, " ").trim(); return s.length > n ? s.slice(0, n - 1) + "…" : s; };
const safeUrl = (u) => (/^https:\/\//i.test(u || "") ? u : "");
const nowIso = () => new Date().toISOString().replace(/\.\d{3}Z$/, "+00:00");

function store(key, value) {
  try {
    if (value === undefined) return JSON.parse(localStorage.getItem(key) || "null");
    if (value === null) localStorage.removeItem(key); else localStorage.setItem(key, JSON.stringify(value));
  } catch { /* private window or blocked storage: the app still works for this visit */ }
  return value;
}

function ago(iso) {
  if (!iso) return "";
  const t = Date.parse(iso);
  if (Number.isNaN(t)) return "";
  const h = (Date.now() - t) / 3.6e6;
  if (h < 0) return "soon";
  if (h < 1) return `${Math.max(1, Math.round(h * 60))}m ago`;
  if (h < 48) return `${Math.round(h)}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

function when(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(+d)) return "";
  try {
    return new Intl.DateTimeFormat("en-GB", { timeZone: app.tz, weekday: "short", day: "numeric", month: "short",
                                              hour: "2-digit", minute: "2-digit" }).format(d);
  } catch { return d.toLocaleString(); }
}

function toast(msg, bad = false) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast show" + (bad ? " bad" : "");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { t.className = "toast"; }, 2600);
}

function sinceDays(days) {
  return new Date(Date.now() - days * 864e5).toISOString().replace(/\.\d{3}Z$/, "+00:00");
}

const SOURCE_NAMES = {
  tavily: "Tavily", exa: "Exa", linkup: "Linkup", serper: "Serper", brave: "Brave", searxng: "SearXNG",
  ddgs: "Web search (keyless)", "company-page": "LinkedIn company page", manual: "You sent it",
  "google-news": "Google News", "bing-news": "Bing News", "linkedin-post-page": "LinkedIn post pages",
};
function sourceName(s) {
  if (!s) return "unknown";
  if (s.startsWith("ddgs:")) return `Web search (${s.slice(5)})`;
  if (s.startsWith("rss:")) return `RSS: ${s.slice(4)}`;
  return SOURCE_NAMES[s] || s;
}

// ── database client (Turso HTTP API, POST /v2/pipeline) ──────────────────────

class AuthError extends Error {}

function httpUrl(u) {
  u = String(u || "").trim();
  for (const s of ["libsql://", "turso://", "wss://", "ws://"]) if (u.startsWith(s)) u = "https://" + u.slice(s.length);
  return u.replace(/\/+$/, "");
}

function val(v) {
  if (v == null) return { type: "null" };
  if (typeof v === "boolean") return { type: "integer", value: v ? "1" : "0" };
  if (typeof v === "number") return Number.isInteger(v) ? { type: "integer", value: String(v) } : { type: "float", value: v };
  return { type: "text", value: String(v) };
}
function unval(v) {
  switch (v.type) {
    case "null": return null;
    case "integer": return Number(v.value);
    case "float": return Number(v.value);
    case "blob": return null;
    default: return v.value;
  }
}

class DB {
  constructor(url, token) { this.url = httpUrl(url); this.token = token; }
  async run(stmts) {
    const body = { requests: stmts.map(([sql, args = []]) => ({ type: "execute", stmt: { sql, args: args.map(val) } })) };
    body.requests.push({ type: "close" });
    let r;
    try {
      r = await fetch(`${this.url}/v2/pipeline`, {
        method: "POST", body: JSON.stringify(body), referrerPolicy: "no-referrer",
        headers: { Authorization: `Bearer ${this.token}`, "Content-Type": "application/json" },
      });
    } catch (e) { throw new Error("Can't reach the database. Check your internet connection."); }
    if (r.status === 401 || r.status === 403) throw new AuthError("The database refused this key.");
    if (!r.ok) throw new Error(`Database error ${r.status}`);
    const data = await r.json();
    return data.results.slice(0, stmts.length).map((res) => {
      if (res.type !== "ok") throw new Error((res.error && res.error.message) || "Database error");
      const result = res.response.result;
      const cols = result.cols.map((c) => c.name);
      return result.rows.map((row) => Object.fromEntries(row.map((v, j) => [cols[j], unval(v)])));
    });
  }
  async q(sql, args = []) { return (await this.run([[sql, args]]))[0]; }
}

// ── app state ────────────────────────────────────────────────────────────────

const app = {
  db: null,
  tz: "Asia/Jerusalem",
  config: {},            // kv app_config, written by every radar run
  searches: [],          // rows of `searches` with parsed spec
  pending: new Map(),    // "item:12" / "match:5" → action value not yet applied by a run
  prefs: Object.assign({ feed: {}, people: {} }, store(PREFS_KEY) || {}),
};
const savePrefs = () => store(PREFS_KEY, app.prefs);

// The two tables only this page writes (the radar creates them too; this covers a brand-new database).
const APP_TABLES = [
  `CREATE TABLE IF NOT EXISTS actions (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT NOT NULL, kind TEXT NOT NULL,
     target TEXT, value TEXT, source TEXT DEFAULT 'web', applied_at TEXT, result TEXT)`,
  `CREATE TABLE IF NOT EXISTS search_specs (id TEXT PRIMARY KEY, spec TEXT NOT NULL, deleted INTEGER DEFAULT 0, updated_at TEXT)`,
];

async function loadBasics() {
  if (!app.tablesReady) { await app.db.run(APP_TABLES.map((sql) => [sql])); app.tablesReady = true; }
  const [kv, searches, pending, lastRun] = await app.db.run([
    ["SELECT key, value FROM kv WHERE key IN ('app_config','last_digest_slot','mute_until')"],
    ["SELECT * FROM searches ORDER BY added_at, id"],
    ["SELECT kind, target, value FROM actions WHERE applied_at IS NULL ORDER BY id"],
    ["SELECT id, status, started_at, finished_at, mode FROM runs ORDER BY id DESC LIMIT 1"],
  ]);
  const kvm = Object.fromEntries(kv.map((r) => [r.key, json(r.value, r.value)]));
  app.config = kvm.app_config || {};
  app.tz = (app.config.schedule && app.config.schedule.timezone) || "Asia/Jerusalem";
  app.kv = kvm;
  app.searches = searches.map((s) => ({ ...s, spec: json(s.spec, {}) }));
  app.pending = new Map();
  for (const a of pending) {
    if (a.kind === "item_feedback") app.pending.set(`item:${a.target}`, a.value);
    if (a.kind === "match_feedback") app.pending.set(`match:${a.target}`, a.value);
  }
  app.lastRun = lastRun[0] || null;
  renderStatus();
}

function renderStatus() {
  const r = app.lastRun;
  const el = $("#status");
  if (!r) { el.textContent = ""; return; }
  const icon = r.status === "ok" ? "●" : r.status === "running" ? "◐" : "▲";
  el.className = `status ${r.status}`;
  el.innerHTML = `<span class="dot">${icon}</span> Last run ${esc(ago(r.started_at))}`;
  el.title = `Run #${r.id} · ${r.mode} · ${r.status} · ${when(r.started_at)}`;
}

function searchName(id) {
  const s = app.searches.find((x) => x.id === id);
  return (s && s.name) || id;
}
function searchMode(id) {
  const s = app.searches.find((x) => x.id === id);
  return s && s.type === "event_attendees" ? "people" : "posts";
}

async function act(kind, target, value, key) {
  try {
    await app.db.run([["INSERT INTO actions(created_at, kind, target, value, source) VALUES(?,?,?,?, 'web')",
                       [nowIso(), kind, String(target), value]]]);
    if (key) app.pending.set(key, value);
    toast("Saved. The next radar run applies it.");
    return true;
  } catch (e) { toast(e.message, true); return false; }
}

// ── connect screen ───────────────────────────────────────────────────────────

function renderConnect(error = "") {
  $("#nav").hidden = true;
  $("#status").textContent = "";
  $("#view").innerHTML = `
  <section class="connect card">
    <h1>Connect your radar</h1>
    <p class="muted">This page has no data of its own. It reads your radar's cloud database directly, using the
      address and key you enter here. They are saved only in this browser.</p>
    ${error ? `<p class="error">${esc(error)}</p>` : ""}
    <label>Database address<input id="c-url" placeholder="libsql://linkedin-alerts-….turso.io" autocomplete="off" spellcheck="false"></label>
    <label>Database key<input id="c-token" type="password" placeholder="The token from Turso" autocomplete="off"></label>
    <button class="primary" id="c-go">Connect</button>
    <p class="hint">Fastest way: in Terminal run <code>~/.local/bin/uv run radar app-link</code> and open the link it prints.
      Or copy the two values from <a href="https://app.turso.tech" rel="noopener noreferrer" target="_blank">app.turso.tech</a>.</p>
  </section>`;
  $("#c-go").onclick = async () => {
    const url = $("#c-url").value.trim(), token = $("#c-token").value.trim();
    if (!url || !token) { renderConnect("Enter both the address and the key."); return; }
    await connect({ url, token });
  };
}

async function connect(conn) {
  const db = new DB(conn.url, conn.token);
  try {
    await db.q("SELECT COUNT(*) AS n FROM items");
  } catch (e) {
    renderConnect(e instanceof AuthError ? "That key was refused. Copy it again from Turso."
      : /no such table/i.test(e.message) ? "Connected, but this database has no radar data yet."
      : e.message);
    return false;
  }
  store(CONN_KEY, { url: db.url, token: conn.token });
  app.db = db;
  $("#nav").hidden = false;
  await loadBasics();
  if (!location.hash.startsWith("#/")) location.hash = "#/feed"; else route();
  return true;
}

function readConnectLink() {
  // #connect=<base64url of {"url":…,"token":…}>, printed by `radar app-link` in the user's own Terminal.
  const m = location.hash.match(/^#connect=([A-Za-z0-9_\-=]+)/);
  if (!m) return null;
  history.replaceState(null, "", location.pathname + "#/feed");   // the key never stays in the address bar
  try {
    const b64 = m[1].replace(/-/g, "+").replace(/_/g, "/");
    return JSON.parse(atob(b64 + "=".repeat((4 - (b64.length % 4)) % 4)));
  } catch { return null; }
}

// ── router ───────────────────────────────────────────────────────────────────

const ROUTES = {
  feed: renderFeed, people: renderPeople, searches: renderSearches, search: renderSearchEdit,
  runs: renderRuns, settings: renderSettings, item: renderDetail, match: renderDetail,
};

async function route() {
  if (!app.db) return;
  const [, name = "feed", ...rest] = location.hash.replace(/^#/, "").split("/");
  const fn = ROUTES[name] || renderFeed;
  for (const a of $$("#nav a")) a.classList.toggle("on", a.dataset.route === (name === "search" ? "searches" : name));
  const view = $("#view");
  if (route.last !== location.hash) window.scrollTo(0, 0);
  route.last = location.hash;
  try {
    await fn(view, rest.map(decodeURIComponent), name);
  } catch (e) {
    if (e instanceof AuthError) { store(CONN_KEY, null); app.db = null; renderConnect(e.message); return; }
    view.innerHTML = `<div class="card error">Couldn't load this page: ${esc(e.message)}</div>`;
  }
}
window.addEventListener("hashchange", () => {
  const link = location.hash.startsWith("#connect=") && readConnectLink();
  if (link) connect(link); else route();
});

// ── shared card for posts, news and saved-search matches ─────────────────────

const ROW_COLS = `kind, url, title, snippet, text, text_complete, author_name, author_handle, author_url,
  author_followers, likes, comments_count, published_at`;

function badge(score) {
  if (score == null) return `<span class="score none">–</span>`;
  const cls = score >= 80 ? "hot" : score >= 60 ? "good" : score >= 40 ? "ok" : "low";
  return `<span class="score ${cls}" title="Score out of 100">${score}</span>`;
}

function feedbackButtons(r) {
  const isMatch = r.rtype === "match";
  const key = `${isMatch ? "match" : "item"}:${r.rid}`;
  const chosen = app.pending.get(key) || feedbackCode(r.feedback, isMatch);
  const people = isMatch && searchMode(r.search_id) === "people";
  const b = (code, label) => `<button class="fb${chosen === code ? " on" : ""}" data-act="${code}" data-key="${key}"
     data-kind="${isMatch ? "match_feedback" : "item_feedback"}" data-target="${r.rid}">${label}</button>`;
  const p = isMatch ? "m" : "";
  let out = b(p + "u", "👍 Useful") + b(p + "n", "👎 Not relevant");
  if (r.kind !== "news") out += b(p + "c", people ? "✅ Reached out" : "✅ I commented");
  if (r.author_handle && r.kind !== "news") out += b(p + "m", "🙈 Mute author");
  return out;
}

function feedbackCode(feedback, isMatch) {
  const p = isMatch ? "m" : "";
  return { useful: p + "u", not_relevant: p + "n", commented: p + "c", contacted: p + "c" }[feedback] || "";
}

function draft(label, text) {
  if (!text) return "";
  return `<div class="draft"><div class="draft-head"><span>${label}</span>
    <button class="copy" data-copy="${esc(text)}">Copy</button></div><p>${esc(text)}</p></div>`;
}

function attendance(a) {
  const label = { exhibiting: "Exhibiting", speaking: "Speaking", meetings: "Taking meetings", attending: "Attending" }[a.attending_as] || "";
  return [label, a.stand && `stand ${a.stand}`, a.dates, a.invites_meetings && a.attending_as !== "meetings" && "invites meetings"]
    .filter(Boolean).join(" · ");
}

function card(r, { compact = false, full = false } = {}) {
  const a = json(r.analysis, {}) || {};
  const isMatch = r.rtype === "match";
  const people = isMatch && searchMode(r.search_id) === "people";
  const score = r.score ?? (r.stage === "filtered" ? null : r.triage_score);
  const kind = isMatch ? searchName(r.search_id)
    : { post: "LinkedIn post", article: "LinkedIn article", news: "News", manual: "Your link" }[r.kind] || r.kind;
  const sent = r.notified_at ? "Sent to Telegram" : r.digest ? "In a digest" : "";
  const who = people ? (a.person || r.author_name || a.company || "Unknown")
    : (r.kind === "news" ? (r.source_label || "") : (r.author_name || r.author_handle || "Unknown author"));
  const role = people ? [a.role, a.company && !(a.role || "").includes(a.company) ? a.company : ""].filter(Boolean).join(", ")
    : a.author_role || "";
  const meta = [];
  if (r.author_followers) meta.push(`${num(r.author_followers)} followers`);
  if (r.likes != null) meta.push(`${num(r.likes)} likes`);
  if (r.comments_count != null) meta.push(`${num(r.comments_count)} comments`);
  if (people && a.company_type && a.company_type !== "other") meta.push(a.company_type.replace(/_/g, " "));
  const body = r.text || r.snippet || r.title || "";
  const long = !full && body.length > 320;
  const sources = (Array.isArray(r.sources) ? r.sources : json(r.sources, null) || (r.sources ? [r.sources] : []));
  const queries = json(r.queries, []) || [];
  const url = safeUrl(r.url);
  const detail = `#/${isMatch ? "match" : "item"}/${r.rid}`;
  return `<article class="card item${r.stage === "filtered" ? " filtered" : ""}">
    <div class="item-head">${badge(score)}<span class="kind">${esc(kind)}</span>
      <span class="muted">${esc(ago(r.published_at) || "date unknown")}</span>
      ${sent ? `<span class="chip tg">${sent}</span>` : ""}
      <a class="more" href="${detail}" title="Details">Details</a></div>
    <div class="who"><strong>${r.author_url && !people ? `<a href="${esc(safeUrl(r.author_url))}" target="_blank" rel="noopener noreferrer">${esc(who)}</a>` : esc(who)}</strong>
      ${role ? `<span class="role">${esc(clip(role, 120))}</span>` : ""}</div>
    ${meta.length ? `<div class="muted small">${esc(meta.join(" · "))}</div>` : ""}
    ${people && attendance(a) ? `<div class="attend">📍 ${esc(attendance(a))}</div>` : ""}
    ${r.kind === "news" && r.title ? `<p class="title">${esc(r.title)}</p>` : ""}
    <p class="body${long ? " clamp" : ""}">${esc(r.kind === "news" ? (r.snippet || "") : body)}</p>
    ${long ? `<button class="link toggle">Show more</button>` : ""}
    ${r.stage === "filtered" ? `<p class="why muted">Filtered out: ${esc(r.filter_reason || "")}</p>` : ""}
    ${a.why || a.so_what ? `<p class="why"><b>Why:</b> ${esc(a.why || a.so_what)}</p>` : (r.triage_reason ? `<p class="why muted">${esc(r.triage_reason)}</p>` : "")}
    ${a.angle ? `<p class="why"><b>Your angle:</b> ${esc(a.angle)}</p>` : ""}
    ${a.post_idea ? `<p class="why"><b>Post idea:</b> ${esc(a.post_idea)}</p>` : ""}
    ${a.contacts && a.contacts.length ? `<p class="why"><b>Also going:</b> ${esc(a.contacts.join(", "))}</p>` : ""}
    ${compact ? "" : draft("Comment", a.comment || a.comment_expert) + draft("Question comment", a.comment_question) + draft("Connection note", a.connect_note)}
    ${a.qa_problems && a.qa_problems.length ? `<p class="warn">Draft may need a touch-up: ${esc(a.qa_problems.slice(0, 2).join("; "))}</p>` : ""}
    <div class="foot">
      ${url ? `<a class="btn" href="${esc(url)}" target="_blank" rel="noopener noreferrer">Open ${r.kind === "news" ? "article" : "post"} ↗</a>` : ""}
      ${r.stage === "filtered" ? "" : feedbackButtons(r)}
    </div>
    <div class="muted small src">Found by ${esc(sources.map(sourceName).join(", ") || "unknown")}
      ${queries.length ? ` · “${esc(clip(queries[0].replace(/^(company|author|feed):/, ""), 60))}”${queries.length > 1 ? ` +${queries.length - 1}` : ""}` : ""}
      · ${esc(ago(r.found_at))}</div>
  </article>`;
}

function wireCards(root) {
  for (const t of $$(".toggle", root)) t.onclick = () => {
    const p = t.previousElementSibling;
    p.classList.toggle("clamp");
    t.textContent = p.classList.contains("clamp") ? "Show more" : "Show less";
  };
  for (const c of $$(".copy", root)) c.onclick = async () => {
    try { await navigator.clipboard.writeText(c.dataset.copy); toast("Copied"); }
    catch { toast("Copy didn't work in this browser. Select the text instead.", true); }
  };
  for (const b of $$(".fb", root)) b.onclick = async () => {
    if (await act(b.dataset.kind, b.dataset.target, b.dataset.act, b.dataset.key)) {
      for (const x of $$(`.fb[data-key="${b.dataset.key}"]`, root)) x.classList.toggle("on", x === b);
    }
  };
}

// ── feed ─────────────────────────────────────────────────────────────────────

const FEED_DEFAULTS = { search: "all", status: "open", source: "", min: 0, days: 7, q: "", sort: "new" };

function feedQuery(f, page) {
  const parts = [], args = [];
  const since = f.days ? sinceDays(f.days) : null;
  const like = f.q ? `%${f.q}%` : null;
  const itemCols = `'item' AS rtype, i.id AS rid, NULL AS search_id, ${ROW_COLS.replace(/(\w+)/g, "i.$1")},
    i.discovered_at AS found_at, i.source AS sources, i.source AS source_label, i.queries, i.stage, i.filter_reason, i.score,
    i.triage_score, i.triage_reason, i.analysis, i.notified_at, i.digest_date AS digest, i.feedback`;
  const matchCols = `'match' AS rtype, m.id AS rid, m.search_id, ${ROW_COLS.replace(/(\w+)/g, "i.$1")},
    m.found_at, m.sources, i.source AS source_label, m.queries, m.stage, m.filter_reason, m.score,
    m.triage_score, m.triage_reason, m.analysis, m.notified_at, m.digest_slot AS digest, m.feedback`;
  const stageCond = (t) => ({
    open: `${t}.stage = 'analyzed' AND ${t}.feedback IS NULL`,
    rated: `${t}.feedback IS NOT NULL`,
    all: `${t}.stage IN ('analyzed','triaged')`,
    filtered: `${t}.stage = 'filtered'`,
  }[f.status] || `${t}.stage = 'analyzed'`);
  const savedIds = app.searches.map((s) => s.id);
  if (["all", "hot", "news"].includes(f.search)) {
    const w = ["i.stage <> 'search'", stageCond("i")];
    if (f.search === "hot") w.push("i.kind <> 'news'");
    if (f.search === "news") w.push("i.kind = 'news'");
    if (since) { w.push("i.discovered_at >= ?"); args.push(since); }
    if (f.min) { w.push("COALESCE(i.score, i.triage_score, 0) >= ?"); args.push(Number(f.min)); }
    if (f.source) { w.push(f.source === "ddgs" ? "i.source LIKE 'ddgs%'" : "i.source = ?"); if (f.source !== "ddgs") args.push(f.source); }
    if (like) { w.push("(i.title LIKE ? OR i.text LIKE ? OR i.snippet LIKE ? OR i.author_name LIKE ?)"); args.push(like, like, like, like); }
    parts.push(`SELECT ${itemCols} FROM items i WHERE ${w.join(" AND ")}`);
  }
  if (f.search === "all" || savedIds.includes(f.search)) {
    const w = [stageCond("m")];
    if (f.search !== "all") { w.push("m.search_id = ?"); args.push(f.search); }
    if (since) { w.push("m.found_at >= ?"); args.push(since); }
    if (f.min) { w.push("COALESCE(m.score, m.triage_score, 0) >= ?"); args.push(Number(f.min)); }
    if (f.source) { w.push("m.sources LIKE ?"); args.push(`%"${f.source}%`); }
    if (like) { w.push("(i.title LIKE ? OR i.text LIKE ? OR i.snippet LIKE ? OR i.author_name LIKE ? OR m.analysis LIKE ?)"); args.push(like, like, like, like, like); }
    parts.push(`SELECT ${matchCols} FROM matches m JOIN items i ON i.id = m.item_id WHERE ${w.join(" AND ")}`);
  }
  const order = f.sort === "score" ? "COALESCE(score, triage_score, 0) DESC, found_at DESC" : "found_at DESC";
  return [`SELECT * FROM (${parts.join(" UNION ALL ")}) ORDER BY ${order} LIMIT ${PAGE + 1} OFFSET ${page * PAGE}`, args];
}

async function renderFeed(view) {
  const f = Object.assign({}, FEED_DEFAULTS, app.prefs.feed);
  const sources = await app.db.q(
    "SELECT source, COUNT(*) AS n FROM items WHERE stage <> 'search' AND source IS NOT NULL GROUP BY source ORDER BY n DESC LIMIT 40");
  const runSources = await app.db.q("SELECT DISTINCT source FROM run_sources WHERE search_id NOT IN ('hot-topics','news','enrich')");
  const srcOpts = new Map();
  for (const s of sources) {
    const key = s.source.startsWith("ddgs") ? "ddgs" : s.source;
    srcOpts.set(key, (srcOpts.get(key) || 0) + s.n);
  }
  for (const s of runSources) if (!srcOpts.has(s.source)) srcOpts.set(s.source, 0);
  const opt = (v, label, cur) => `<option value="${esc(v)}"${String(cur) === String(v) ? " selected" : ""}>${esc(label)}</option>`;
  view.innerHTML = `
  <section class="filters card">
    <label>Search<select id="f-search">${opt("all", "Everything", f.search)}${opt("hot", "Hot topics (LinkedIn)", f.search)}${opt("news", "Industry news", f.search)}
      ${app.searches.map((s) => opt(s.id, s.name, f.search)).join("")}</select></label>
    <label>Show<select id="f-status">${opt("open", "To review", f.status)}${opt("rated", "Rated by you", f.status)}${opt("all", "All checked", f.status)}${opt("filtered", "Filtered out", f.status)}</select></label>
    <label>Source<select id="f-source">${opt("", "All sources", f.source)}${[...srcOpts.keys()].map((s) => opt(s, sourceName(s), f.source)).join("")}</select></label>
    <label>Period<select id="f-days">${opt(1, "Last 24 hours", f.days)}${opt(3, "Last 3 days", f.days)}${opt(7, "Last 7 days", f.days)}${opt(30, "Last 30 days", f.days)}${opt(0, "Any time", f.days)}</select></label>
    <label>Min score<select id="f-min">${[0, 40, 50, 60, 70, 80].map((n) => opt(n, n ? `${n}+` : "Any", f.min)).join("")}</select></label>
    <label>Sort<select id="f-sort">${opt("new", "Newest first", f.sort)}${opt("score", "Best first", f.sort)}</select></label>
    <label class="grow">Find<input id="f-q" type="search" placeholder="Name, company, word…" value="${esc(f.q)}"></label>
  </section>
  <div id="feed-list"></div>
  <div class="pager"><button id="f-more" class="btn" hidden>Show more</button></div>`;
  const update = () => {
    app.prefs.feed = {
      search: $("#f-search").value, status: $("#f-status").value, source: $("#f-source").value,
      days: Number($("#f-days").value), min: Number($("#f-min").value), sort: $("#f-sort").value, q: $("#f-q").value.trim(),
    };
    savePrefs();
    loadFeed(0, true);
  };
  for (const id of ["f-search", "f-status", "f-source", "f-days", "f-min", "f-sort"]) $(`#${id}`).onchange = update;
  let t;
  $("#f-q").oninput = () => { clearTimeout(t); t = setTimeout(update, 400); };
  await loadFeed(0, true);
}

async function loadFeed(page, reset) {
  const f = Object.assign({}, FEED_DEFAULTS, app.prefs.feed);
  const list = $("#feed-list");
  if (reset) list.innerHTML = `<p class="muted pad">Loading…</p>`;
  const [sql, args] = feedQuery(f, page);
  const rows = await app.db.q(sql, args);
  const more = rows.length > PAGE;
  const html = rows.slice(0, PAGE).map((r) => card(r)).join("");
  if (reset) list.innerHTML = html || `<div class="card empty"><p>Nothing here with these filters.</p>
    <p class="muted">Try “All checked”, a longer period or “Everything”.</p></div>`;
  else list.insertAdjacentHTML("beforeend", html);
  wireCards(list);
  const btn = $("#f-more");
  btn.hidden = !more;
  btn.onclick = () => loadFeed(page + 1, false);
}

// ── item / match detail ──────────────────────────────────────────────────────

async function renderDetail(view, [id], name) {
  const isMatch = name === "match";
  const rows = isMatch
    ? await app.db.q(`SELECT 'match' AS rtype, m.id AS rid, m.search_id, i.*, m.found_at, m.sources, i.source AS source_label,
        m.queries, m.stage, m.filter_reason, m.score, m.triage_score, m.triage_reason, m.analysis, m.breakdown, m.notified_at,
        m.digest_slot AS digest, m.feedback, m.analyzed_at FROM matches m JOIN items i ON i.id = m.item_id WHERE m.id = ?`, [Number(id)])
    : await app.db.q(`SELECT 'item' AS rtype, i.id AS rid, NULL AS search_id, i.*, i.discovered_at AS found_at, i.source AS sources,
        i.source AS source_label, i.digest_date AS digest FROM items i WHERE i.id = ?`, [Number(id)]);
  const r = rows[0];
  if (!r) { view.innerHTML = `<div class="card empty">This item is no longer stored.</div>`; return; }
  const b = json(r.breakdown, {}) || {};
  const comments = json(r.top_comments, []) || [];
  const timeline = [["Published", r.published_at], ["Found", r.found_at], ["Analyzed", r.analyzed_at],
                    ["Sent to Telegram", r.notified_at], ["In digest", r.digest]].filter(([, v]) => v);
  const fullText = r.text || r.snippet || "";
  view.innerHTML = `<p><a href="#/feed" class="link">← Back</a></p>
    ${card({ ...r, text: fullText }, { full: true })}
    <section class="card">
      <h2>Score breakdown</h2>
      ${Object.keys(b).length ? `<div class="bars">${Object.entries(b).filter(([k]) => k !== "penalties").map(([k, v]) =>
        `<div class="bar-row"><span>${esc(k.replace(/_/g, " "))}</span><span class="bar"><i style="--w:${Math.min(100, v * 2.5)}%"></i></span><b>${esc(v)}</b></div>`).join("")}
        ${b.penalties && Object.keys(b.penalties).length ? `<p class="warn">Penalties: ${esc(Object.entries(b.penalties).map(([k, v]) => `${k.replace(/_/g, " ")} −${v}`).join(", "))}</p>` : ""}</div>`
      : `<p class="muted">Not analyzed in depth${r.triage_reason ? `: ${esc(r.triage_reason)}` : "."}</p>`}
      <h2>Timeline</h2><ul class="plain">${timeline.map(([k, v]) => `<li><b>${k}:</b> ${esc(when(v))} <span class="muted">(${esc(ago(v))})</span></li>`).join("")}</ul>
      ${comments.length ? `<h2>Existing comments</h2><ul class="plain">${comments.slice(0, 8).map((c) => `<li><b>${esc(c.author || "")}:</b> ${esc(clip(c.text, 300))}</li>`).join("")}</ul>` : ""}
    </section>`;
  wireCards(view);
}

// ── people ───────────────────────────────────────────────────────────────────

const PEOPLE_DEFAULTS = { search: "", status: "open", type: "", min: 50 };

async function renderPeople(view) {
  const peopleSearches = app.searches.filter((s) => s.type === "event_attendees");
  if (!peopleSearches.length) {
    view.innerHTML = `<div class="card empty"><p>No “people to meet” searches yet.</p>
      <p><a class="btn primary" href="#/search/new/event_attendees">Create one</a></p></div>`;
    return;
  }
  const f = Object.assign({}, PEOPLE_DEFAULTS, { search: peopleSearches[0].id }, app.prefs.people);
  if (!peopleSearches.some((s) => s.id === f.search)) f.search = peopleSearches[0].id;
  const rows = await app.db.q(`SELECT 'match' AS rtype, m.id AS rid, m.search_id, ${ROW_COLS.replace(/(\w+)/g, "i.$1")},
      m.found_at, m.sources, m.queries, m.stage, m.score, m.triage_score, m.analysis, m.notified_at, m.digest_slot AS digest, m.feedback
    FROM matches m JOIN items i ON i.id = m.item_id WHERE m.search_id = ? AND m.stage = 'analyzed' ORDER BY m.score DESC`, [f.search]);
  const waiting = await app.db.q(`SELECT COUNT(*) AS n FROM matches WHERE search_id = ? AND (stage = 'new' OR stage = 'triaged')`, [f.search]);
  const seen = new Set();
  let people = [];
  for (const r of rows) {
    const k = r.author_handle || `m${r.rid}`;
    if (seen.has(k)) continue;
    seen.add(k);
    people.push({ ...r, a: json(r.analysis, {}) || {} });
  }
  const types = [...new Set(people.map((p) => p.a.company_type).filter(Boolean))].sort();
  const status = (p) => {
    const v = app.pending.get(`match:${p.rid}`) || feedbackCode(p.feedback, true);
    return v === "mc" ? "contacted" : v === "mn" || v === "mm" ? "not_relevant" : v === "mu" ? "useful" : "open";
  };
  people = people.filter((p) => (p.score ?? 0) >= f.min && (!f.type || p.a.company_type === f.type)
    && (f.status === "all" || (f.status === "open" ? ["open", "useful"].includes(status(p)) : status(p) === f.status)));
  const opt = (v, label, cur) => `<option value="${esc(v)}"${String(cur) === String(v) ? " selected" : ""}>${esc(label)}</option>`;
  view.innerHTML = `
  <section class="filters card">
    <label>Search<select id="p-search">${peopleSearches.map((s) => opt(s.id, s.name, f.search)).join("")}</select></label>
    <label>Show<select id="p-status">${opt("open", "To contact", f.status)}${opt("contacted", "Reached out", f.status)}${opt("not_relevant", "Not relevant", f.status)}${opt("all", "Everyone", f.status)}</select></label>
    <label>Company type<select id="p-type">${opt("", "Any", f.type)}${types.map((t) => opt(t, t.replace(/_/g, " "), f.type)).join("")}</select></label>
    <label>Min score<select id="p-min">${[0, 40, 50, 60, 70].map((n) => opt(n, n ? `${n}+` : "Any", f.min)).join("")}</select></label>
    <div class="grow right"><button class="btn" id="p-csv">Download CSV</button></div>
  </section>
  <p class="muted pad">${people.length} people · ${waiting[0].n} more posts still being checked by the radar</p>
  <div class="table-wrap"><table class="people">
    <thead><tr><th>Score</th><th>Person</th><th>Role and company</th><th>At the event</th><th>Status</th><th></th></tr></thead>
    <tbody>${people.map((p) => `<tr>
      <td>${badge(p.score)}</td>
      <td><a href="#/match/${p.rid}"><b>${esc(p.a.person || p.author_name || p.a.company || "?")}</b></a>
        ${p.a.poster_type === "company" && p.author_name ? `<div class="muted small">posted by ${esc(p.author_name)}</div>` : ""}</td>
      <td>${esc([p.a.role, p.a.company].filter(Boolean).join(", "))}<div class="muted small">${esc((p.a.company_type || "").replace(/_/g, " "))}</div></td>
      <td>${esc(attendance(p.a))}</td>
      <td><span class="chip st-${status(p)}">${{ open: "To contact", useful: "Useful", contacted: "Reached out", not_relevant: "Not relevant" }[status(p)]}</span></td>
      <td class="nowrap">${safeUrl(p.url) ? `<a class="btn small" href="${esc(safeUrl(p.url))}" target="_blank" rel="noopener noreferrer">Post ↗</a>` : ""}
        <button class="fb small" data-kind="match_feedback" data-target="${p.rid}" data-key="match:${p.rid}" data-act="mc">✅</button>
        <button class="fb small" data-kind="match_feedback" data-target="${p.rid}" data-key="match:${p.rid}" data-act="mn">👎</button></td>
    </tr>`).join("")}</tbody></table></div>
  ${people.length ? "" : `<div class="card empty">No one matches these filters yet.</div>`}`;
  const update = () => {
    app.prefs.people = { search: $("#p-search").value, status: $("#p-status").value, type: $("#p-type").value, min: Number($("#p-min").value) };
    savePrefs();
    renderPeople(view);
  };
  for (const id of ["p-search", "p-status", "p-type", "p-min"]) $(`#${id}`).onchange = update;
  for (const b of $$(".fb", view)) b.onclick = async () => {
    if (await act(b.dataset.kind, b.dataset.target, b.dataset.act, b.dataset.key)) renderPeople(view);
  };
  $("#p-csv").onclick = () => downloadCsv(`${f.search}-people.csv`, [
    ["score", "person", "role", "company", "company_type", "attending_as", "stand", "dates", "invites_meetings",
     "why", "comment", "connect_note", "post_url", "status"],
    ...people.map((p) => [p.score, p.a.person || p.author_name || "", p.a.role, p.a.company, p.a.company_type, p.a.attending_as,
      p.a.stand, p.a.dates, p.a.invites_meetings ? "yes" : "", p.a.why, p.a.comment, p.a.connect_note, p.url, status(p)]),
  ]);
}

function downloadCsv(name, rows) {
  const cell = (v) => { const s = String(v ?? ""); return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s; };
  const blob = new Blob(["﻿" + rows.map((r) => r.map(cell).join(",")).join("\n")], { type: "text/csv;charset=utf-8" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = name;
  document.body.append(a);
  a.click();
  setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
}

// ── searches ─────────────────────────────────────────────────────────────────

async function webSpecs() {
  const rows = await app.db.q("SELECT id, spec, deleted, updated_at FROM search_specs");
  return new Map(rows.map((r) => [r.id, { ...r, spec: json(r.spec, {}) }]));
}

function effectiveSpec(s, web) {
  const w = web.get(s.id);
  return w && !w.deleted ? { ...s.spec, ...w.spec } : s.spec;
}

async function renderSearches(view) {
  const [counts, web] = await Promise.all([
    app.db.q(`SELECT search_id, stage, COUNT(*) AS n, SUM(CASE WHEN score >= 50 THEN 1 ELSE 0 END) AS strong
              FROM matches GROUP BY search_id, stage`),
    webSpecs(),
  ]);
  const stat = {};
  for (const c of counts) {
    const s = (stat[c.search_id] ||= { found: 0, matched: 0, analyzed: 0, strong: 0, waiting: 0 });
    s.found += c.n;
    if (c.stage === "triaged" || c.stage === "analyzed") s.matched += c.n;
    if (c.stage === "analyzed") { s.analyzed += c.n; s.strong += c.strong || 0; }
    if (c.stage === "new" || c.stage === "triaged") s.waiting += c.n;
  }
  const newOnly = [...web.values()].filter((w) => !w.deleted && !app.searches.some((s) => s.id === w.id));
  const today = new Date().toISOString().slice(0, 10);
  const statusOf = (spec) => spec.enabled === false ? "paused" : spec.stop_after && spec.stop_after < today ? "ended" : "active";
  const cards = app.searches.map((s) => {
    const spec = effectiveSpec(s, web);
    const st = stat[s.id] || { found: 0, matched: 0, analyzed: 0, strong: 0, waiting: 0 };
    const w = web.get(s.id);
    const pending = w && (!s.last_run || w.updated_at > s.last_run);
    const status = w && w.deleted ? "deleted" : statusOf(spec);
    return `<article class="card search">
      <div class="item-head"><span class="chip st-${status}">${{ active: "Active", paused: "Paused", ended: "Ended", deleted: "Deleted" }[status]}</span>
        <h2>${esc(spec.name || s.name)}</h2></div>
      <p class="muted">${esc({ event_attendees: "People to meet at an event", keyword_alert: "Keyword alert", custom: "Custom search" }[spec.type] || spec.type)}
        · every ${esc(spec.every_hours || 8)}h${spec.published_from ? ` · posts from ${esc(spec.published_from)}` : ""}${spec.stop_after ? ` · until ${esc(spec.stop_after)}` : ""}
        · ${(spec.queries || []).length} phrases · last run ${esc(ago(s.last_run) || "not yet")}</p>
      ${pending ? `<p class="warn">Changes saved here apply on the next radar run.</p>` : ""}
      <div class="stats">
        <div><b>${num(st.found)}</b><span>posts found</span></div><div><b>${num(st.matched)}</b><span>real matches</span></div>
        <div><b>${num(st.analyzed)}</b><span>analyzed</span></div><div><b>${num(st.strong)}</b><span>worth your time</span></div>
        <div><b>${num(st.waiting)}</b><span>being checked</span></div></div>
      <div class="foot">
        <a class="btn" href="#/search/${encodeURIComponent(s.id)}">Edit</a>
        ${status === "deleted" ? "" : `<button class="btn" data-toggle="${esc(s.id)}">${status === "paused" ? "Resume" : "Pause"}</button>`}
        <a class="btn" href="#/feed" data-show="${esc(s.id)}">See results</a>
        ${spec.type === "event_attendees" ? `<a class="btn" href="#/people">People</a>` : ""}
      </div>
      <details><summary>Phrases and how they perform</summary><div class="phr" data-phrases="${esc(s.id)}"><p class="muted">Loading…</p></div></details>
    </article>`;
  }).join("");
  view.innerHTML = `
    <div class="row-head"><h1>Your searches</h1>
      <div><button class="btn" id="run-now">Run now</button>
        <a class="btn primary" href="#/search/new/event_attendees">New search</a></div></div>
    ${newOnly.map((w) => `<div class="card"><b>${esc(w.spec.name || w.id)}</b> <span class="chip">New</span>
       <p class="muted">Saved in the app. The next radar run starts it.</p>
       <a class="btn" href="#/search/${encodeURIComponent(w.id)}">Edit</a></div>`).join("")}
    ${cards}
    <div class="card"><h2>Built in</h2><p class="muted">The hot-topics radar (aviation and travel tech) and the
      industry news brief also run on every digest. Their topics and phrases live in <code>config.yaml</code>.</p></div>`;
  for (const b of $$("[data-toggle]", view)) b.onclick = async () => {
    const s = app.searches.find((x) => x.id === b.dataset.toggle);
    const spec = { ...effectiveSpec(s, web) };
    spec.enabled = spec.enabled === false;
    if (await saveSpec(spec)) renderSearches(view);
  };
  for (const a of $$("[data-show]", view)) a.onclick = () => {
    app.prefs.feed = { ...FEED_DEFAULTS, ...app.prefs.feed, search: a.dataset.show, status: "all", days: 0 };
    savePrefs();
  };
  for (const d of $$("details", view)) d.ontoggle = async () => {
    const box = $("[data-phrases]", d);
    if (!d.open || box.dataset.loaded) return;
    box.dataset.loaded = "1";
    const qs = await app.db.q(`SELECT text, active, runs, results, fresh, good, last_run FROM search_queries
                               WHERE search_id = ? ORDER BY active DESC, good DESC, fresh DESC`, [box.dataset.phrases]);
    box.innerHTML = `<table class="mini"><thead><tr><th>Phrase</th><th>Runs</th><th>Results</th><th>New</th><th>Useful</th><th>Last</th></tr></thead>
      <tbody>${qs.map((q) => `<tr class="${q.active ? "" : "muted"}"><td>${esc(q.text)}</td><td>${q.runs}</td><td>${q.results}</td>
        <td>${q.fresh}</td><td>${q.good}</td><td>${esc(ago(q.last_run) || "–")}</td></tr>`).join("")}</tbody></table>`;
  };
  $("#run-now").onclick = () => runNow();
}

async function saveSpec(spec, deleted = false) {
  try {
    await app.db.run([["INSERT OR REPLACE INTO search_specs(id, spec, deleted, updated_at) VALUES(?,?,?,?)",
                       [spec.id, JSON.stringify(spec), deleted ? 1 : 0, nowIso()]]]);
    toast(deleted ? "Deleted. The next radar run stops it." : "Saved. The next radar run uses it.");
    return true;
  } catch (e) { toast(e.message, true); return false; }
}

const slug = (s) => String(s || "").toLowerCase().normalize("NFKD").replace(/['’]/g, "").replace(/[^a-z0-9]+/g, "-")
  .replace(/^-+|-+$/g, "").slice(0, 40);
const lines = (s) => String(s || "").split("\n").map((x) => x.trim()).filter(Boolean);
const isoDay = (d) => d.toISOString().slice(0, 10);

function eventPhrases(full, short) {
  short = short || full.replace(/\s+20\d\d$/, "");
  const year = (full.match(/20\d\d/) || [""])[0];
  const tag = short.replace(/[^A-Za-z0-9]/g, "");
  return [`"${full}"`, year && !full.includes(short + " " + year) ? `"${short} ${year}"` : `"${short}"`, `#${tag}`,
    year ? `#${tag}${year.slice(2)}` : "", `"see you at ${short}"`, `"heading to ${short}"`, `"meet us at ${short}"`,
    `"${short}" stand`, `attending ${full} come meet us`, `speaking at ${full}`].filter(Boolean);
}

const TEMPLATES = {
  event_attendees: () => ({
    type: "event_attendees", name: "", goal: "", channels: ["linkedin_posts"], queries: [], require_any: [],
    match: "The author, or the author's company, will be at the event: attending, exhibiting (stand or booth), speaking, hosting meetings, or inviting people to meet them there.",
    not_match: "General news or previews about the event without the author going; recaps of earlier editions; posts by the organisers that name no participants; job ads; ticket or discount promotions.",
    priorities: "Highest: airlines, OTAs, flight consolidators and ticket wholesalers, travel payments and fintech, travel-tech founders and investors. Medium: TMCs, GDS and distribution tech, tour operators, airports. Lowest: hotels, destinations, tourism boards, DMCs, PR agencies.",
    published_from: isoDay(new Date(Date.now() - 14 * 864e5)), stop_after: "", every_hours: 6, max_queries_per_run: 12,
    backends: ["tavily", "exa"], backends_per_query: 1, event: { name: "", place: "", dates: "" },
    delivery: { digest_min: 50, telegram_instant_min: 80, telegram_digest: true },
  }),
  keyword_alert: () => ({
    type: "keyword_alert", name: "", goal: "", channels: ["linkedin_posts"], queries: [], require_any: [],
    match: "The post talks about the topic in a way where a thoughtful comment from Anastasia fits naturally.",
    not_match: "Job ads, pure promotions, unrelated uses of the words.",
    priorities: "Higher for airline, travel-tech and travel-payments people and for posts with active discussion.",
    published_from: "", stop_after: "", every_hours: 12, max_queries_per_run: 10, backends: ["tavily", "exa"],
    backends_per_query: 1, delivery: { digest_min: 60, telegram_instant_min: 85, telegram_digest: true },
  }),
};
TEMPLATES.custom = () => ({ ...TEMPLATES.keyword_alert(), type: "custom" });

async function renderSearchEdit(view, [id, template]) {
  const web = await webSpecs();
  const isNew = id === "new";
  const existing = app.searches.find((s) => s.id === id);
  let spec = isNew ? (TEMPLATES[template] || TEMPLATES.event_attendees)()
    : existing ? effectiveSpec(existing, web) : web.get(id) ? web.get(id).spec : null;
  if (!spec) { view.innerHTML = `<div class="card empty">This search doesn't exist.</div>`; return; }
  spec = JSON.parse(JSON.stringify(spec));
  const ev = spec.event || {};
  const d = spec.delivery || {};
  const backends = Object.entries(app.config.search_backends || { tavily: {}, exa: {}, linkup: {} })
    .filter(([n, c]) => c.enabled !== false && n !== "brave");
  const people = spec.type === "event_attendees";
  view.innerHTML = `<p><a href="#/searches" class="link">← Searches</a></p>
  <form class="card form" id="sf" autocomplete="off">
    <h1>${isNew ? "New search" : "Edit search"}</h1>
    ${isNew ? `<div class="tabs">${Object.entries({ event_attendees: "People at an event", keyword_alert: "Keyword alert", custom: "Custom" })
      .map(([k, v]) => `<a class="tab${k === spec.type ? " on" : ""}" href="#/search/new/${k}">${v}</a>`).join("")}</div>` : ""}
    <label>Name<input name="name" required value="${esc(spec.name)}" placeholder="${people ? "ITB Asia 2026: who's going" : "Unused flight tickets"}"></label>
    ${people ? `<fieldset><legend>Event</legend>
      <div class="cols3"><label>Event name<input name="ev_name" value="${esc(ev.name)}" placeholder="ITB Asia 2026"></label>
      <label>Place<input name="ev_place" value="${esc(ev.place)}" placeholder="Marina Bay Sands, Singapore"></label>
      <label>Dates<input name="ev_dates" value="${esc(ev.dates)}" placeholder="21-23 October 2026"></label></div>
      <button type="button" class="btn" id="suggest">Suggest search phrases from the event name</button></fieldset>` : ""}
    <label>Goal, in plain words<textarea name="goal" rows="2" placeholder="Who or what you want to find, and why">${esc(spec.goal)}</textarea></label>
    <label>Counts as a match<textarea name="match" rows="2">${esc(spec.match)}</textarea></label>
    <label>Doesn't count<textarea name="not_match" rows="2">${esc(spec.not_match)}</textarea></label>
    <label>Priorities<textarea name="priorities" rows="2">${esc(spec.priorities)}</textarea></label>
    <label>Search phrases, one per line <span class="muted">("quotes" = exact phrase)</span>
      <textarea name="queries" rows="8" required>${esc((spec.queries || []).join("\n"))}</textarea></label>
    <label>A post must mention one of <span class="muted">(comma-separated; skips the AI for anything else)</span>
      <input name="require_any" value="${esc((spec.require_any || []).join(", "))}"></label>
    <div class="cols3">
      <label>Posts published from<input type="date" name="published_from" value="${esc(spec.published_from || "")}"></label>
      <label>Stop after<input type="date" name="stop_after" value="${esc(spec.stop_after || "")}"></label>
      <label>Search every<select name="every_hours">${[6, 8, 12, 24].map((h) => `<option value="${h}"${Number(spec.every_hours) === h ? " selected" : ""}>${h} hours</option>`).join("")}</select></label>
    </div>
    <fieldset><legend>Where to look</legend>
      <label class="check"><input type="checkbox" name="ch_posts"${(spec.channels || []).includes("linkedin_posts") ? " checked" : ""}> LinkedIn posts</label>
      <label class="check"><input type="checkbox" name="ch_articles"${(spec.channels || []).includes("linkedin_articles") ? " checked" : ""}> LinkedIn articles</label>
      <div class="muted small">Search services:</div>
      ${backends.map(([n]) => `<label class="check"><input type="checkbox" name="be_${esc(n)}"${!spec.backends || !spec.backends.length || spec.backends.includes(n) ? " checked" : ""}> ${esc(sourceName(n))}</label>`).join("")}
    </fieldset>
    <fieldset><legend>Telegram</legend>
      <div class="cols3"><label>List in the digest from score<input type="number" name="digest_min" min="0" max="100" value="${esc(d.digest_min ?? 50)}"></label>
      <label>Instant card from score<input type="number" name="instant_min" min="0" max="100" value="${esc(d.telegram_instant_min ?? 80)}"></label>
      <label class="check"><input type="checkbox" name="enabled"${spec.enabled === false ? "" : " checked"}> Search is on</label></div>
    </fieldset>
    <p class="budget" id="budget"></p>
    <div class="foot"><button class="primary" type="submit">Save</button>
      ${isNew ? "" : `<button type="button" class="btn danger" id="del">Delete</button>`}</div>
  </form>`;
  const form = $("#sf");
  const read = () => {
    const v = (n) => (form.elements[n] ? form.elements[n].value.trim() : "");
    const c = (n) => !!(form.elements[n] && form.elements[n].checked);
    const out = { ...spec, name: v("name"), goal: v("goal"), match: v("match"), not_match: v("not_match"), priorities: v("priorities"),
      queries: lines(v("queries")), require_any: v("require_any").split(",").map((x) => x.trim()).filter(Boolean),
      published_from: v("published_from") || null, stop_after: v("stop_after") || null, every_hours: Number(v("every_hours")) || 8,
      channels: [c("ch_posts") && "linkedin_posts", c("ch_articles") && "linkedin_articles"].filter(Boolean),
      backends: backends.map(([n]) => n).filter((n) => c(`be_${n}`)), enabled: c("enabled"),
      delivery: { ...d, digest_min: Number(v("digest_min")) || 50, telegram_instant_min: Number(v("instant_min")) || 80 } };
    if (people) out.event = { name: v("ev_name"), place: v("ev_place"), dates: v("ev_dates") };
    out.id = isNew ? slug(out.name) : spec.id;
    return out;
  };
  const showBudget = async () => {
    const s = read();
    const perRun = Math.min(s.queries.length, Number(s.max_queries_per_run || 12));
    const runsPerDay = 24 / s.every_hours;
    const end = s.stop_after ? Date.parse(s.stop_after) : Date.now() + 30 * 864e5;
    const days = Math.max(1, Math.ceil((end - Date.now()) / 864e5));
    const calls = Math.round(perRun * runsPerDay * days * Number(s.backends_per_query || 1));
    const month = new Date().toISOString().slice(0, 7);
    const used = await app.db.q("SELECT provider, SUM(requests) AS r FROM usage WHERE day LIKE ? AND provider LIKE 'search:%' GROUP BY provider", [`${month}%`]);
    let total = 0;
    const left = s.backends.map((n) => {
      const lim = (app.config.search_backends || {})[n] && app.config.search_backends[n].monthly_limit;
      const u = (used.find((x) => x.provider === `search:${n}`) || {}).r || 0;
      total += lim ? Math.max(0, lim - u) : 0;
      return lim ? `${sourceName(n)} ${num(Math.max(0, lim - u))} left this month` : "";
    }).filter(Boolean);
    const tight = left.length && calls > total;
    $("#budget").className = "budget" + (tight ? " tight" : "");
    $("#budget").innerHTML = `Estimated cost: about <b>${num(calls)}</b> searches over ${days} days
      (${perRun} phrases × ${runsPerDay.toFixed(0)} runs a day). ${left.length ? `Free allowance: ${esc(left.join(", "))}.` : ""}
      ${tight ? "<b>That's more than the free allowance left.</b> Fewer phrases, a later start or searching less often helps; otherwise the radar spreads the searches out. "
        : ""}The radar never goes past a free limit.${!s.stop_after && people ? " Tip: set “Stop after” to the day after the event." : ""}`;
  };
  form.oninput = () => { clearTimeout(showBudget._t); showBudget._t = setTimeout(showBudget, 300); };
  showBudget();
  if ($("#suggest")) $("#suggest").onclick = () => {
    const name = form.elements.ev_name.value.trim();
    if (!name) { toast("Enter the event name first.", true); return; }
    const short = name.replace(/\s+20\d\d$/, "");
    form.elements.queries.value = eventPhrases(name, short).join("\n");
    if (!form.elements.require_any.value) form.elements.require_any.value = short;
    if (!form.elements.name.value) form.elements.name.value = `${name}: who's going`;
    if (!form.elements.goal.value) form.elements.goal.value =
      `Find people in airlines, OTAs, travel tech, travel payments and flight ticket distribution who say they will attend, exhibit or speak at ${name}, so Anastasia can meet them there.`;
    showBudget();
  };
  form.onsubmit = async (e) => {
    e.preventDefault();
    const s = read();
    if (!s.id) { toast("Give the search a name.", true); return; }
    if (!s.queries.length) { toast("Add at least one search phrase.", true); return; }
    if (!s.channels.length) { toast("Pick at least one place to look.", true); return; }
    if (isNew && app.searches.some((x) => x.id === s.id)) { toast("A search with this name already exists.", true); return; }
    if (await saveSpec(s)) location.hash = "#/searches";
  };
  if ($("#del")) $("#del").onclick = async () => {
    if (!confirm(`Delete “${spec.name}”? Its results stay; it just stops searching.`)) return;
    if (await saveSpec(spec, true)) location.hash = "#/searches";
  };
}

// ── runs and sources ─────────────────────────────────────────────────────────

async function renderRuns(view) {
  const month = new Date().toISOString().slice(0, 7);
  const today = new Date().toISOString().slice(0, 10);
  const week = sinceDays(7);
  const [usageMonth, usageToday, sources, found, matchSrc, runs] = await app.db.run([
    ["SELECT provider, SUM(requests) AS r FROM usage WHERE day LIKE ? GROUP BY provider", [`${month}%`]],
    ["SELECT provider, requests AS r FROM usage WHERE day = ?", [today]],
    [`SELECT rs.search_id, rs.source, SUM(rs.calls) AS calls, SUM(rs.results) AS results, SUM(rs.new) AS new,
             SUM(rs.errors) AS errors, MAX(rs.last_error) AS last_error
      FROM run_sources rs JOIN runs ru ON ru.id = rs.run_id WHERE ru.started_at >= ?
      GROUP BY rs.search_id, rs.source ORDER BY rs.search_id, results DESC`, [week]],
    [`SELECT CASE WHEN source LIKE 'ddgs%' THEN 'ddgs' ELSE source END AS source, kind, COUNT(*) AS n,
             SUM(CASE WHEN stage = 'analyzed' THEN 1 ELSE 0 END) AS analyzed,
             SUM(CASE WHEN score >= 60 THEN 1 ELSE 0 END) AS good
      FROM items WHERE discovered_at >= ? AND stage <> 'search' GROUP BY 1, 2 ORDER BY n DESC`, [week]],
    ["SELECT sources, stage, score FROM matches WHERE found_at >= ?", [week]],
    ["SELECT * FROM runs ORDER BY id DESC LIMIT 30"],
  ]);
  const cfg = app.config;
  const meter = (label, used, limit) => {
    const pct = limit ? Math.min(100, Math.round((used / limit) * 100)) : 0;
    return `<div class="meter"><div class="meter-top"><span>${esc(label)}</span><span>${num(used)}${limit ? ` / ${num(limit)}` : ""}</span></div>
      <span class="bar${pct > 85 ? " high" : ""}"><i style="--w:${pct}%"></i></span></div>`;
  };
  const um = Object.fromEntries(usageMonth.map((u) => [u.provider, u.r]));
  const ut = Object.fromEntries(usageToday.map((u) => [u.provider, u.r]));
  const searchMeters = Object.entries(cfg.search_backends || {}).filter(([n, c]) => c.enabled !== false && c.monthly_limit)
    .map(([n, c]) => meter(`${sourceName(n)} (this month)`, um[`search:${n}`] || 0, c.monthly_limit)).join("");
  const llmMeters = (cfg.llm_providers || []).filter((p) => ut[`llm:${p.name}`])
    .map((p) => meter(`AI: ${p.name} (today)`, ut[`llm:${p.name}`] || 0, p.daily_limit)).join("");
  const monthRuns = runs.filter((r) => (r.started_at || "").startsWith(month));
  const minutes = monthRuns.reduce((s, r) => s + (r.finished_at ? Math.ceil((Date.parse(r.finished_at) - Date.parse(r.started_at)) / 6e4) + 1 : 1), 0);
  // Matches per source (a match can be credited to several sources).
  const ms = {};
  for (const m of matchSrc) for (const s of json(m.sources, []) || []) {
    const x = (ms[s] ||= { n: 0, matched: 0, good: 0 });
    x.n++; if (m.stage === "triaged" || m.stage === "analyzed") x.matched++; if ((m.score || 0) >= 50) x.good++;
  }
  // LinkedIn sources first, then the strongest news feeds; the long tail of feeds is one row.
  const posts = found.filter((f) => f.kind !== "news");
  const news = found.filter((f) => f.kind === "news").sort((a, b) => b.good - a.good || b.n - a.n);
  const tail = news.slice(6).reduce((t, f) => ({ n: t.n + f.n, analyzed: t.analyzed + f.analyzed, good: t.good + f.good, count: t.count + 1 }),
                                    { n: 0, analyzed: 0, good: 0, count: 0 });
  const bySearch = {};
  for (const s of sources) (bySearch[s.search_id] ||= []).push(s);
  const maxRes = Math.max(1, ...sources.map((s) => s.results));
  view.innerHTML = `
  <h1>Runs and sources</h1>
  <section class="grid2">
    <div class="card"><h2>Free allowances</h2>${searchMeters}${llmMeters || `<p class="muted small">No AI calls yet today.</p>`}
      ${meter("GitHub minutes (logged runs this month, estimate)", minutes, 2000)}
      <p class="muted small">Every service is capped below its free limit. When one runs out, the radar switches to the next.</p></div>
    <div class="card"><h2>Where the useful posts came from <span class="muted small">(7 days)</span></h2>
      <table class="mini"><thead><tr><th>Source</th><th>Found</th><th>Analyzed</th><th>Scored 60+</th></tr></thead><tbody>
      ${[...posts, ...news.slice(0, 6)].map((f) => `<tr><td>${esc(sourceName(f.source))}${f.kind === "news" ? ` <span class="muted small">news</span>` : ""}</td><td>${num(f.n)}</td><td>${num(f.analyzed)}</td><td>${num(f.good)}</td></tr>`).join("")}
      ${tail.count ? `<tr class="muted"><td>${tail.count} other news publishers</td><td>${num(tail.n)}</td><td>${num(tail.analyzed)}</td><td>${num(tail.good)}</td></tr>` : ""}
      ${Object.entries(ms).map(([s, x]) => `<tr><td>${esc(sourceName(s))} <span class="muted small">saved searches</span></td><td>${num(x.n)}</td><td>${num(x.matched)} matches</td><td>${num(x.good)} (50+)</td></tr>`).join("")}
      </tbody></table></div>
  </section>
  <section class="card"><h2>What each source returned <span class="muted small">(logged runs, 7 days)</span></h2>
    ${Object.keys(bySearch).length ? Object.entries(bySearch).map(([sid, rows]) => `<h3>${esc({ "hot-topics": "Hot topics", news: "Industry news", enrich: "Reading full posts" }[sid] || searchName(sid))}</h3>
      ${rows.map((s) => `<div class="bar-row wide"><span>${esc(sourceName(s.source))}</span>
        <span class="bar"><i style="--w:${Math.round((s.results / maxRes) * 100)}%"></i></span>
        <b>${num(s.results)}</b><span class="muted small">${num(s.new)} new · ${num(s.calls)} calls${s.errors ? ` · <span class="bad">${num(s.errors)} errors</span>` : ""}</span></div>
        ${s.errors && s.last_error ? `<div class="muted small err">${esc(clip(s.last_error, 160))}</div>` : ""}`).join("")}`).join("")
      : `<p class="muted">The run log starts with the next full run.</p>`}
  </section>
  <section class="card"><h2>Recent runs</h2>
    <table class="mini runs"><thead><tr><th>When</th><th>What</th><th>Started by</th><th>Took</th><th>AI calls</th><th>Telegram</th><th>Result</th></tr></thead><tbody>
    ${runs.map((r) => {
      const llm = json(r.llm, {}) || {};
      const tg = json(r.telegram, {}) || {};
      const took = r.finished_at ? `${Math.max(1, Math.round((Date.parse(r.finished_at) - Date.parse(r.started_at)) / 6e4))} min` : "…";
      return `<tr><td>${esc(when(r.started_at))}</td><td>${esc(r.mode)}</td><td>${esc(r.trigger)}</td><td>${took}</td>
        <td>${esc(Object.entries(llm).map(([k, v]) => `${k} ${v}`).join(", ") || "–")}</td>
        <td>${tg.delivered != null ? `${tg.delivered} sent${tg.failed ? `, ${tg.failed} failed` : ""}` : "–"}</td>
        <td><span class="chip st-${esc(r.status)}">${esc(r.status)}</span>${r.error ? `<div class="muted small err">${esc(clip(r.error, 140))}</div>` : ""}</td></tr>`;
    }).join("")}</tbody></table>
    <div class="foot"><button class="btn" id="run-now">Run now</button></div>
  </section>`;
  $("#run-now").onclick = () => runNow();
}

async function runNow(mode = "force-collect") {
  const gh = store(GH_KEY) || {};
  const repo = gh.repo || app.config.github_repo;
  if (!gh.token || !repo) {
    toast("To start runs from here, add a GitHub key in Settings.", true);
    location.hash = "#/settings";
    return;
  }
  try {
    const r = await fetch(`https://api.github.com/repos/${repo}/actions/workflows/radar.yml/dispatches`, {
      method: "POST", referrerPolicy: "no-referrer",
      headers: { Authorization: `Bearer ${gh.token}`, Accept: "application/vnd.github+json" },
      body: JSON.stringify({ ref: "main", inputs: { mode, trigger: "web" } }),
    });
    if (r.status === 204) toast("Run started. New results arrive in about 5–10 minutes.");
    else if (r.status === 401 || r.status === 403 || r.status === 404) toast("GitHub refused the key. Check it in Settings.", true);
    else toast(`GitHub answered ${r.status}.`, true);
  } catch { toast("Couldn't reach GitHub.", true); }
}

// ── settings ─────────────────────────────────────────────────────────────────

async function renderSettings(view) {
  const [tracked, muted] = await app.db.run([
    ["SELECT handle, name, url, origin, best_score, commented FROM authors WHERE tracked = 1 AND muted = 0 ORDER BY best_score DESC LIMIT 200"],
    ["SELECT handle, name FROM authors WHERE muted = 1 ORDER BY name LIMIT 200"],
  ]);
  const cfg = app.config;
  const p = cfg.profile || {};
  const gh = store(GH_KEY) || {};
  const sched = cfg.schedule || {};
  view.innerHTML = `
  <h1>Settings</h1>
  <section class="grid2">
    <div class="card"><h2>This browser</h2>
      <p>Connected to <code>${esc(new URL(app.db.url).host)}</code>.</p>
      <p class="muted small">The database key is saved only in this browser. Sign out on shared computers.</p>
      <button class="btn danger" id="signout">Sign out on this device</button>
      <h2>Start runs from here <span class="muted small">(optional)</span></h2>
      <p class="muted small">Create a GitHub key that can only start runs: on
        <a href="https://github.com/settings/personal-access-tokens/new" target="_blank" rel="noopener noreferrer">this page</a>
        pick “Only select repositories” → <b>${esc((cfg.github_repo || "linkedinalerts").split("/").pop())}</b>, then under
        Permissions set <b>Actions</b> to “Read and write”. Paste it here.</p>
      <label>Repository<input id="gh-repo" value="${esc(gh.repo || cfg.github_repo || "")}" placeholder="owner/linkedinalerts"></label>
      <label>GitHub key<input id="gh-token" type="password" value="${gh.token ? "••••••••" : ""}" placeholder="github_pat_…"></label>
      <button class="btn" id="gh-save">Save</button></div>
    <div class="card"><h2>Delivery</h2>
      <p>Digests at <b>${esc((sched.digest_times || []).join(" and "))}</b> (${esc(sched.timezone || "")}), quiet hours ${esc(sched.quiet_hours || "none")}.</p>
      <p class="muted small">Instant alerts from score ${esc((cfg.scoring || {}).urgent ?? 80)}, digest from ${esc((cfg.scoring || {}).digest ?? 60)}.</p>
      <h2>What the AI may say about you</h2>
      <ul class="plain">${(p.facts || []).map((f) => `<li>${esc(f)}</li>`).join("") || "<li class=muted>No facts yet.</li>"}</ul>
      <p class="muted small">Edit these, the topics and the comment style in <code>config.yaml</code>.</p></div>
  </section>
  <section class="card"><h2>People the radar follows <span class="muted small">(${tracked.length})</span></h2>
    <div class="track-add"><input id="track-in" placeholder="LinkedIn profile link or handle"><button class="btn" id="track-go">Follow</button></div>
    <ul class="people-list">${tracked.map((a) => `<li><a href="${esc(safeUrl(a.url) || `https://www.linkedin.com/in/${a.handle}`)}" target="_blank" rel="noopener noreferrer">${esc(a.name || a.handle)}</a>
      <span class="muted small">${esc(a.origin || "")}${a.commented ? " · you commented" : ""}</span>
      <button class="link" data-untrack="${esc(a.handle)}">Stop following</button></li>`).join("")}</ul>
    ${muted.length ? `<h2>Muted</h2><ul class="people-list">${muted.map((a) => `<li>${esc(a.name || a.handle)}
      <button class="link" data-unmute="${esc(a.handle)}">Unmute</button></li>`).join("")}</ul>` : ""}
  </section>
  <section class="card"><h2>Companies watched</h2>
    <p>${(cfg.companies || []).map((c) => esc(c.name)).join(" · ") || "–"}</p></section>`;
  $("#signout").onclick = () => {
    if (!confirm("Remove the database key from this browser?")) return;
    store(CONN_KEY, null); store(GH_KEY, null); app.db = null; renderConnect();
  };
  $("#gh-save").onclick = () => {
    const token = $("#gh-token").value.trim();
    store(GH_KEY, { repo: $("#gh-repo").value.trim(), token: token.startsWith("••") ? gh.token : token });
    toast("Saved in this browser.");
  };
  $("#track-go").onclick = async () => {
    const v = $("#track-in").value.trim();
    const m = v.match(/linkedin\.com\/in\/([^/?#]+)/i);
    const handle = (m ? m[1] : v).replace(/^@/, "").toLowerCase();
    if (!handle) return;
    if (await act("track_author", handle, "")) $("#track-in").value = "";
  };
  for (const b of $$("[data-untrack]", view)) b.onclick = async () => { if (await act("untrack_author", b.dataset.untrack, "")) b.closest("li").classList.add("muted"); };
  for (const b of $$("[data-unmute]", view)) b.onclick = async () => { if (await act("unmute_author", b.dataset.unmute, "")) b.closest("li").classList.add("muted"); };
}

// ── start ────────────────────────────────────────────────────────────────────

$("#status").addEventListener("click", async () => {
  if (!app.db) return;
  await loadBasics();
  route();
  toast("Refreshed");
});

(async function boot() {
  const conn = readConnectLink() || store(CONN_KEY);
  if (!conn || !conn.url || !conn.token) { renderConnect(); return; }
  await connect(conn);
})();
