/* ═══════════════ iShare — app.js ═══════════════
   ثوابت → أدوات → حالة → IndexedDB → خلفيات → إشارات → WebRTC → نقل → عرض → أحداث → init */

'use strict';

/* ═══════════ 1) ثوابت ═══════════ */
const APP_VERSION = 'ishare-v1.0.6';
const MAX_FILE = 500 * 1024 * 1024;
const CHUNK_SIZE = 64 * 1024;
const HIGH_WATER = 4 * 1024 * 1024;
const LOW_WATER = 1024 * 1024;
const RECV_FLUSH = 4 * 1024 * 1024;

const MQTT_URL = 'wss://broker.emqx.io:8084/mqtt';
const TOPIC = 'ishare/v1/g';
const PRESENCE_MS = 7000;
const PEER_TTL = 26000;

const ICE_SERVERS = [{ urls: ['stun:stun.l.google.com:19302', 'stun:stun1.l.google.com:19302'] }];

const GLASS_LABELS = { clear: 'شفاف', soft: 'ناعم', solid: 'مصنّر' };
const BG_LABELS = { none: 'بلا خلفية', circles: 'دوائر', galaxy: 'مجرة' };
const THEME_LABELS = { dark: 'داكن', light: 'فاتح' };

/* ═══════════ 2) أدوات ═══════════ */
const $ = (s, r) => (r || document).querySelector(s);
const $$ = (s, r) => Array.from((r || document).querySelectorAll(s));

function clamp(n, a, b) { return Math.min(b, Math.max(a, n)); }
function uid(n) {
  const c = 'abcdef0123456789';
  let s = '';
  for (let i = 0; i < n; i++) s += c[Math.floor(Math.random() * c.length)];
  return s;
}
function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function fmtBytes(n) {
  if (!isFinite(n) || n < 0) n = 0;
  if (n < 1024) return Math.round(n) + ' بايت';
  if (n < 1048576) return (n / 1024).toFixed(1) + ' ك.ب';
  if (n < 1073741824) return (n / 1048576).toFixed(1) + ' م.ب';
  return (n / 1073741824).toFixed(2) + ' ج.ب';
}
function fmtSpeed(bps) {
  if (!isFinite(bps) || bps <= 0) return '—';
  return fmtBytes(bps) + '/ث';
}
function fmtETA(sec) {
  if (!isFinite(sec) || sec <= 0) return '—';
  sec = Math.round(sec);
  const m = Math.floor(sec / 60), s = sec % 60;
  return m > 0 ? m + ':' + String(s).padStart(2, '0') + ' د' : s + ' ث';
}
function fileIcon(name, mime) {
  const e = (name.split('.').pop() || '').toLowerCase();
  if ((mime || '').startsWith('video/') || ['mp4', 'mkv', 'avi', 'mov', 'webm'].includes(e)) return '🎬';
  if ((mime || '').startsWith('audio/') || ['mp3', 'wav', 'ogg', 'm4a', 'flac'].includes(e)) return '🎵';
  if ((mime || '').startsWith('image/') || ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(e)) return '🖼️';
  if (['zip', 'rar', '7z', 'tar', 'gz'].includes(e)) return '🗜️';
  if (['pdf'].includes(e)) return '📕';
  if (['doc', 'docx', 'txt', 'rtf', 'odt'].includes(e)) return '📄';
  if (['apk'].includes(e)) return '🤖';
  return '📦';
}
function vibrate(ms) { try { if (navigator.vibrate) navigator.vibrate(ms); } catch (e) { /* تجاهل */ } }

/* كائنات URL مع كاش LRU */
const urlCache = new Map();
function urlCacheLimit() { return state.prefs.memsave ? 40 : 120; }
function getUrl(key, make) {
  if (urlCache.has(key)) {
    const v = urlCache.get(key);
    urlCache.delete(key); urlCache.set(key, v);
    return v;
  }
  const url = make();
  urlCache.set(key, url);
  while (urlCache.size > urlCacheLimit()) {
    const oldest = urlCache.keys().next().value;
    const u = urlCache.get(oldest);
    try { URL.revokeObjectURL(u); } catch (e) { /* تجاهل */ }
    urlCache.delete(oldest);
  }
  return url;
}
function dropUrl(key) {
  const u = urlCache.get(key);
  if (u) { try { URL.revokeObjectURL(u); } catch (e) { /* تجاهل */ } urlCache.delete(key); }
}

/* ═══════════ 3) الحالة المركزية ═══════════ */
const state = {
  prefs: { theme: 'dark', bg: 'none', glass: 'soft', memsave: false, autoAccept: false, name: '', code: '', pv: 0 },
  tab: 'share',
  myId: uid(10),
  peers: new Map(),
  transfers: new Map(),
  seq: 0,
  chatPeer: null,
  selected: null,
  ws: null,
  wsState: 'idle',
  bc: null,
  backoff: 1000,
  history: []
};

function loadPrefs() {
  try {
    const raw = localStorage.getItem('ishare-prefs');
    if (raw) Object.assign(state.prefs, JSON.parse(raw));
  } catch (e) { /* تجاهل */ }
  if (!state.prefs.code) state.prefs.code = String(Math.floor(1000 + Math.random() * 9000));
  if (!state.prefs.name) state.prefs.name = 'جهاز-' + state.prefs.code;
  if (state.prefs.pv < 3) { state.prefs.autoAccept = false; state.prefs.pv = 3; }
  savePrefs();
}
function savePrefs() {
  try { localStorage.setItem('ishare-prefs', JSON.stringify(state.prefs)); } catch (e) { /* تجاهل */ }
}
function loadSession() {
  try {
    const raw = localStorage.getItem('ishare-session');
    if (raw) {
      const s = JSON.parse(raw);
      if (s.tab === 'share' || s.tab === 'history') state.tab = s.tab;
    }
  } catch (e) { /* تجاهل */ }
}
function saveSession() {
  try { localStorage.setItem('ishare-session', JSON.stringify({ tab: state.tab })); } catch (e) { /* تجاهل */ }
}

/* ═══════════ 4) IndexedDB ═══════════ */
let idb = null;
function initIDB() {
  return new Promise((resolve) => {
    let done = false;
    try {
      const req = indexedDB.open('ishare-db', 1);
      req.onupgradeneeded = (e) => {
        const db = e.target.result;
        if (!db.objectStoreNames.contains('files')) db.createObjectStore('files', { keyPath: 'id' });
        if (!db.objectStoreNames.contains('chunks')) db.createObjectStore('chunks', { keyPath: ['tx', 'seq'] });
      };
      req.onsuccess = (e) => { idb = e.target.result; done = true; resolve(true); };
      req.onerror = () => { done = true; resolve(false); };
      setTimeout(() => { if (!done) resolve(false); }, 3000);
    } catch (e) { resolve(false); }
  });
}
function idbStore(store, mode) {
  return idb.transaction(store, mode).objectStore(store);
}
function idbPut(store, val) {
  return new Promise((res) => {
    if (!idb) return res(false);
    try {
      const r = idbStore(store, 'readwrite').put(val);
      r.onsuccess = () => res(true);
      r.onerror = () => res(false);
    } catch (e) { res(false); }
  });
}
function idbGet(store, key) {
  return new Promise((res) => {
    if (!idb) return res(null);
    try {
      const r = idbStore(store, 'readonly').get(key);
      r.onsuccess = () => res(r.result || null);
      r.onerror = () => res(null);
    } catch (e) { res(null); }
  });
}
function idbDelete(store, key) {
  return new Promise((res) => {
    if (!idb) return res(false);
    try {
      const r = idbStore(store, 'readwrite').delete(key);
      r.onsuccess = () => res(true);
      r.onerror = () => res(false);
    } catch (e) { res(false); }
  });
}
function idbGetAll(store) {
  return new Promise((res) => {
    if (!idb) return res([]);
    try {
      const r = idbStore(store, 'readonly').getAll();
      r.onsuccess = () => res(r.result || []);
      r.onerror = () => res([]);
    } catch (e) { res([]); }
  });
}
function idbGetRange(store, tx) {
  return new Promise((res) => {
    if (!idb) return res([]);
    try {
      const out = [];
      const req = idbStore(store, 'readonly').openCursor(IDBKeyRange.bound([tx, 0], [tx, Infinity]));
      req.onsuccess = (e) => {
        const cur = e.target.result;
        if (cur) { out.push(cur.value); cur.continue(); }
        else res(out);
      };
      req.onerror = () => res(out);
    } catch (e) { res([]); }
  });
}
function idbClearRange(store, tx) {
  return new Promise((res) => {
    if (!idb) return res(false);
    try {
      const r = idbStore(store, 'readwrite').delete(IDBKeyRange.bound([tx, 0], [tx, Infinity]));
      r.onsuccess = () => res(true);
      r.onerror = () => res(false);
    } catch (e) { res(false); }
  });
}
async function loadHistory() {
  const rows = await idbGetAll('files');
  rows.sort((a, b) => (b.ts || 0) - (a.ts || 0));
  state.history = rows.slice(0, 60);
  renderHistory();
}

/* ═══════════ 5) Toast + مودالات ═══════════ */
function toast(text, kind) {
  try {
    const box = $('#toasts');
    const el = document.createElement('div');
    el.className = 'toast' + (kind ? ' ' + kind : '');
    el.textContent = text;
    box.appendChild(el);
    setTimeout(() => {
      el.classList.add('out');
      setTimeout(() => { try { el.remove(); } catch (e) { /* تجاهل */ } }, 350);
    }, 2600);
    while (box.children.length > 4) box.firstChild.remove();
  } catch (e) { /* تجاهل */ }
}

function openModal(id) {
  const el = $('#' + id);
  if (!el) return;
  el.hidden = false;
  vibrate(10);
}
function closeModal(id) {
  const el = $('#' + id);
  if (!el || el.hidden) return;
  el.classList.add('closing');
  setTimeout(() => {
    el.hidden = true;
    el.classList.remove('closing');
  }, 240);
}

let inputResolve = null;
function askInput(title, placeholder, value) {
  return new Promise((res) => {
    inputResolve = res;
    $('#inputTitle').textContent = title;
    const f = $('#inputField');
    f.placeholder = placeholder || '';
    f.value = value || '';
    openModal('inputModal');
    setTimeout(() => { try { f.focus(); } catch (e) { /* تجاهل */ } }, 300);
  });
}
let confirmResolve = null;
function askConfirm(title, text, okLabel) {
  return new Promise((res) => {
    confirmResolve = res;
    $('#confirmTitle').textContent = title;
    $('#confirmText').textContent = text || '';
    $('#confirmOk').textContent = okLabel || 'حذف';
    openModal('confirmModal');
  });
}

/* ═══════════ 6) الثيم + الخلفيات ═══════════ */
function applyPrefs() {
  const p = state.prefs;
  const h = document.documentElement;
  h.setAttribute('data-theme', p.theme);
  h.setAttribute('data-bg', p.bg);
  h.setAttribute('data-glass', p.glass);
  if (p.memsave) h.setAttribute('data-memsave', '');
  else h.removeAttribute('data-memsave');
  const meta = $('meta[name="theme-color"]');
  if (meta) meta.setAttribute('content', p.theme === 'light' ? '#eef1f8' : '#0b0e17');
  $('#themeValue').textContent = THEME_LABELS[p.theme] || 'داكن';
  $('#bgValue').textContent = BG_LABELS[p.bg] || 'بلا خلفية';
  $('#glassValue').textContent = GLASS_LABELS[p.glass] || 'ناعم';
  $('#autoSwitch').classList.toggle('on', !!p.autoAccept);
  $('#memSwitch').classList.toggle('on', !!p.memsave);
  $('#rowGlass').setAttribute('data-disabled', p.bg === 'none' ? '1' : '0');
  $('#nameValue').textContent = p.name;
  $('#myName').textContent = p.name;
  $('#myCode').textContent = p.code;
  startCanvas();
}

/* ═══════════ 7) خلفيات canvas ═══════════ */
let bgRAF = 0;
let bgState = null;

function setupCanvas(cv, dpr) {
  const w = window.innerWidth, h = window.innerHeight;
  cv.width = Math.max(1, Math.round(w * dpr));
  cv.height = Math.max(1, Math.round(h * dpr));
  const ctx = cv.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return ctx;
}

function rnd2(a, b) { return a + Math.random() * (b - a); }

function initBgState(cv, mode) {
  const dpr = clamp(window.devicePixelRatio || 1, 1, 2);
  setupCanvas(cv, dpr);
  const w = window.innerWidth, h = window.innerHeight;
  if (mode === 'circles') {
    const rings = [];
    for (let i = 0; i < 22; i++) {
      rings.push({
        x: rnd2(0, w), y: rnd2(0, h), r: rnd2(24, 130),
        vy: -rnd2(6, 20), vx: rnd2(-6, 6),
        a: rnd2(0.05, 0.16), lw: rnd2(1.2, 3), ph: rnd2(0, 6.28)
      });
    }
    bgState = { rings, w, h };
  } else {
    const stars = [];
    for (let i = 0; i < 150; i++) {
      stars.push({ x: rnd2(0, w), y: rnd2(0, h * 0.94), r: rnd2(0.6, 2.1), ph: rnd2(0, 6.28), sp: rnd2(0.6, 2.2) });
    }
    const clouds = [];
    for (let i = 0; i < 5; i++) {
      clouds.push({ x: rnd2(0, w), y: rnd2(h * 0.08, h * 0.5), r: rnd2(40, 95), vx: rnd2(4, 12) });
    }
    bgState = { stars, clouds, w, h, shoot: null, nextShoot: performance.now() + rnd2(1500, 5000) };
  }
}

function paintBase(ctx, w, h) {
  const light = state.prefs.theme === 'light';
  const mode = state.prefs.bg;
  let g;
  if (mode === 'galaxy' && light) {
    g = ctx.createLinearGradient(0, 0, 0, h);
    g.addColorStop(0, '#9cc8ff'); g.addColorStop(0.55, '#cfe4ff'); g.addColorStop(1, '#eef1f8');
  } else if (mode === 'circles' && light) {
    g = ctx.createLinearGradient(0, 0, w * 0.4, h);
    g.addColorStop(0, '#e8edfa'); g.addColorStop(1, '#eef1f8');
  } else if (light) {
    g = ctx.createLinearGradient(0, 0, w * 0.4, h);
    g.addColorStop(0, '#e8edfa'); g.addColorStop(1, '#eef1f8');
  } else {
    g = ctx.createLinearGradient(0, 0, w * 0.5, h);
    g.addColorStop(0, '#0b0e17'); g.addColorStop(0.6, '#0e1424'); g.addColorStop(1, '#0b0e17');
  }
  ctx.fillStyle = g;
  ctx.fillRect(0, 0, w, h);
}

function drawFrame(cv, mode) {
  const ctx = cv.getContext('2d');
  const w = window.innerWidth, h = window.innerHeight;
  if (bgState && (bgState.w !== w || bgState.h !== h)) { initBgState(cv, mode); }
  if (!bgState) initBgState(cv, mode);
  paintBase(ctx, w, h);
  const light = state.prefs.theme === 'light';
  const t = performance.now() / 1000;
  const acc = light ? '47,107,255' : '79,141,255';

  if (mode === 'circles' && bgState.rings) {
    for (const r of bgState.rings) {
      r.y += r.vy / 60; r.x += r.vx / 60 + Math.sin(t + r.ph) * 0.15;
      if (r.y < -r.r - 20) { r.y = h + r.r; r.x = Math.random() * w; }
      if (r.x < -r.r - 20) r.x = w + r.r;
      if (r.x > w + r.r + 20) r.x = -r.r;
      ctx.beginPath();
      ctx.arc(r.x, r.y, r.r, 0, 6.2832);
      ctx.strokeStyle = 'rgba(' + acc + ',' + r.a.toFixed(3) + ')';
      ctx.lineWidth = r.lw;
      ctx.stroke();
    }
  }

  if (mode === 'galaxy' && bgState.stars) {
    if (light) {
      const sx = w * 0.82, sy = h * 0.16;
      const sun = ctx.createRadialGradient(sx, sy, 6, sx, sy, 150);
      sun.addColorStop(0, 'rgba(255,236,150,.95)');
      sun.addColorStop(0.35, 'rgba(255,220,120,.45)');
      sun.addColorStop(1, 'rgba(255,220,120,0)');
      ctx.fillStyle = sun;
      ctx.beginPath(); ctx.arc(sx, sy, 150, 0, 6.2832); ctx.fill();
      ctx.fillStyle = '#ffe98a';
      ctx.beginPath(); ctx.arc(sx, sy, 34, 0, 6.2832); ctx.fill();
      for (const c of bgState.clouds) {
        c.x += c.vx / 60;
        if (c.x - c.r > w) c.x = -c.r;
        const cg = ctx.createRadialGradient(c.x, c.y, 4, c.x, c.y, c.r);
        cg.addColorStop(0, 'rgba(255,255,255,.85)');
        cg.addColorStop(1, 'rgba(255,255,255,0)');
        ctx.fillStyle = cg;
        ctx.beginPath(); ctx.ellipse(c.x, c.y, c.r * 1.6, c.r * 0.75, 0, 0, 6.2832); ctx.fill();
      }
    } else {
      for (const s of bgState.stars) {
        const tw = 0.35 + 0.65 * Math.abs(Math.sin(t * s.sp + s.ph));
        ctx.beginPath();
        ctx.arc(s.x, s.y, s.r, 0, 6.2832);
        ctx.fillStyle = 'rgba(210,225,255,' + (tw * 0.85).toFixed(3) + ')';
        ctx.fill();
      }
      if (!bgState.shoot && performance.now() > bgState.nextShoot) {
        bgState.shoot = { x: rnd2(w * 0.2, w), y: rnd2(0, h * 0.3), vx: -rnd2(500, 800), vy: rnd2(220, 380), life: 1 };
        bgState.nextShoot = performance.now() + rnd2(3000, 9000);
      }
      if (bgState.shoot) {
        const s = bgState.shoot;
        s.x += s.vx / 60; s.y += s.vy / 60; s.life -= 0.02;
        const grd = ctx.createLinearGradient(s.x, s.y, s.x - s.vx * 0.14, s.y - s.vy * 0.14);
        grd.addColorStop(0, 'rgba(255,255,255,' + Math.max(0, s.life).toFixed(2) + ')');
        grd.addColorStop(1, 'rgba(255,255,255,0)');
        ctx.strokeStyle = grd;
        ctx.lineWidth = 2.2;
        ctx.beginPath();
        ctx.moveTo(s.x, s.y);
        ctx.lineTo(s.x - s.vx * 0.14, s.y - s.vy * 0.14);
        ctx.stroke();
        if (s.life <= 0 || s.x < -80 || s.y > h + 80) bgState.shoot = null;
      }
    }
  }
}

function drawStaticFrame(cv, mode) {
  try {
    const dpr = 1;
    setupCanvas(cv, dpr);
    const w = window.innerWidth, h = window.innerHeight;
    if (mode === 'circles') {
      const rings = [];
      for (let i = 0; i < 22; i++) rings.push({ x: rnd2(0, w), y: rnd2(0, h), r: rnd2(24, 130), a: rnd2(0.05, 0.14), lw: rnd2(1.2, 3) });
      bgState = { rings, w, h };
    } else {
      const stars = [];
      for (let i = 0; i < 150; i++) stars.push({ x: rnd2(0, w), y: rnd2(0, h * 0.94), r: rnd2(0.6, 2.1) });
      bgState = { stars, clouds: [], w, h };
    }
    const ctx = cv.getContext('2d');
    paintBase(ctx, w, h);
    const light = state.prefs.theme === 'light';
    const acc = light ? '47,107,255' : '79,141,255';
    if (mode === 'circles') {
      for (const r of bgState.rings) {
        ctx.beginPath(); ctx.arc(r.x, r.y, r.r, 0, 6.2832);
        ctx.strokeStyle = 'rgba(' + acc + ',' + r.a.toFixed(3) + ')';
        ctx.lineWidth = r.lw; ctx.stroke();
      }
    } else if (!light) {
      for (const s of bgState.stars) {
        ctx.beginPath(); ctx.arc(s.x, s.y, s.r, 0, 6.2832);
        ctx.fillStyle = 'rgba(210,225,255,.7)'; ctx.fill();
      }
    }
  } catch (e) { /* تجاهل */ }
}

function startCanvas() {
  const cv = $('#bgCanvas');
  if (!cv) return;
  if (bgRAF) { cancelAnimationFrame(bgRAF); bgRAF = 0; }
  const mode = state.prefs.bg;
  if (mode === 'none') return;
  if (state.prefs.memsave) { drawStaticFrame(cv, mode); return; }
  bgState = null;
  const loop = () => {
    try { drawFrame(cv, mode); } catch (e) { /* تجاهل */ }
    bgRAF = requestAnimationFrame(loop);
  };
  bgRAF = requestAnimationFrame(loop);
}

/* ═══════════ 8) طبقة الإشارات: MQTT خام + BroadcastChannel ═══════════ */
const enc = new TextEncoder();
const dec = new TextDecoder();

function mqttBytes(...parts) {
  let len = 0;
  for (const p of parts) len += p.length;
  const out = new Uint8Array(len);
  let o = 0;
  for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}
function mqttLen(n) {
  const out = [];
  do {
    let d = n % 128;
    n = Math.floor(n / 128);
    if (n > 0) d |= 0x80;
    out.push(d);
  } while (n > 0);
  return new Uint8Array(out);
}
function mqttStr(s) {
  const b = enc.encode(s);
  return mqttBytes(new Uint8Array([b.length >> 8, b.length & 255]), b);
}
function mqttPacket(type, body) {
  return mqttBytes(new Uint8Array([type]), mqttLen(body.length), body);
}

function mqttConnectPacket(clientId) {
  const vh = new Uint8Array([0x00, 0x04, 0x4D, 0x51, 0x54, 0x54, 0x04, 0x02, 0x00, 0x3C]);
  return mqttPacket(0x10, mqttBytes(vh, mqttStr(clientId)));
}
function mqttSubscribePacket(topic, pid) {
  const body = mqttBytes(new Uint8Array([pid >> 8, pid & 255]), mqttStr(topic), new Uint8Array([0x00]));
  return mqttPacket(0x82, body);
}
function mqttPublishPacket(topic, payloadStr) {
  return mqttPacket(0x30, mqttBytes(mqttStr(topic), enc.encode(payloadStr)));
}

let wsBuf = new Uint8Array(0);
let connackSeen = false;
function wsAppend(chunk) {
  const merged = new Uint8Array(wsBuf.length + chunk.length);
  merged.set(wsBuf, 0);
  merged.set(chunk, wsBuf.length);
  wsBuf = merged;
}
function wsParsePackets() {
  let i = 0;
  while (i + 2 <= wsBuf.length) {
    const type = wsBuf[i] >> 4;
    let rem = 0, mult = 1, j = i + 1, ok = false;
    while (j < wsBuf.length) {
      const b = wsBuf[j];
      rem += (b & 0x7F) * mult;
      mult *= 128;
      j++;
      if (!(b & 0x80)) { ok = true; break; }
    }
    if (!ok || j + rem > wsBuf.length) break;
    const body = wsBuf.subarray(j, j + rem);
    if (type === 2) {
      if (!connackSeen && body.length >= 1 && body[body.length - 1] === 0) {
        connackSeen = true;
        wsAfterConnack();
      }
    } else if (type === 3) {
      const tlen = (body[0] << 8) | body[1];
      const payload = body.subarray(2 + tlen);
      try { handleSignal(dec.decode(payload)); } catch (e) { /* تجاهل */ }
    }
    i = j + rem;
  }
  wsBuf = wsBuf.subarray(i);
}

function signalSend(msg) {
  const payload = JSON.stringify(msg);
  try {
    if (state.ws && state.wsState === 'open') state.ws.send(mqttPublishPacket(TOPIC, payload));
  } catch (e) { /* تجاهل */ }
  try {
    if (state.bc) state.bc.postMessage(payload);
  } catch (e) { /* تجاهل */ }
}

function handleSignal(raw) {
  let m;
  try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || !m.t) return;
  const pid = m.from || m.id;
  if (!pid || pid === state.myId) return;
  const peer = state.peers.get(pid);

  switch (m.t) {
    case 'hi':
      if (!peer) {
        state.peers.set(pid, { id: pid, name: m.name || 'جهاز', code: m.code || '----', lastSeen: Date.now(), connected: false });
        signalSend({ t: 'hi', id: state.myId, name: state.prefs.name, code: state.prefs.code });
        renderPeers();
      } else if (!peer.connected) {
        peer.name = m.name || peer.name;
        peer.code = m.code || peer.code;
        peer.lastSeen = Date.now();
        renderPeers();
      }
      break;
    case 'find':
      if (m.to === state.prefs.code) announcePresence();
      break;
    case 'req':
      if (m.to !== state.myId) break;
      if (peer && peer.connected) break;
      if (state.prefs.autoAccept) acceptPeer(pid);
      else showRequest(pid, m.name || 'جهاز');
      break;
    case 'ok':
      if (m.to !== state.myId) break;
      startConnection(pid, true);
      break;
    case 'sdp':
      if (m.to !== state.myId) break;
      onRemoteSDP(pid, m.kind, m.sdp);
      break;
    case 'ice':
      if (m.to !== state.myId) break;
      onRemoteICE(pid, m.cand);
      break;
    case 'decl':
      if (m.to !== state.myId) break;
      toast('تم رفض طلب الاتصال من ' + (m.name || 'الجهاز'), 'err');
      renderPeers();
      break;
    case 'bye':
      if (m.to && m.to !== state.myId) break;
      if (peer && peer.connected) disconnectPeer(pid, true);
      else if (peer) { state.peers.delete(pid); renderPeers(); }
      break;
    default:
      break;
  }
}

function wsConnect() {
  if (state.wsState === 'connecting') return;
  state.wsState = 'connecting';
  connackSeen = false;
  wsBuf = new Uint8Array(0);
  setConnState('جارٍ الاتصال بخدمة الاكتشاف…', '');
  let ws;
  try { ws = new WebSocket(MQTT_URL, 'mqtt'); }
  catch (e) { scheduleReconnect(); return; }
  ws.binaryType = 'arraybuffer';
  state.ws = ws;

  ws.onopen = () => {
    try { ws.send(mqttConnectPacket('ishare_' + state.myId + '_' + String(Date.now() % 100000))); }
    catch (e) { /* تجاهل */ }
  };
  ws.onmessage = (ev) => {
    try {
      if (typeof ev.data === 'string') { handleSignal(ev.data); return; }
      wsAppend(new Uint8Array(ev.data));
      wsParsePackets();
    } catch (e) { /* تجاهل */ }
  };
  ws.onclose = () => {
    state.wsState = 'closed';
    setConnState('تعذر الوصول لخدمة الاكتشاف — إعادة المحاولة…', 'err');
    scheduleReconnect();
  };
  ws.onerror = () => { try { ws.close(); } catch (e) { /* تجاهل */ } };
}

function wsAfterConnack() {
  state.wsState = 'open';
  setConnState('متصل بخدمة الاكتشاف ✓', 'ok');
  state.backoff = 1000;
  try {
    if (state.ws) state.ws.send(mqttSubscribePacket(TOPIC, 1));
  } catch (e) { /* تجاهل */ }
  announcePresence();
}

function scheduleReconnect() {
  setTimeout(() => { try { wsConnect(); } catch (e) { /* تجاهل */ } }, state.backoff);
  state.backoff = Math.min(state.backoff * 2, 15000);
}

function announcePresence() {
  signalSend({ t: 'hi', id: state.myId, name: state.prefs.name, code: state.prefs.code });
  state.lastHi = Date.now();
}

function setConnState(text, cls) {
  const el = $('#connState');
  if (!el) return;
  el.textContent = text;
  el.className = 'conn-state' + (cls ? ' ' + cls : '');
}

function prunePeers() {
  const now = Date.now();
  let changed = false;
  for (const [id, p] of state.peers) {
    if (!p.connected && now - p.lastSeen > PEER_TTL) { state.peers.delete(id); changed = true; }
  }
  if (changed) renderPeers();
  if (state.wsState === 'open' && now - state.lastHi > PRESENCE_MS) announcePresence();
}

function initTransports() {
  try {
    state.bc = new BroadcastChannel('ishare-v1');
    state.bc.onmessage = (ev) => { try { handleSignal(ev.data); } catch (e) { /* تجاهل */ } };
  } catch (e) { state.bc = null; }

  wsConnect();
  setInterval(prunePeers, 4000);
  setInterval(() => {
    try { if (state.ws && state.wsState === 'open') state.ws.send(new Uint8Array([0xC0, 0x00])); }
    catch (e) { /* تجاهل */ }
  }, 40000);
  window.addEventListener('beforeunload', () => {
    try {
      signalSend({ t: 'bye', id: state.myId });
      for (const p of state.peers.values()) {
        if (p.connected && p.dc) p.dc.send(JSON.stringify({ t: 'bye' }));
      }
    } catch (e) { /* تجاهل */ }
  });
}

/* ═══════════ 9) WebRTC + نقل الملفات + الشات ═══════════ */
function connectToPeer(id) {
  const peer = state.peers.get(id);
  if (!peer) return;
  if (peer.connected) { openChat(id); return; }
  signalSend({ t: 'req', from: state.myId, to: id, name: state.prefs.name });
  toast('تم إرسال طلب الاتصال إلى ' + peer.name + '…');
  renderPeers();
}

function acceptPeer(id) {
  const peer = state.peers.get(id);
  if (!peer) return;
  signalSend({ t: 'ok', from: state.myId, to: id, name: state.prefs.name });
  startConnection(id, false);
}

function declinePeer(id) {
  signalSend({ t: 'decl', from: state.myId, to: id, name: state.prefs.name });
  toast('تم رفض الطلب');
}

function showRequest(id, name) {
  const peer = state.peers.get(id);
  if (!peer) return;
  peer.pending = true;
  renderPeers();
  $('#reqTitle').textContent = name + ' يريد الاتصال بك';
  $('#reqText').textContent = 'سيتمكن من إرسال الملفات والرسائل إليك';
  $('#reqAvatar').textContent = (name || '؟').charAt(0);
  const reqModal = $('#reqModal');
  reqModal.hidden = false;
  $('#reqAccept').onclick = () => {
    reqModal.hidden = true;
    peer.pending = false;
    acceptPeer(id);
  };
  $('#reqReject').onclick = () => {
    reqModal.hidden = true;
    peer.pending = false;
    declinePeer(id);
    renderPeers();
  };
}

function startConnection(id, isInitiator) {
  let peer = state.peers.get(id);
  if (!peer) {
    peer = { id, name: 'جهاز', code: '----', lastSeen: Date.now() };
    state.peers.set(id, peer);
  }
  if (peer.pc) { try { peer.pc.close(); } catch (e) { /* تجاهل */ } }
  peer.connected = false;
  peer.retries = 0;

  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  peer.pc = pc;
  peer.initiator = !!isInitiator;

  pc.onicecandidate = (e) => {
    if (e.candidate) {
      try { signalSend({ t: 'ice', from: state.myId, to: id, cand: e.candidate.toJSON ? e.candidate.toJSON() : e.candidate }); }
      catch (err) { /* تجاهل */ }
    }
  };
  pc.onconnectionstatechange = () => {
    const st = pc.connectionState;
    if (st === 'failed') {
      if ((peer.retries || 0) < 2) {
        peer.retries = (peer.retries || 0) + 1;
        try { pc.restartIce(); } catch (err) { /* تجاهل */ }
        if (peer.initiator) signalSend({ t: 'req', from: state.myId, to: id, name: state.prefs.name });
        else signalSend({ t: 'ok', from: state.myId, to: id, name: state.prefs.name });
      } else {
        disconnectPeer(id, false);
        toast('انقطع الاتصال بـ ' + peer.name, 'err');
      }
    }
  };
  pc.oniceconnectionstatechange = () => {
    const st = pc.iceConnectionState;
    if (st === 'disconnected') {
      setTimeout(() => {
        if (pc.iceConnectionState === 'disconnected') {
          try { pc.restartIce(); } catch (err) { /* تجاهل */ }
        }
      }, 3000);
    }
  };

  if (isInitiator) {
    const dc = pc.createDataChannel('ishare', { ordered: true });
    wireChannel(peer, dc);
    pc.createOffer().then((offer) => pc.setLocalDescription(offer)).then(() => {
      signalSend({ t: 'sdp', from: state.myId, to: id, kind: 'offer', sdp: pc.localDescription });
    }).catch(() => { /* تجاهل */ });
  } else {
    pc.ondatachannel = (e) => wireChannel(peer, e.channel);
  }
  renderPeers();
}

async function onRemoteSDP(id, kind, sdp) {
  let peer = state.peers.get(id);
  if (!peer || !peer.pc) {
    startConnection(id, false);
    peer = state.peers.get(id);
    if (!peer || !peer.pc) return;
  }
  const pc = peer.pc;
  try {
    if (kind === 'offer') {
      await pc.setRemoteDescription(sdp);
      const answer = await pc.createAnswer();
      await pc.setLocalDescription(answer);
      signalSend({ t: 'sdp', from: state.myId, to: id, kind: 'answer', sdp: pc.localDescription });
    } else if (kind === 'answer') {
      if (pc.signalingState === 'have-local-offer') await pc.setRemoteDescription(sdp);
    }
    if (peer.pendingIce && peer.pendingIce.length && pc.remoteDescription) {
      const list = peer.pendingIce.splice(0);
      for (const c of list) {
        try { await pc.addIceCandidate(c); } catch (err) { /* تجاهل */ }
      }
    }
  } catch (e) { /* تجاهل */ }
}

async function onRemoteICE(id, cand) {
  const peer = state.peers.get(id);
  if (!peer || !cand) return;
  if (!peer.pc || !peer.pc.remoteDescription) {
    peer.pendingIce = peer.pendingIce || [];
    peer.pendingIce.push(cand);
    return;
  }
  try { await peer.pc.addIceCandidate(cand); } catch (e) { /* تجاهل */ }
}

function wireChannel(peer, dc) {
  peer.dc = dc;
  dc.binaryType = 'arraybuffer';
  dc.bufferedAmountLowThreshold = LOW_WATER;
  dc.onopen = () => {
    peer.connected = true;
    peer.retries = 0;
    renderPeers();
    toast('تم الاتصال بـ ' + peer.name, 'ok');
    vibrate([30, 40, 30]);
  };
  dc.onclose = () => {
    if (peer.connected) {
      peer.connected = false;
      renderPeers();
      toast('انقطع الاتصال بـ ' + peer.name, 'err');
    }
    for (const [k, t] of state.transfers) {
      if (t.peerId === peer.id && !t.done) {
        state.transfers.delete(k);
        if (t.dir === 'recv') {
          idbClearRange('chunks', t.id);
          toast('أُلغي استلام «' + t.name + '»', 'err');
        }
      }
    }
    renderTransfers();
  };
  dc.onerror = () => { /* تجاهل */ };
  dc.onmessage = (e) => {
    try {
      if (typeof e.data === 'string') onControl(peer, e.data);
      else onChunk(peer, e.data);
    } catch (err) { /* تجاهل */ }
  };
}

function disconnectPeer(id, silent) {
  const peer = state.peers.get(id);
  if (!peer) return;
  try { if (peer.dc) peer.dc.close(); } catch (e) { /* تجاهل */ }
  try { if (peer.pc) peer.pc.close(); } catch (e) { /* تجاهل */ }
  state.peers.delete(id);
  if (state.chatPeer === id) { state.chatPeer = null; closeModal('chatModal'); }
  if (state.selected === id) state.selected = null;
  renderPeers();
  if (!silent) toast('تم قطع الاتصال بـ ' + (peer.name || 'الجهاز'));
}

function onControl(peer, raw) {
  let m;
  try { m = JSON.parse(raw); } catch (e) { return; }
  if (!m || !m.t) return;
  if (m.t === 'bye') { disconnectPeer(peer.id, true); toast('خرج ' + peer.name + ' من الشبكة'); return; }
  if (m.t === 'chat') { addChatMsg(peer, m.text, false); if (state.chatPeer !== peer.id) toast('رسالة جديدة من ' + peer.name); return; }
  if (m.t === 'meta') {
    const tx = 'r' + uid(6);
    const t = {
      id: tx, remoteTx: m.tx, dir: 'recv', peerId: peer.id, peerName: peer.name,
      name: String(m.name || 'ملف'), size: Number(m.size) || 0, mime: m.mime || 'application/octet-stream',
      got: 0, done: false, started: Date.now(), buf: [], bufLen: 0, seq: 0, speed: 0, lastT: Date.now()
    };
    if (t.size > MAX_FILE) {
      try { peer.dc.send(JSON.stringify({ t: 'too-big', tx: m.tx })); } catch (e) { /* تجاهل */ }
      toast('الملف أكبر من 500 م.ب — مرفوض', 'err');
      return;
    }
    state.transfers.set(tx, t);
    showTransferPanel();
    renderTransfers();
    return;
  }
  if (m.t === 'file-done') {
    let target = null;
    for (const t of state.transfers.values()) {
      if (t.dir === 'recv' && t.remoteTx === m.tx) { target = t; break; }
    }
    if (target) finishRecv(peer, target.id);
  }
}

function onChunk(peer, data) {
  for (const t of state.transfers.values()) {
    if (t.dir === 'recv' && !t.done && t.peerId === peer.id) {
      const u8 = new Uint8Array(data.byteLength);
      u8.set(new Uint8Array(data));
      t.buf.push(u8);
      t.bufLen += u8.byteLength;
      t.got += u8.byteLength;
      if (t.bufLen >= RECV_FLUSH) flushRecv(t);
      return;
    }
  }
}

async function flushRecv(t) {
  if (!t.bufLen) return;
  const blob = new Blob(t.buf);
  await idbPut('chunks', { tx: t.id, seq: t.seq, data: blob });
  t.seq++;
  t.buf = [];
  t.bufLen = 0;
}

async function finishRecv(peer, tx) {
  const t = state.transfers.get(tx);
  if (!t || t.done) return;
  t.done = true;
  await flushRecv(t);
  const chunks = await idbGetRange('chunks', tx);
  const blob = new Blob(chunks.map((c) => c.data), { type: t.mime });
  await idbClearRange('chunks', tx);
  const rec = {
    id: 'f' + uid(8), name: t.name, size: t.size, mime: t.mime,
    from: t.peerName, ts: Date.now(), blob
  };
  await idbPut('files', rec);
  state.history.unshift(rec);
  if (state.history.length > 60) state.history.pop();
  renderHistory();
  downloadBlob(rec.id, rec.blob, rec.name);
  const speed = (Date.now() - t.started) > 500 ? fmtBytes(t.size / ((Date.now() - t.started) / 1000)) + '/ث' : '';
  toast('وصل ملف: ' + t.name + (speed ? ' (' + speed + ')' : ''), 'ok');
  vibrate([20, 30, 20]);
  setTimeout(() => { state.transfers.delete(tx); renderTransfers(); }, 4000);
  renderTransfers();
}

function downloadBlob(key, blob, name) {
  const url = getUrl(key, () => URL.createObjectURL(blob));
  const a = document.createElement('a');
  a.href = url;
  a.download = name || 'file';
  document.body.appendChild(a);
  a.click();
  setTimeout(() => { try { a.remove(); } catch (e) { /* تجاهل */ } }, 400);
}

/* ═══════════ 10) إرسال الملفات (طابور) ═══════════ */
function pickFiles() {
  const peer = currentPeer();
  if (!peer || !peer.connected) { toast('اختر جهازًا متصلًا أولًا', 'err'); return; }
  $('#fileInput').value = '';
  $('#fileInput').click();
}

async function onFilesChosen(ev) {
  const files = Array.from(ev.target.files || []);
  if (!files.length) return;
  const peer = currentPeer();
  if (!peer || !peer.connected) { toast('لا يوجد جهاز متصل', 'err'); return; }
  const valid = [];
  for (const f of files) {
    if (f.size > MAX_FILE) { toast('«' + f.name + '» أكبر من 500 م.ب — تم تجاهله', 'err'); continue; }
    valid.push(f);
  }
  if (!valid.length) return;
  peer.queue = (peer.queue || []).concat(valid);
  if (!peer.sending) sendQueue(peer);
  toast('أُضيف ' + valid.length + ' ملف لطابور الإرسال');
}

async function sendQueue(peer) {
  peer.sending = true;
  while (peer.queue && peer.queue.length && peer.connected) {
    const f = peer.queue.shift();
    try { await sendOne(peer, f); }
    catch (e) { toast('فشل إرسال ' + (f && f.name ? f.name : 'ملف'), 'err'); break; }
  }
  peer.sending = false;
  renderTransfers();
}

function waitDrain(dc) {
  return new Promise((resolve) => {
    let to = null;
    const done = () => { dc.removeEventListener('bufferedamountlow', done); if (to) clearTimeout(to); resolve(); };
    dc.addEventListener('bufferedamountlow', done);
    to = setTimeout(() => {
      dc.removeEventListener('bufferedamountlow', done);
      resolve();
    }, 5000);
  });
}

async function sendOne(peer, file) {
  const dc = peer.dc;
  if (!dc || dc.readyState !== 'open') throw new Error('closed');
  const tx = 's' + uid(6);
  const t = {
    id: tx, dir: 'send', peerId: peer.id, peerName: peer.name,
    name: file.name, size: file.size, mime: file.type || 'application/octet-stream',
    sent: 0, done: false, started: Date.now(), speed: 0, lastSent: 0, lastT: Date.now()
  };
  state.transfers.set(tx, t);
  showTransferPanel();
  renderTransfers();
  setPeerTransfering(peer.id, true);

  dc.send(JSON.stringify({ t: 'meta', tx, name: file.name, size: file.size, mime: t.mime }));

  let offset = 0;
  while (offset < file.size && dc.readyState === 'open' && peer.connected) {
    if (dc.bufferedAmount > HIGH_WATER) await waitDrain(dc);
    if (dc.readyState !== 'open') throw new Error('closed');
    const slice = file.slice(offset, Math.min(offset + CHUNK_SIZE, file.size));
    const buf = await slice.arrayBuffer();
    dc.send(buf);
    offset += buf.byteLength;
    t.sent = offset;
  }
  if (dc.readyState === 'open') dc.send(JSON.stringify({ t: 'file-done', tx }));

  t.done = true;
  setPeerTransfering(peer.id, false);
  const dur = (Date.now() - t.started) / 1000;
  toast('أُرسل «' + file.name + '»' + (dur > 0.6 ? ' بسرعة ' + fmtBytes(file.size / dur) + '/ث' : ''), 'ok');
  renderTransfers();
  setTimeout(() => { state.transfers.delete(tx); renderTransfers(); }, 4000);
}

function setPeerTransfering(id, on) {
  const peer = state.peers.get(id);
  if (peer) peer.transfering = on;
  renderPeers();
}

/* مؤقت حساب السرعة */
setInterval(() => {
  const now = Date.now();
  let changed = false;
  for (const t of state.transfers.values()) {
    if (t.done) continue;
    const dt = (now - t.lastT) / 1000;
    if (dt >= 0.4) {
      const cur = t.dir === 'send' ? t.sent : t.got;
      const inst = (cur - (t.lastSent || 0)) / dt;
      t.speed = t.speed ? t.speed * 0.55 + inst * 0.45 : inst;
      t.lastSent = cur;
      t.lastT = now;
      changed = true;
    }
  }
  if (changed && !$('#transferPanel').hidden) renderTransfers();
}, 450);

/* ═══════════ 11) الشات ═══════════ */
function currentPeer() {
  if (state.selected && state.peers.get(state.selected) && state.peers.get(state.selected).connected) {
    return state.peers.get(state.selected);
  }
  for (const p of state.peers.values()) if (p.connected) return p;
  return null;
}

function openChat(id) {
  const peer = state.peers.get(id);
  if (!peer || !peer.connected) return;
  state.selected = id;
  state.chatPeer = id;
  peer.msgs = peer.msgs || [];
  $('#chatAvatar').textContent = (peer.name || '؟').charAt(0);
  $('#chatPeerName').textContent = peer.name;
  $('#chatPeerState').textContent = 'متصل ✓';
  $('#chatPeerState').className = '';
  renderChat();
  openModal('chatModal');
  $('#pickHint').textContent = 'الهدف: ' + peer.name;
  $('#pickHint').className = 'hint ready';
}

function addChatMsg(peer, text, mine) {
  peer.msgs = peer.msgs || [];
  peer.msgs.push({ text, mine, ts: Date.now() });
  if (state.chatPeer === peer.id) renderChat();
  if (!mine) vibrate(15);
}

function sendChat() {
  const peer = state.chatPeer ? state.peers.get(state.chatPeer) : null;
  if (!peer || !peer.dc || peer.dc.readyState !== 'open') { toast('الاتصال غير جاهز', 'err'); return; }
  const input = $('#chatInput');
  const text = (input.value || '').trim();
  if (!text) return;
  try { peer.dc.send(JSON.stringify({ t: 'chat', text })); } catch (e) { toast('فشل الإرسال', 'err'); return; }
  addChatMsg(peer, text, true);
  input.value = '';
}

function copyText(text) {
  const done = () => toast('تم النسخ إلى الحافظة', 'ok');
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).then(done).catch(() => { fallbackCopy(text); done(); });
  } else { fallbackCopy(text); done(); }
}
function fallbackCopy(text) {
  try {
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.select();
    document.execCommand('copy');
    ta.remove();
  } catch (e) { /* تجاهل */ }
}

/* ═══════════ 12) الدوال العرضية ═══════════ */
function renderPeers() {
  const orbit = $('#orbit');
  const nearList = $('#nearList');
  const connected = [];
  const near = [];
  for (const p of state.peers.values()) {
    if (p.connected) connected.push(p);
    else near.push(p);
  }
  near.sort((a, b) => b.lastSeen - a.lastSeen);

  $$('.orbit-peer', orbit).forEach((el) => el.remove());
  const n = connected.length;
  connected.forEach((p, i) => {
    const el = document.createElement('button');
    el.type = 'button';
    el.className = 'orbit-peer' + (p.transfering ? ' transfering' : '');
    el.dataset.id = p.id;
    const R = Math.max(60, orbit.clientWidth * 0.5 - 66);
    const ang = (-90 + i * (360 / Math.max(n, 1))) * Math.PI / 180;
    const x = Math.cos(ang) * R, y = Math.sin(ang) * R;
    el.style.transform = 'translate(' + x.toFixed(1) + 'px,' + y.toFixed(1) + 'px)';
    el.innerHTML = '<span class="op-dot">' + esc((p.name || '؟').charAt(0)) + '</span><span class="op-name">' + esc(p.name) + '</span>';
    el.addEventListener('click', () => { openChat(p.id); renderPeers(); });
    orbit.appendChild(el);
  });

  nearList.innerHTML = '';
  if (!near.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-near';
    empty.innerHTML = '<svg viewBox="0 0 24 24" width="34" height="34" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"><circle cx="12" cy="12" r="9"/><path d="M12 7v5l3.2 2"/></svg><span>في انتظار ظهور الأجهزة… افتح iShare على الجهاز الآخر</span>';
    nearList.appendChild(empty);
    return;
  }
  near.forEach((p, i) => {
    const row = document.createElement('div');
    row.className = 'near-row';
    row.style.animationDelay = (i * 0.05).toFixed(2) + 's';
    const info = document.createElement('div');
    info.className = 'near-info';
    info.innerHTML = '<div class="near-name">' + esc(p.name) + '</div><div class="near-sub">رمز ' + esc(p.code) + '</div>';
    const cta = document.createElement('button');
    cta.type = 'button';
    cta.className = 'near-cta';
    cta.textContent = p.pending ? 'بانتظار الرد…' : 'اتصال';
    cta.disabled = !!p.pending;
    cta.addEventListener('click', (e) => { e.stopPropagation(); connectToPeer(p.id); });
    const av = document.createElement('div');
    av.className = 'near-avatar';
    av.textContent = (p.name || '؟').charAt(0);
    row.appendChild(av);
    row.appendChild(info);
    row.appendChild(cta);
    row.addEventListener('click', () => connectToPeer(p.id));
    nearList.appendChild(row);
  });
}

/* عرض لوحة النقل */
function showTransferPanel() {
  const panel = $('#transferPanel');
  if (panel.hidden) { panel.hidden = false; panel.classList.remove('closing'); }
}
function hideTransferPanel() {
  const panel = $('#transferPanel');
  panel.classList.add('closing');
  setTimeout(() => { panel.hidden = true; panel.classList.remove('closing'); }, 300);
}
function renderTransfers() {
  const list = $('#transferList');
  const items = Array.from(state.transfers.values()).reverse();
  if (!items.length) { hideTransferPanel(); return; }
  showTransferPanel();
  let html = '';
  for (const t of items) {
    const cur = t.dir === 'send' ? t.sent : t.got;
    const pct = t.size ? clamp((cur / t.size) * 100, 0, 100) : 0;
    const eta = t.speed > 0 && !t.done ? fmtETA((t.size - cur) / t.speed) : '—';
    const dirLabel = t.dir === 'send' ? 'إرسال ← ' + t.peerName : 'استلام ← ' + t.peerName;
    const status = t.done
      ? '<span class="tp-status ok">' + (t.dir === 'send' ? 'تم الإرسال ✓' : 'وصل ✓') + '</span>'
      : '<span class="tp-status">' + dirLabel + '</span>';
    html += '<div class="tp-item">'
      + '<div class="tp-line1"><span class="tp-name">' + esc(t.name) + '</span>'
      + (t.done ? '' : '<span class="tp-speed">' + fmtSpeed(t.speed) + '</span><span class="tp-eta">' + eta + '</span>')
      + '</div>'
      + '<div class="tp-bar"><div class="tp-fill' + (t.done ? ' done' : '') + '" style="width:' + pct.toFixed(1) + '%"></div></div>'
      + '<div class="tp-sub">' + status + '<span class="tnum">' + fmtBytes(cur) + ' / ' + fmtBytes(t.size) + ' — ' + pct.toFixed(0) + '%</span></div>'
      + '</div>';
  }
  list.innerHTML = html;
}

/* عرض السجل */
function renderHistory() {
  const list = $('#historyList');
  const empty = $('#historyEmpty');
  list.innerHTML = '';
  if (!state.history.length) { empty.hidden = false; return; }
  empty.hidden = true;
  state.history.forEach((f, i) => {
    const card = document.createElement('div');
    card.className = 'row-card';
    card.style.animationDelay = (Math.min(i, 10) * 0.04).toFixed(2) + 's';
    const d = new Date(f.ts || Date.now());
    const sub = fmtBytes(f.size) + ' • ' + (f.from || '') + ' • ' + d.toLocaleDateString('ar-EG') + ' ' + d.toLocaleTimeString('ar-EG', { hour: '2-digit', minute: '2-digit' });
    card.innerHTML = '<div class="row-thumb"><span style="font-size:22px">' + fileIcon(f.name, f.mime) + '</span></div>'
      + '<div class="row-info"><div class="row-title">' + esc(f.name) + '</div><div class="row-sub">' + esc(sub) + '</div></div>';
    const actions = document.createElement('div');
    actions.className = 'row-actions';
    const dl = document.createElement('button');
    dl.type = 'button';
    dl.className = 'icon-btn icon-btn-accent';
    dl.setAttribute('aria-label', 'تحميل');
    dl.innerHTML = '<svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 4v12M12 16l-4.5-4.5M12 16l4.5-4.5"/><path d="M4 19h16"/></svg>';
    dl.addEventListener('click', () => downloadBlob(f.id, f.blob, f.name));
    const del = document.createElement('button');
    del.type = 'button';
    del.className = 'icon-btn';
    del.setAttribute('aria-label', 'حذف');
    del.innerHTML = '<svg viewBox="0 0 24 24" width="19" height="19" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 7h16M10 4h4M7 7l1 13h8l1-13M10 11v5M14 11v5"/></svg>';
    del.addEventListener('click', async () => {
      const ok = await askConfirm('حذف الملف', 'سيتم حذف «' + f.name + '» من السجل نهائيًا', 'حذف');
      if (!ok) return;
      dropUrl(f.id);
      await idbDelete('files', f.id);
      state.history = state.history.filter((x) => x.id !== f.id);
      renderHistory();
      toast('تم الحذف');
    });
    actions.appendChild(dl);
    actions.appendChild(del);
    card.appendChild(actions);
    list.appendChild(card);
  });
}

/* عرض الشات */
function renderChat() {
  const peer = state.chatPeer ? state.peers.get(state.chatPeer) : null;
  const box = $('#chatMsgs');
  if (!peer) { box.innerHTML = '<div class="chat-empty">لا يوجد اتصال</div>'; return; }
  peer.msgs = peer.msgs || [];
  if (!peer.msgs.length) {
    box.innerHTML = '<div class="chat-empty">أرسل أول رسالة 👋</div>';
    return;
  }
  box.innerHTML = '';
  peer.msgs.forEach((m) => {
    const wrap = document.createElement('div');
    wrap.className = 'chat-msg' + (m.mine ? ' mine' : '');
    const b = document.createElement('div');
    b.className = 'bubble';
    b.textContent = m.text;
    const cp = document.createElement('button');
    cp.type = 'button';
    cp.className = 'bubble-copy';
    cp.textContent = 'نسخ';
    cp.addEventListener('click', () => copyText(m.text));
    wrap.appendChild(b);
    wrap.appendChild(cp);
    box.appendChild(wrap);
  });
  box.scrollTop = box.scrollHeight;
}

/* ═══════════ 13) ربط الأحداث ═══════════ */
function switchTab(tab) {
  state.tab = tab;
  saveSession();
  $$('.tab').forEach((b) => {
    const on = b.dataset.tab === tab;
    b.classList.toggle('active', on);
    b.setAttribute('aria-selected', on ? 'true' : 'false');
  });
  $('.tabs').setAttribute('data-active', tab);
  $$('.view').forEach((v) => v.classList.remove('active'));
  $('#view-' + tab).classList.add('active');
}

function bindEvents() {
  $$('.tab').forEach((b) => b.addEventListener('click', () => switchTab(b.dataset.tab)));

  $('#settingsBtn').addEventListener('click', () => openModal('settingsSheet'));
  $('#pickFilesBtn').addEventListener('click', pickFiles);
  $('#fileInput').addEventListener('change', onFilesChosen);

  $('#tpMinBtn').addEventListener('click', hideTransferPanel);

  $('#chatSend').addEventListener('click', sendChat);
  $('#chatInput').addEventListener('keydown', (e) => { if (e.key === 'Enter') sendChat(); });
  $('#chatFilesBtn').addEventListener('click', () => {
    if (state.chatPeer && state.peers.get(state.chatPeer) && state.peers.get(state.chatPeer).connected) {
      closeModal('chatModal');
      $('#fileInput').value = '';
      $('#fileInput').click();
    }
  });

  $('#codeBtn').addEventListener('click', async () => {
    const code = await askInput('اتصال برمز', 'أدخل رمز الجهاز الآخر (4 أرقام)', '');
    if (!code) return;
    const c = code.replace(/\D/g, '');
    if (c.length !== 4) { toast('الرمز يجب أن يكون 4 أرقام', 'err'); return; }
    if (c === state.prefs.code) { toast('هذا رمز جهازك نفسه', 'err'); return; }
    let found = null;
    for (const p of state.peers.values()) if (p.code === c) { found = p; break; }
    if (found) { connectToPeer(found.id); return; }
    signalSend({ t: 'find', from: state.myId, to: c, name: state.prefs.name });
    toast('جارٍ البحث عن رمز ' + c + '…');
  });

  $('#rowName').addEventListener('click', async () => {
    const v = await askInput('اسم الجهاز', 'اكتب اسمًا يظهر للأجهزة الأخرى', state.prefs.name);
    if (!v) return;
    const name = v.trim().slice(0, 30);
    if (!name) return;
    state.prefs.name = name;
    savePrefs();
    applyPrefs();
    announcePresence();
    toast('تم تغيير الاسم إلى ' + name, 'ok');
  });

  buildPicker('#rowTheme', '#themeValue', THEME_LABELS, 'theme', (v) => { state.prefs.theme = v; });
  buildPicker('#rowBg', '#bgValue', BG_LABELS, 'bg', (v) => { state.prefs.bg = v; });
  buildPicker('#rowGlass', '#glassValue', GLASS_LABELS, 'glass', (v) => { state.prefs.glass = v; });

  $('#rowAuto').addEventListener('click', () => {
    state.prefs.autoAccept = !state.prefs.autoAccept;
    savePrefs(); applyPrefs();
  });
  $('#rowMem').addEventListener('click', () => {
    state.prefs.memsave = !state.prefs.memsave;
    savePrefs(); applyPrefs();
    toast(state.prefs.memsave ? 'وضع توفير الذاكرة مفعّل' : 'تم إلغاء توفير الذاكرة', 'ok');
  });

  $$('[data-close]').forEach((b) => b.addEventListener('click', () => closeModal(b.dataset.close)));

  $$('.sheet-backdrop, .modal-backdrop').forEach((bk) => {
    bk.addEventListener('click', (e) => { if (e.target === bk) closeModal(bk.id); });
  });

  $('#inputOk').addEventListener('click', () => {
    const v = $('#inputField').value;
    closeModal('inputModal');
    if (inputResolve) { inputResolve(v); inputResolve = null; }
  });
  $('#inputField').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') {
      const v = $('#inputField').value;
      closeModal('inputModal');
      if (inputResolve) { inputResolve(v); inputResolve = null; }
    }
  });
  $('#confirmOk').addEventListener('click', () => {
    closeModal('confirmModal');
    if (confirmResolve) { confirmResolve(true); confirmResolve = null; }
  });

  window.addEventListener('resize', () => {
    if (state.prefs.bg !== 'none') {
      if (state.prefs.memsave) drawStaticFrame($('#bgCanvas'), state.prefs.bg);
      else bgState = null;
    }
    renderPeers();
  });
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && state.prefs.bg !== 'none' && !state.prefs.memsave) startCanvas();
  });
}

function buildPicker(rowSel, valSel, labels, key, setter) {
  const row = $(rowSel);
  const menu = document.createElement('div');
  menu.className = 'picker-menu';
  Object.entries(labels).forEach(([v, label]) => {
    const opt = document.createElement('button');
    opt.type = 'button';
    opt.className = 'picker-opt';
    opt.dataset.v = v;
    opt.innerHTML = '<span>' + esc(label) + '</span><span class="check">✓</span>';
    opt.addEventListener('click', () => {
      setter(v);
      savePrefs();
      applyPrefs();
      refreshPickerMenus();
      menu.classList.remove('open');
      vibrate(8);
    });
    menu.appendChild(opt);
  });
  row.after(menu);
  row.addEventListener('click', () => {
    const wasOpen = menu.classList.contains('open');
    $$('.picker-menu').forEach((m) => m.classList.remove('open'));
    if (!wasOpen) { refreshPickerMenus(); menu.classList.add('open'); }
  });
  menu._valSel = valSel;
  menu._key = key;
  menu._labels = labels;
}

function refreshPickerMenus() {
  $$('.picker-menu').forEach((m) => {
    const cur = state.prefs[m._key];
    $$('.picker-opt', m).forEach((o) => o.classList.toggle('selected', o.dataset.v === cur));
    const valEl = $(m._valSel);
    if (valEl) valEl.textContent = m._labels[cur] || valEl.textContent;
  });
  $('#rowGlass').setAttribute('data-disabled', state.prefs.bg === 'none' ? '1' : '0');
}

/* ═══════════ 14) تحديث الـ PWA ═══════════ */
let pendingWorker = null;
let reloaded = false;

function initSW() {
  $('#verLabel').textContent = APP_VERSION.replace('ishare-', '');
  if (!('serviceWorker' in navigator)) return;
  navigator.serviceWorker.register('sw.js').then((reg) => {
    const checkWaiting = () => {
      if (reg.waiting && navigator.serviceWorker.controller) {
        pendingWorker = reg.waiting;
        $('#updateBar').hidden = false;
      }
    };
    checkWaiting();
    reg.addEventListener('updatefound', () => {
      const nw = reg.installing;
      if (!nw) return;
      nw.addEventListener('statechange', () => {
        if (nw.state === 'installed' && navigator.serviceWorker.controller) {
          pendingWorker = nw;
          $('#updateBar').hidden = false;
        }
      });
    });
  }).catch(() => { /* تجاهل */ });

  navigator.serviceWorker.addEventListener('controllerchange', () => {
    if (reloaded) return;
    reloaded = true;
    setTimeout(() => location.reload(), 150);
  });

  $('#updateBtn').addEventListener('click', () => {
    if (!pendingWorker) return;
    pendingWorker.postMessage({ type: 'SKIP_WAITING' });
    toast('جارٍ تحديث التطبيق…');
  });
}

/* ═══════════ 15) init ═══════════ */
async function init() {
  loadPrefs();
  loadSession();
  applyPrefs();
  bindEvents();
  await initIDB();
  await loadHistory();
  switchTab(state.tab);
  initTransports();
  initSW();
  renderPeers();
  setTimeout(() => toast('أهلًا بك في iShare — شارك ملفاتك بسرعة 👋'), 700);
}

init();
