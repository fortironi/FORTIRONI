// Zona Final – servidor multiplayer (sem dependências, só Node.js >= 16)
// Uso: node server.js   (porta 3000, ou a variável PORT)
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto');
const PORT = process.env.PORT || 3005;
const MAX_PLAYERS = 10;
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const RELAY = new Set(['p', 'shot', 'hit', 'dead', 'take', 'cx', 'bots', 'bhit', 'bd']);
const rooms = new Map();
let nextId = 1;

const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html') {
    fs.readFile(path.join(__dirname, 'index.html'), (err, data) => {
      if (err) { res.writeHead(500); res.end('index.html não encontrado'); return; }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(data);
    });
  } else if (u === '/health') { res.writeHead(200); res.end('ok'); }
  else { res.writeHead(404); res.end('404'); }
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (req.url.split('?')[0] !== '/ws' || !key) { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  new Client(socket);
});

class Client {
  constructor(socket) {
    this.socket = socket; this.buf = Buffer.alloc(0); this.frag = []; this.alive = true;
    this.id = nextId++; this.name = 'Jogador'; this.room = null; this.cnt = 0; this.cntT = Date.now();
    socket.setNoDelay(true);
    socket.on('data', d => this.onData(d));
    socket.on('close', () => this.close());
    socket.on('error', () => this.close());
  }
  onData(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    if (this.buf.length > 2e6) return this.kill();
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = !!(b[0] & 128), op = b[0] & 15, masked = !!(b[1] & 128);
      let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 1e6) return this.kill();
      const need = off + (masked ? 4 : 0) + len;
      if (b.length < need) return;
      let payload = Buffer.from(b.subarray(off + (masked ? 4 : 0), need));
      if (masked) { const mk = b.subarray(off, off + 4); for (let i = 0; i < payload.length; i++) payload[i] ^= mk[i & 3]; }
      this.buf = b.subarray(need);
      this.frame(fin, op, payload);
    }
  }
  frame(fin, op, payload) {
    if (op === 8) { this.sendRaw(0x88, Buffer.alloc(0)); return this.kill(); }
    if (op === 9) { this.sendRaw(0x8A, payload); return; }
    if (op === 10) return;
    if (op === 1 || op === 2) this.frag = [];
    this.frag.push(payload);
    if (fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = []; this.onMessage(s); }
  }
  sendRaw(b0, payload) {
    if (!this.alive || this.socket.destroyed) return;
    const n = payload.length; let h;
    if (n < 126) h = Buffer.from([b0, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = b0; h[1] = 126; h.writeUInt16BE(n, 2); }
    else { h = Buffer.alloc(10); h[0] = b0; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
    try { this.socket.write(Buffer.concat([h, payload])); } catch (e) {}
  }
  send(o) { this.sendRaw(0x81, Buffer.from(typeof o === 'string' ? o : JSON.stringify(o))); }
  kill() { try { this.socket.destroy(); } catch (e) {} this.close(); }
  onMessage(s) {
    const now = Date.now();
    if (now - this.cntT > 1000) { this.cntT = now; this.cnt = 0; }
    if (++this.cnt > 300) return;           // limite de mensagens por segundo
    let m; try { m = JSON.parse(s); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    const room = this.room;
    if (m.t === 'create' && !room) {
      this.name = cleanName(m.name);
      let code; do { code = genCode(); } while (rooms.has(code));
      const r = { code, players: new Map(), host: this.id, started: false };
      rooms.set(code, r); this.join(r);
    } else if (m.t === 'join' && !room) {
      this.name = cleanName(m.name);
      const r = rooms.get(String(m.code || '').toUpperCase().trim());
      if (!r) return this.send({ t: 'err', m: 'Sala não encontrada. Confira o código.' });
      if (r.started) return this.send({ t: 'err', m: 'A partida dessa sala já começou.' });
      if (r.players.size >= MAX_PLAYERS) return this.send({ t: 'err', m: 'Sala cheia (máx. ' + MAX_PLAYERS + ').' });
      this.join(r);
    } else if (m.t === 'start' && room) {
      if (room.host !== this.id || room.started) return;
      room.started = true;
      broadcast(room, { t: 'go', a: Math.random() * Math.PI * 2, off: (Math.random() - .5) * 300, zs: (Math.random() * 1e9) | 0, players: list(room) });
    } else if (room && room.started && RELAY.has(m.t)) {
      m.from = this.id;
      const out = JSON.stringify(m);
      if (m.to === 'host') { const h = room.players.get(room.host); if (h && h !== this) h.send(out); }
      else if (typeof m.to === 'number') { const p = room.players.get(m.to); if (p) p.send(out); }
      else for (const p of room.players.values()) if (p !== this) p.send(out);
    }
  }
  join(r) {
    this.room = r; r.players.set(this.id, this);
    this.send({ t: 'joined', code: r.code, id: this.id, host: r.host, players: list(r) });
    for (const p of r.players.values()) if (p !== this) p.send({ t: 'players', host: r.host, players: list(r) });
  }
  close() {
    if (!this.alive) return; this.alive = false;
    const r = this.room; if (!r) return;
    r.players.delete(this.id);
    if (!r.players.size) { rooms.delete(r.code); return; }
    broadcast(r, { t: 'left', id: this.id });
    if (r.host === this.id) {
      r.host = Math.min(...r.players.keys());
      broadcast(r, { t: 'host', id: r.host });
    }
    if (!r.started) broadcast(r, { t: 'players', host: r.host, players: list(r) });
  }
}
function list(r) { return [...r.players.values()].map(p => ({ id: p.id, name: p.name })); }
function broadcast(r, o) { const s = JSON.stringify(o); for (const p of r.players.values()) p.send(s); }
function cleanName(n) { n = String(n || '').replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 14); return n || 'Jogador'; }
function genCode() { const a = 'ABCDEFGHJKLMNPQRSTUVWXYZ'; let c = ''; for (let i = 0; i < 4; i++) c += a[Math.floor(Math.random() * a.length)]; return c; }

// ping periódico para o host da nuvem não derrubar conexões ociosas
setInterval(() => { for (const r of rooms.values()) for (const p of r.players.values()) p.sendRaw(0x89, Buffer.alloc(0)); }, 25000);

server.listen(PORT, () => {
  console.log('Zona Final rodando na porta ' + PORT + '  →  http://localhost:' + PORT);
});
