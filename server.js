/**
 * Island Capture — Multiplayer Server
 * Pure Node.js, zero dependencies
 * Run: node server.js
 *
 * DEPLOYMENT: Backend on Render, Frontend on Vercel
 *
 * Environment variables (set in Render dashboard):
 *   PORT            — injected automatically by Render (do NOT hardcode)
 *   ALLOWED_ORIGIN  — your Vercel URL, e.g. https://island-capture.vercel.app
 *                     Accepts comma-separated list for multiple origins.
 *                     Defaults to * (all origins) if not set — safe for dev,
 *                     but always set it in production.
 *
 * NOTE (Render free tier): The service spins down after ~15 min of inactivity.
 * The first WebSocket connection after spin-down will take ~30 s to respond
 * while Render cold-starts the instance. This is normal on the free plan.
 */

const http   = require('http');
const crypto = require('crypto');

// ── Config ───────────────────────────────────────────────────
// Render injects PORT at runtime — never hardcode on Render or the
// service will bind to the wrong port and fail its health checks.
const PORT = process.env.PORT || 3000;

// Comma-separated list of allowed origins, e.g.:
//   ALLOWED_ORIGIN=https://island-capture.vercel.app
// Leave unset (or set to *) to allow all origins during local dev.
const RAW_ORIGINS  = (process.env.ALLOWED_ORIGIN || '*').split(',').map(s => s.trim());
const ALLOW_ALL    = RAW_ORIGINS.includes('*');

function isAllowedOrigin(origin) {
  if (ALLOW_ALL) return true;
  if (!origin)   return false;          // no Origin header → reject in prod
  return RAW_ORIGINS.includes(origin);
}

// Build the Access-Control-Allow-Origin value for a given request origin.
// We must echo back the exact origin (not '*') when credentials are involved.
function corsOriginHeader(reqOrigin) {
  if (ALLOW_ALL)                      return '*';
  if (isAllowedOrigin(reqOrigin))     return reqOrigin;
  return null;   // origin not allowed
}

// Standard CORS headers added to every HTTP response
function corsHeaders(reqOrigin) {
  const origin = corsOriginHeader(reqOrigin);
  if (!origin) return null;   // caller should respond 403
  return {
    'Access-Control-Allow-Origin':      origin,
    'Access-Control-Allow-Methods':     'GET, OPTIONS',
    'Access-Control-Allow-Headers':     'Content-Type, Upgrade, Connection, Sec-WebSocket-Key, Sec-WebSocket-Version',
    'Access-Control-Allow-Credentials': 'true',
    'Vary':                             'Origin',
  };
}

// ── Room store ───────────────────────────────────────────────
const rooms = new Map();

function genCode() {
  return crypto.randomBytes(3).toString('hex').toUpperCase(); // e.g. "A3F9B2"
}

// Expire rooms older than 1 hour
function cleanRooms() {
  const now = Date.now();
  const expired = [];
  for (const [code, room] of rooms) {
    if (now - room.created > 60 * 60 * 1000) expired.push(code);
  }
  expired.forEach(code => {
    console.log(`[Room ${code}] Expired, removing`);
    rooms.delete(code);
  });
}
setInterval(cleanRooms, 5 * 60 * 1000);

// ── HTTP server ──────────────────────────────────────────────
// Frontend is served by Vercel — this server only handles:
//   GET /         → health check (Render uses this to verify the service is up)
//   GET /health   → same health check
//   OPTIONS *     → CORS preflight
const server = http.createServer((req, res) => {
  const reqOrigin = req.headers['origin'];
  const headers   = corsHeaders(reqOrigin);

  // Reject requests from disallowed origins in production
  if (!headers) {
    res.writeHead(403, { 'Content-Type': 'text/plain' });
    res.end(`Origin "${reqOrigin}" not allowed.`);
    return;
  }

  // Handle CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, headers);
    res.end();
    return;
  }

  // Health check — Render pings GET / to confirm the service is alive
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { ...headers, 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      service: 'Island Capture Multiplayer Server',
      rooms: rooms.size,
      uptime: Math.floor(process.uptime()) + 's',
    }));
    return;
  }

  res.writeHead(404, headers);
  res.end('Not found');
});

// ── WebSocket upgrade ────────────────────────────────────────
server.on('upgrade', (req, socket) => {
  const reqOrigin = req.headers['origin'];

  // Validate origin before completing the handshake
  if (!isAllowedOrigin(reqOrigin)) {
    console.warn(`[WS] Rejected connection from origin: "${reqOrigin}"`);
    socket.write(
      'HTTP/1.1 403 Forbidden\r\n' +
      'Content-Type: text/plain\r\n\r\n' +
      `Origin "${reqOrigin}" not allowed.\r\n`
    );
    socket.destroy();
    return;
  }

  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }

  const accept = crypto
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  // Include CORS headers in the 101 response so browsers accept the upgrade
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n` +
    `Access-Control-Allow-Origin: ${corsOriginHeader(reqOrigin)}\r\n\r\n`
  );

  attachWS(socket);
});

// ── Per-socket WebSocket handler ─────────────────────────────
function attachWS(socket) {
  let player = null; // { code, role: 'p1'|'p2', name }

  // ── Send helper ────────────────────────────────────────────
  function send(obj) {
    if (socket.destroyed) return;
    try {
      const data = Buffer.from(JSON.stringify(obj));
      const len  = data.length;
      let header;
      if (len < 126) {
        header = Buffer.from([0x81, len]);
      } else if (len < 65536) {
        header = Buffer.from([0x81, 126, (len >> 8) & 0xff, len & 0xff]);
      } else {
        header = Buffer.from([
          0x81, 127,
          0, 0, 0, 0,
          (len >> 24) & 0xff, (len >> 16) & 0xff,
          (len >> 8)  & 0xff,  len        & 0xff,
        ]);
      }
      socket.write(Buffer.concat([header, data]));
    } catch (err) {
      console.error('send() error:', err.message);
    }
  }

  socket._sendFn = send;

  // ── Broadcast to all players in a room (optional role exclude) ──
  function broadcast(room, obj, excludeRole) {
    for (const [role, ws] of Object.entries(room.players)) {
      if (role !== excludeRole && ws && !ws.destroyed) {
        ws._sendFn(obj);
      }
    }
  }

  // ── WS frame parser ────────────────────────────────────────
  let buf = Buffer.alloc(0);

  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);

    while (buf.length >= 2) {
      const opcode = buf[0] & 0x0f;

      if (opcode === 0x8) { socket.destroy(); return; }   // close frame

      // Ignore non-text / non-binary / non-ping frames gracefully
      if (opcode !== 0x1 && opcode !== 0x2 && opcode !== 0x9) {
        buf = Buffer.alloc(0); break;
      }

      const masked = (buf[1] & 0x80) !== 0;
      let payloadLen = buf[1] & 0x7f;
      let offset = 2;

      if (payloadLen === 126) {
        if (buf.length < 4) break;
        payloadLen = buf.readUInt16BE(2);
        offset = 4;
      } else if (payloadLen === 127) {
        if (buf.length < 10) break;
        payloadLen = buf.readUInt32BE(2) * 0x100000000 + buf.readUInt32BE(6);
        offset = 10;
      }

      const totalLen = offset + (masked ? 4 : 0) + payloadLen;
      if (buf.length < totalLen) break;

      let payload;
      if (masked) {
        const mask = buf.slice(offset, offset + 4);
        offset += 4;
        payload = Buffer.alloc(payloadLen);
        for (let i = 0; i < payloadLen; i++) {
          payload[i] = buf[offset + i] ^ mask[i % 4];
        }
      } else {
        payload = buf.slice(offset, offset + payloadLen);
      }

      buf = buf.slice(offset + payloadLen);

      if (opcode === 0x1 || opcode === 0x2) {
        try {
          handleMsg(JSON.parse(payload.toString()));
        } catch (err) {
          console.error('Bad JSON from client:', err.message);
        }
      }
    }
  });

  // ── Message handler ────────────────────────────────────────
  function handleMsg(msg) {
    switch (msg.type) {

      // ── CREATE ROOM ────────────────────────────────────────
      case 'create_room': {
        const code = genCode();
        const room = {
          code,
          created: Date.now(),
          players: { p1: socket, p2: null },
          names:   { p1: msg.name || 'Captain 1', p2: null },
          ready:   { p1: false, p2: false },
        };
        rooms.set(code, room);
        player = { code, role: 'p1', name: msg.name || 'Captain 1' };
        send({ type: 'room_created', code, role: 'p1', name: player.name });
        console.log(`[Room ${code}] Created by "${player.name}"`);
        break;
      }

      // ── JOIN ROOM ──────────────────────────────────────────
      case 'join_room': {
        const code = (msg.code || '').toUpperCase().trim();
        const room = rooms.get(code);
        if (!room) {
          send({ type: 'error', msg: `Room "${code}" not found. Check the code!` });
          return;
        }
        if (room.players.p2) {
          send({ type: 'error', msg: 'Room is already full.' });
          return;
        }
        room.players.p2 = socket;
        room.names.p2   = msg.name || 'Captain 2';
        player = { code, role: 'p2', name: room.names.p2 };
        send({ type: 'room_joined', code, role: 'p2', name: player.name, hostName: room.names.p1 });
        if (room.players.p1) {
          room.players.p1._sendFn({ type: 'opponent_joined', name: room.names.p2, role: 'p2' });
        }
        console.log(`[Room ${code}] "${player.name}" joined`);
        break;
      }

      // ── READY ──────────────────────────────────────────────
      case 'ready': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        room.ready[player.role] = true;
        console.log(`[Room ${player.code}] ${player.role} is ready`);
        if (room.ready.p1 && room.ready.p2) {
          const startMsg = { type: 'game_start', names: room.names };
          if (room.players.p1) room.players.p1._sendFn({ ...startMsg, yourRole: 'p1' });
          if (room.players.p2) room.players.p2._sendFn({ ...startMsg, yourRole: 'p2' });
          console.log(`[Room ${player.code}] *** Game started ***`);
        }
        break;
      }

      // ── PLAYER STATE ──────────────────────────────────────
      case 'player_state': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        const oppRole = player.role === 'p1' ? 'p2' : 'p1';
        const opp = room.players[oppRole];
        if (opp) opp._sendFn({ type: 'opponent_state', state: msg.state });
        break;
      }

      // ── BULLET FIRED ──────────────────────────────────────
      case 'bullet': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        const oppRole = player.role === 'p1' ? 'p2' : 'p1';
        const opp = room.players[oppRole];
        if (opp) opp._sendFn({ type: 'opp_bullet', bullet: msg.bullet });
        break;
      }

      // ── GAME OVER ─────────────────────────────────────────
      case 'game_over': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        console.log(`[Room ${player.code}] Game over — winner: "${msg.winner}"`);
        broadcast(room, {
          type:   'game_over',
          winner: msg.winner,
          loser:  msg.loser,   // true = recipient lost, false = recipient won
          stats:  msg.stats || {},
        }, null);
        break;
      }

      // ── WORLD STATE ───────────────────────────────────────
      case 'world_state': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        const oppRole = player.role === 'p1' ? 'p2' : 'p1';
        const opp = room.players[oppRole];
        if (opp) opp._sendFn({ type: 'world_state', isles: msg.isles, ships: msg.ships });
        break;
      }

      // ── GAME EVENT ────────────────────────────────────────
      case 'game_event': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        broadcast(room, { type: 'game_event', event: msg.event }, player.role);
        break;
      }

      // ── CHAT ──────────────────────────────────────────────
      case 'chat': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        broadcast(room, { type: 'chat', from: player.name, text: msg.text }, null);
        break;
      }

      // ── PING ──────────────────────────────────────────────
      case 'ping': {
        send({ type: 'pong', t: msg.t });
        break;
      }

      default:
        console.warn(`[Room ${player?.code}] Unknown message type: "${msg.type}"`);
    }
  }

  // ── Disconnect ─────────────────────────────────────────────
  socket.on('close', () => {
    if (!player) return;
    const room = rooms.get(player.code);
    if (!room) return;
    room.players[player.role] = null;
    room.ready[player.role]   = false;
    console.log(`[Room ${player.code}] "${player.name}" disconnected`);
    const oppRole = player.role === 'p1' ? 'p2' : 'p1';
    const opp = room.players[oppRole];
    if (opp) opp._sendFn({ type: 'opponent_left', name: player.name });
    if (!room.players.p1 && !room.players.p2) {
      rooms.delete(player.code);
      console.log(`[Room ${player.code}] Empty, removed`);
    }
  });

  socket.on('error', (err) => {
    console.error(`Socket error (${player?.name || 'unknown'}):`, err.message);
    socket.destroy();
  });
}

// ── Start ─────────────────────────────────────────────────────
server.listen(PORT, () => {
  const origins = ALLOW_ALL ? '* (all — set ALLOWED_ORIGIN in production!)' : RAW_ORIGINS.join(', ');
  console.log(`\n⚓  Island Capture Multiplayer Server`);
  console.log(`   Listening on port ${PORT}`);
  console.log(`   Allowed origins: ${origins}`);
  console.log(`\n   RENDER DEPLOYMENT CHECKLIST:`);
  console.log(`   ✓ Set ALLOWED_ORIGIN = https://<your-app>.vercel.app`);
  console.log(`   ✓ PORT is injected by Render automatically — do not override`);
  console.log(`   ✓ Update the WS URL in island-capture.html to wss://<your-render-app>.onrender.com`);
  console.log(`\n   HOW TO PLAY:`);
  console.log(`   1. Open your Vercel URL in Browser Tab 1`);
  console.log(`   2. Click "Multiplayer" → enter name → "Create Room" → note the code`);
  console.log(`   3. Share the code with your opponent`);
  console.log(`   4. Opponent opens Vercel URL → "Multiplayer" → enters code → "Join Room"\n`);
});
