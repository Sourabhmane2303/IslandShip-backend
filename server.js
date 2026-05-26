/**
 * Island Capture — Multiplayer Server (FIXED)
 * Pure Node.js, zero dependencies
 * Run: node server.js
 * Players share a 6-char room code to join the same game
 *
 * BUGS FIXED:
 *  1. socket.on("game_over") used Socket.IO syntax (io.to / .emit) — replaced
 *     with proper raw-WebSocket relay via broadcast().
 *  2. game_over case body ran CLIENT-side code (G.over, myName, showGameOver)
 *     inside the SERVER switch — replaced with server-side relay logic.
 *  3. game_over case was missing curly braces { } like every other case,
 *     causing fall-through / syntax ambiguity.
 *  4. socket.on("game_over") was placed BETWEEN the data handler and
 *     handleMsg — structurally illegal and never triggered correctly;
 *     moved inside handleMsg switch as a proper case.
 *  5. broadcast() excluded the sender — game_over must go to the OPPONENT,
 *     so excludeRole is correctly set to player.role.
 *  6. payloadLen === 127 read UInt32BE at offset 6 but should start at 2;
 *     fixed to buf.readUInt32BE(2) giving correct 8-byte length.
 *  7. Missing try/catch around JSON.parse — malformed frames crashed server.
 *  8. HTTP handler: fs.readFileSync could throw if index.html missing — wrapped
 *     in try/catch with a clear 500 error response.
 *  9. cleanRooms mutated rooms Map while iterating — converted to Array first.
 * 10. socket.on('error') only called destroy() — added console.error log.
 * 11. player_state spread msg.state into the send object: if state contained
 *     a 'type' key it overwrote 'opponent_state', silently dropping the packet
 *     and freezing the opponent ship. Fixed by nesting as { state: msg.state }.
 * 12. world_state was gated on player.role === 'p1', silently dropping P2's
 *     world updates and freezing pirates/NPCs on P1's screen. Removed the role
 *     guard; now relays bidirectionally to the opponent regardless of role.
 * 13. game_over excluded the sender (excludeRole = player.role), so whichever
 *     player triggered game_over never saw the result on their own screen.
 *     Changed excludeRole to null so both players always receive it.
 */

const http   = require('http');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');

const PORT = 3000;

// ── Room store ──────────────────────────────────────────────
const rooms = new Map();

function genCode() {
  return crypto.randomBytes(3).toString('hex').toUpperCase(); // e.g. "A3F9B2"
}

// BUG FIX #9 — collect keys into array before deleting during iteration
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

// ── HTTP server — serves the game HTML ──────────────────────
// BUG FIX #8 — wrap readFileSync in try/catch
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/index.html') {
    try {
      const html = fs.readFileSync(path.join(__dirname, 'index.html'));
      res.writeHead(200, { 'Content-Type': 'text/html' });
      res.end(html);
    } catch (err) {
      console.error('index.html not found:', err.message);
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Server error: index.html not found. Place index.html next to server.js');
    }
  } else {
    res.writeHead(404); res.end('Not found');
  }
});

// ── WebSocket upgrade (pure Node.js, no external library) ───
server.on('upgrade', (req, socket, head) => {
  const key = req.headers['sec-websocket-key'];
  if (!key) { socket.destroy(); return; }

  const accept = crypto
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');

  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
  );

  attachWS(socket);
});

// ── Per-socket WebSocket handler ────────────────────────────
function attachWS(socket) {
  let player = null; // { code, role: 'p1'|'p2', name }

  // ── Send helper ──────────────────────────────────────────
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
        // BUG FIX #6 — 8-byte extended payload: bytes 2-9, not 6-9
        header = Buffer.from([
          0x81, 127,
          0, 0, 0, 0,
          (len >> 24) & 0xff, (len >> 16) & 0xff,
          (len >> 8)  & 0xff,  len        & 0xff
        ]);
      }
      socket.write(Buffer.concat([header, data]));
    } catch (err) {
      console.error('send() error:', err.message);
    }
  }

  socket._sendFn = send;

  // ── Broadcast to all players in a room except excludeRole ─
  function broadcast(room, obj, excludeRole) {
    for (const [role, ws] of Object.entries(room.players)) {
      if (role !== excludeRole && ws && !ws.destroyed) {
        ws._sendFn(obj);
      }
    }
  }

  // ── WS frame parser ──────────────────────────────────────
  let buf = Buffer.alloc(0);

  socket.on('data', chunk => {
    buf = Buffer.concat([buf, chunk]);

    while (buf.length >= 2) {
      const opcode = buf[0] & 0x0f;

      // Close frame
      if (opcode === 0x8) { socket.destroy(); return; }

      // Skip non-text / non-binary / non-ping frames
      if (opcode !== 0x1 && opcode !== 0x2 && opcode !== 0x9) {
        // Just consume — don't crash on ping/pong/continuation
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
        // BUG FIX #6 — read from offset 2, not 6
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

      // BUG FIX #7 — wrap JSON.parse in try/catch
      if (opcode === 0x1 || opcode === 0x2) {
        try {
          handleMsg(JSON.parse(payload.toString()));
        } catch (err) {
          console.error('Bad JSON from client:', err.message);
        }
      }
    }
  });

  // ── Message handler ──────────────────────────────────────
  // BUG FIX #1 #2 #3 #4 — removed the broken socket.on("game_over") block
  // that used Socket.IO (io.to/.emit) and client-side vars (G, myName).
  // game_over is now a proper case inside handleMsg, relaying via broadcast().
  function handleMsg(msg) {
    switch (msg.type) {

      // ── CREATE ROOM ──────────────────────────────────────
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

      // ── JOIN ROOM ────────────────────────────────────────
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
        send({
          type: 'room_joined', code,
          role: 'p2', name: player.name,
          hostName: room.names.p1
        });
        // Notify host that guest arrived
        if (room.players.p1) {
          room.players.p1._sendFn({
            type: 'opponent_joined',
            name: room.names.p2,
            role: 'p2'
          });
        }
        console.log(`[Room ${code}] "${player.name}" joined`);
        break;
      }

      // ── READY (both must confirm before game starts) ─────
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

      // ── PLAYER STATE (position, angle, hp, gold, kills) ──
      case 'player_state': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        const oppRole = player.role === 'p1' ? 'p2' : 'p1';
        const opp = room.players[oppRole];
        // BUG FIX #11 — nest state instead of spreading: spreading msg.state
        // would overwrite type:'opponent_state' if msg.state contains a 'type'
        // key, causing the client to ignore the packet and freeze the opponent.
        if (opp) opp._sendFn({ type: 'opponent_state', state: msg.state });
        break;
      }

      // ── BULLET FIRED ─────────────────────────────────────
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
        // BUG FIX #13 — broadcast to ALL players (excludeRole = null).
        // Previously excluded the sender, so if P2 triggered game_over,
        // P1 never received it (and vice versa). Both players need the
        // result; each client decides what to show based on msg.winner.
        broadcast(room, {
          type:   'game_over',
          winner: msg.winner,
          stats:  msg.stats || {}
        }, null);
        break;
      }

      // ── WORLD STATE (host → guest: isles + ships) ────────
      // BUG FIX #12 — relay bidirectionally, not just P1→P2.
      // Restricting to player.role === 'p1' meant P2's world updates were
      // silently dropped, freezing pirates and NPC ships on P1's screen.
      case 'world_state': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        // Send to the opponent regardless of who is P1 or P2
        const oppRole = player.role === 'p1' ? 'p2' : 'p1';
        const opp = room.players[oppRole];
        if (opp) {
          opp._sendFn({
            type:  'world_state',
            isles: msg.isles,
            ships: msg.ships
          });
        }
        break;
      }

      // ── GAME EVENT (capture / sink log messages) ──────────
      case 'game_event': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        broadcast(room, { type: 'game_event', event: msg.event }, player.role);
        break;
      }

      // ── CHAT ─────────────────────────────────────────────
      case 'chat': {
        if (!player) return;
        const room = rooms.get(player.code);
        if (!room) return;
        // Broadcast to all (including sender so they see their own msg)
        broadcast(room, { type: 'chat', from: player.name, text: msg.text }, null);
        break;
      }

      // ── PING ─────────────────────────────────────────────
      case 'ping': {
        send({ type: 'pong', t: msg.t });
        break;
      }

      default:
        console.warn(`[Room ${player?.code}] Unknown message type: "${msg.type}"`);
    }
  }

  // ── Disconnect ───────────────────────────────────────────
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
    // Clean up if both gone
    if (!room.players.p1 && !room.players.p2) {
      rooms.delete(player.code);
      console.log(`[Room ${player.code}] Empty, removed`);
    }
  });

  // BUG FIX #10 — log the error before destroying
  socket.on('error', (err) => {
    console.error(`Socket error (${player?.name || 'unknown'}):`, err.message);
    socket.destroy();
  });
}

// ── Start ────────────────────────────────────────────────────
server.listen(PORT, () => {
  console.log(`\n⚓  Island Capture Multiplayer Server`);
  console.log(`   http://localhost:${PORT}`);
  console.log(`\n   HOW TO PLAY:`);
  console.log(`   1. Open http://localhost:${PORT} in Browser Tab 1`);
  console.log(`   2. Click "Create Room" — note the 6-letter code`);
  console.log(`   3. Open http://localhost:${PORT} in Browser Tab 2`);
  console.log(`   4. Enter the code and click "Join Room"\n`);
});