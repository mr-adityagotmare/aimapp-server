/**
 * Aimesig Internet Relay Server — v2
 * Heartbeat-based presence, message queuing for offline peers.
 */

const { createServer } = require('http');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_INTERVAL = 20000; // 20s
const PEER_TIMEOUT = 60000;       // 60s — remove peer if no heartbeat

const httpServer = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Aimesig relay server v2\n');
});

const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingInterval: 10000,
  pingTimeout: 5000,
});

// deviceId → { socketId, name, deviceId, lastSeen }
const onlinePeers = new Map();
// socketId → deviceId
const socketToDevice = new Map();
// deviceId → [{ to, data }]  — messages queued while peer was offline
const messageQueue = new Map();

// ── Helpers ───────────────────────────────────────────────────────────────────

function broadcastPresence(excludeSocketId, event, payload) {
  for (const [, peer] of onlinePeers) {
    if (peer.socketId !== excludeSocketId) {
      io.to(peer.socketId).emit(event, payload);
    }
  }
}

function getPeerList(excludeDeviceId) {
  return [...onlinePeers.values()]
    .filter(p => p.deviceId !== excludeDeviceId)
    .map(({ deviceId, name }) => ({ deviceId, name }));
}

function flushQueue(deviceId) {
  const queue = messageQueue.get(deviceId);
  if (!queue || queue.length === 0) return;
  const peer = onlinePeers.get(deviceId);
  if (!peer) return;
  console.log(`[Q] flushing ${queue.length} queued msgs to ${deviceId}`);
  for (const { data } of queue) {
    io.to(peer.socketId).emit('message', data);
  }
  messageQueue.delete(deviceId);
}

// ── Stale-peer janitor (runs every 30s) ───────────────────────────────────────

setInterval(() => {
  const now = Date.now();
  for (const [deviceId, peer] of onlinePeers) {
    if (now - peer.lastSeen > PEER_TIMEOUT) {
      console.log(`[T] timeout  deviceId=${deviceId}`);
      onlinePeers.delete(deviceId);
      socketToDevice.delete(peer.socketId);
      broadcastPresence(peer.socketId, 'peer_offline', { deviceId, name: peer.name });
    }
  }
}, 30000);

// ── Socket handlers ───────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  console.log(`[+] connected  id=${socket.id}`);

  // ── register ────────────────────────────────────────────────────────────────
  socket.on('register', ({ deviceId, name }) => {
    if (!deviceId) return;

    // If this deviceId reconnected on a new socket, clean up old entry.
    const old = onlinePeers.get(deviceId);
    if (old && old.socketId !== socket.id) {
      socketToDevice.delete(old.socketId);
    }

    onlinePeers.set(deviceId, { socketId: socket.id, name, deviceId, lastSeen: Date.now() });
    socketToDevice.set(socket.id, deviceId);
    console.log(`[R] register  deviceId=${deviceId}  name=${name}`);

    // Tell everyone else this peer came online.
    broadcastPresence(socket.id, 'peer_online', { deviceId, name });

    // Send this peer the full current online list.
    socket.emit('online_peers', getPeerList(deviceId));

    // Deliver any queued messages.
    flushQueue(deviceId);
  });

  // ── heartbeat (client pings every 20s) ──────────────────────────────────────
  socket.on('heartbeat', ({ deviceId }) => {
    const peer = onlinePeers.get(deviceId);
    if (peer && peer.socketId === socket.id) {
      peer.lastSeen = Date.now();
    }
    socket.emit('heartbeat_ack', { ts: Date.now() });
  });

  // ── get_online_peers ─────────────────────────────────────────────────────────
  socket.on('get_online_peers', () => {
    const myDeviceId = socketToDevice.get(socket.id);
    socket.emit('online_peers', getPeerList(myDeviceId));
  });

  // ── send_message ─────────────────────────────────────────────────────────────
  socket.on('send_message', ({ to, data }) => {
    if (!to || !data) return;
    const recipient = onlinePeers.get(to);
    if (recipient) {
      console.log(`[M] relay  to=${to}  type=${data.type || '?'}`);
      io.to(recipient.socketId).emit('message', data);
    } else {
      // Queue for when they come back online (max 100 msgs per peer).
      console.log(`[Q] queue  to=${to}  type=${data.type || '?'}`);
      if (!messageQueue.has(to)) messageQueue.set(to, []);
      const q = messageQueue.get(to);
      if (q.length < 100) q.push({ to, data });
    }
  });

  // ── disconnect ────────────────────────────────────────────────────────────────
  socket.on('disconnect', (reason) => {
    const deviceId = socketToDevice.get(socket.id);
    if (!deviceId) { console.log(`[-] unknown socket  id=${socket.id}`); return; }
    const peer = onlinePeers.get(deviceId);
    // Only remove if this socket is still the active one for this device.
    if (peer && peer.socketId === socket.id) {
      onlinePeers.delete(deviceId);
      socketToDevice.delete(socket.id);
      broadcastPresence(socket.id, 'peer_offline', { deviceId, name: peer.name });
      console.log(`[-] disconnect  deviceId=${deviceId}  reason=${reason}`);
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`Aimesig relay server v2 on port ${PORT}`);
});
