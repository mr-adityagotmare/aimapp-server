/**
 * Aimesig Internet Relay Server
 * ─────────────────────────────
 * Runs on Node.js 18+ with socket.io ^4.
 *
 * Deploy to any Node host (Render, Railway, Fly.io, VPS, …).
 * Set env PORT if needed; defaults to 3000.
 *
 * Install:
 *   npm install
 *
 * Run:
 *   node server.js
 */

const { createServer } = require('http');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;

const httpServer = createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Aimesig relay server running\n');
});

const io = new Server(httpServer, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
});

/**
 * In-memory registry: deviceId → { socketId, name, deviceId }
 * A production app would use Redis so multiple server instances share state.
 */
const onlinePeers = new Map(); // deviceId → { socketId, name, deviceId }
const socketToDevice = new Map(); // socketId → deviceId

io.on('connection', (socket) => {
  console.log(`[+] socket connected  id=${socket.id}`);

  // ── register ──────────────────────────────────────────────────────────────
  socket.on('register', ({ deviceId, name }) => {
    if (!deviceId) return;
    onlinePeers.set(deviceId, { socketId: socket.id, name, deviceId });
    socketToDevice.set(socket.id, deviceId);
    console.log(`[R] registered  deviceId=${deviceId}  name=${name}`);

    // Tell everyone else this peer is online
    socket.broadcast.emit('peer_online', { deviceId, name });

    // Send this new peer the current list of everyone else
    const peers = [...onlinePeers.values()]
      .filter((p) => p.deviceId !== deviceId)
      .map(({ deviceId: did, name: n }) => ({ deviceId: did, name: n }));
    socket.emit('online_peers', peers);
  });

  // ── get_online_peers ──────────────────────────────────────────────────────
  socket.on('get_online_peers', () => {
    const myDeviceId = socketToDevice.get(socket.id);
    const peers = [...onlinePeers.values()]
      .filter((p) => p.deviceId !== myDeviceId)
      .map(({ deviceId, name }) => ({ deviceId, name }));
    socket.emit('online_peers', peers);
  });

  // ── send_message ──────────────────────────────────────────────────────────
  // { to: <deviceId>, data: { … any packet … } }
  socket.on('send_message', ({ to, data }) => {
    if (!to || !data) return;
    const recipient = onlinePeers.get(to);
    if (!recipient) {
      console.log(`[!] recipient offline  to=${to}`);
      // Optionally queue for later delivery here
      return;
    }
    console.log(`[M] relay  to=${to}  type=${data.type || '?'}`);
    io.to(recipient.socketId).emit('message', data);
  });

  // ── disconnect ────────────────────────────────────────────────────────────
  socket.on('disconnect', () => {
    const deviceId = socketToDevice.get(socket.id);
    if (deviceId) {
      const info = onlinePeers.get(deviceId);
      onlinePeers.delete(deviceId);
      socketToDevice.delete(socket.id);
      socket.broadcast.emit('peer_offline', {
        deviceId,
        name: info?.name ?? '',
      });
      console.log(`[-] disconnected  deviceId=${deviceId}`);
    } else {
      console.log(`[-] socket disconnected  id=${socket.id}  (no deviceId)`);
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`Aimesig relay server listening on port ${PORT}`);
});
