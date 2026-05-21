/**
 * Aimesig Internet Relay Server — v3
 * Heartbeat-based presence, message queuing for offline peers.
 * NEW: MongoDB Atlas profile-picture storage (POST/GET /profile-picture).
 */

const { createServer } = require('http');
const { Server } = require('socket.io');
const { MongoClient } = require('mongodb');

const PORT = process.env.PORT || 3000;
const HEARTBEAT_INTERVAL = 20000; // 20s
const PEER_TIMEOUT = 60000;       // 60s

// ── MongoDB ───────────────────────────────────────────────────────────────────
//
// Set MONGODB_URI as an environment variable on your server / Render dashboard.
// Example:
//   MONGODB_URI=mongodb+srv://<user>:<pass>@cluster0.xxxxx.mongodb.net/aimesig?retryWrites=true&w=majority
//
// The database "aimesig" and collection "profile_pictures" are created
// automatically on first write.

const MONGO_URI = process.env.MONGODB_URI || '';
let profilePicturesCollection = null;

async function connectMongo() {
  if (!MONGO_URI) {
    console.warn('[Mongo] MONGODB_URI not set — profile-picture storage disabled.');
    return;
  }
  try {
    const client = new MongoClient(MONGO_URI, { serverSelectionTimeoutMS: 5000 });
    await client.connect();
    const db = client.db('aimesig');
    profilePicturesCollection = db.collection('profile_pictures');

    // Unique index on uid so upsert replaces the existing document
    await profilePicturesCollection.createIndex({ uid: 1 }, { unique: true });

    console.log('[Mongo] connected to Atlas');
  } catch (err) {
    console.error('[Mongo] connection failed:', err.message);
  }
}

connectMongo();

// ── HTTP Server ───────────────────────────────────────────────────────────────

const httpServer = createServer(async (req, res) => {
  // ── CORS preflight ──────────────────────────────────────────────────────────
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  // ── POST /profile-picture ───────────────────────────────────────────────────
  // Body: { uid: string, imageBase64: string }
  // Upserts the document in MongoDB and returns { url: string }.
  if (req.method === 'POST' && req.url === '/profile-picture') {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', async () => {
      try {
        const { uid, imageBase64 } = JSON.parse(body);
        if (!uid || !imageBase64) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'uid and imageBase64 are required' }));
          return;
        }

        if (!profilePicturesCollection) {
          res.writeHead(503, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'MongoDB not available' }));
          return;
        }

        await profilePicturesCollection.updateOne(
          { uid },
          { $set: { uid, imageBase64, updatedAt: new Date() } },
          { upsert: true },
        );

        const url = `${req.headers['x-forwarded-proto'] || 'https'}://${req.headers.host}/profile-picture/${uid}`;
        console.log(`[PP] stored picture for uid=${uid}`);
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ url }));
      } catch (err) {
        console.error('[PP] POST error:', err);
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Internal server error' }));
      }
    });
    return;
  }

  // ── GET /profile-picture/:uid ───────────────────────────────────────────────
  // Returns { imageBase64: string } or 404.
  const match = req.url && req.url.match(/^\/profile-picture\/(.+)$/);
  if (req.method === 'GET' && match) {
    const uid = decodeURIComponent(match[1]);
    if (!profilePicturesCollection) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'MongoDB not available' }));
      return;
    }
    try {
      const doc = await profilePicturesCollection.findOne(
        { uid },
        { projection: { _id: 0, imageBase64: 1 } },
      );
      if (!doc) {
        res.writeHead(404, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'Not found' }));
        return;
      }
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ imageBase64: doc.imageBase64 }));
    } catch (err) {
      console.error('[PP] GET error:', err);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Internal server error' }));
    }
    return;
  }

  // ── Health check ────────────────────────────────────────────────────────────
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Aimesig relay server v3\n');
});

// ── Socket.IO ─────────────────────────────────────────────────────────────────

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
// deviceId → [{ to, data }]
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

  socket.on('register', ({ deviceId, name }) => {
    if (!deviceId) return;
    const old = onlinePeers.get(deviceId);
    if (old && old.socketId !== socket.id) {
      socketToDevice.delete(old.socketId);
    }
    onlinePeers.set(deviceId, { socketId: socket.id, name, deviceId, lastSeen: Date.now() });
    socketToDevice.set(socket.id, deviceId);
    console.log(`[R] register  deviceId=${deviceId}  name=${name}`);
    broadcastPresence(socket.id, 'peer_online', { deviceId, name });
    socket.emit('online_peers', getPeerList(deviceId));
    flushQueue(deviceId);
  });

  socket.on('heartbeat', ({ deviceId }) => {
    const peer = onlinePeers.get(deviceId);
    if (peer && peer.socketId === socket.id) peer.lastSeen = Date.now();
    socket.emit('heartbeat_ack', { ts: Date.now() });
  });

  socket.on('get_online_peers', () => {
    const myDeviceId = socketToDevice.get(socket.id);
    socket.emit('online_peers', getPeerList(myDeviceId));
  });

  socket.on('send_message', ({ to, data }) => {
    if (!to || !data) return;
    const recipient = onlinePeers.get(to);
    if (recipient) {
      console.log(`[M] relay  to=${to}  type=${data.type || '?'}`);
      io.to(recipient.socketId).emit('message', data);
    } else {
      console.log(`[Q] queue  to=${to}  type=${data.type || '?'}`);
      if (!messageQueue.has(to)) messageQueue.set(to, []);
      const q = messageQueue.get(to);
      if (q.length < 100) q.push({ to, data });
    }
  });

  socket.on('disconnect', (reason) => {
    const deviceId = socketToDevice.get(socket.id);
    if (!deviceId) { console.log(`[-] unknown socket  id=${socket.id}`); return; }
    const peer = onlinePeers.get(deviceId);
    if (peer && peer.socketId === socket.id) {
      onlinePeers.delete(deviceId);
      socketToDevice.delete(socket.id);
      broadcastPresence(socket.id, 'peer_offline', { deviceId, name: peer.name });
      console.log(`[-] disconnect  deviceId=${deviceId}  reason=${reason}`);
    }
  });
});

httpServer.listen(PORT, () => {
  console.log(`Aimesig relay server v3 on port ${PORT}`);
});
