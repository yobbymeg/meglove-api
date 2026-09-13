/**
 * MEG LOVE v2.0 — Username-based Call Server
 *
 * Features:
 *   - User registration (phone + unique username)
 *   - Username lookup (search users)
 *   - Call invites (ring target phone via WebSocket)
 *   - In-call text chat
 *   - Group calls (3+ people)
 *   - Room management with multiple participants
 */

const express = require('express');
const cors = require('cors');
const http = require('http');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 3000;
const DB_FILE = path.join(__dirname, 'meglove-users.json');

const app = express();
app.use(cors());
app.use(express.json({ limit: '5mb' }));

// === JSON Storage ===
const db = { users: {}, rooms: {} };

function loadDb() {
  try {
    if (fs.existsSync(DB_FILE)) {
      Object.assign(db, JSON.parse(fs.readFileSync(DB_FILE, 'utf8')));
      console.log(`[DB] Loaded ${Object.keys(db.users).length} users`);
    }
  } catch (e) { console.warn('[DB] Fresh start'); }
}

let saveTimer = null;
function saveDb() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try { fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2)); } catch (e) {}
  }, 1000);
}

// === Online users: userId -> ws ===
const onlineUsers = new Map();

// === API ===

app.get('/health', (req, res) => {
  res.json({ ok: true, users: Object.keys(db.users).length, online: onlineUsers.size });
});

app.get('/', (req, res) => {
  res.json({
    name: 'MEG LOVE',
    version: '2.0.0',
    endpoints: [
      'POST /api/register     { phone, username }',
      'GET  /api/lookup?username=xxx',
      'GET  /api/online',
      'WS   /ws?userId=ID',
    ],
  });
});

// === Register user (phone + unique username) ===
app.post('/api/register', (req, res) => {
  const { phone, username } = req.body || {};
  if (!phone || !username) return res.status(400).json({ error: 'phone + username required' });
  if (username.length < 3) return res.status(400).json({ error: 'Username must be 3+ chars' });
  if (!/^[a-zA-Z0-9_]+$/.test(username)) return res.status(400).json({ error: 'Username: letters, numbers, underscores only' });

  // Check if phone already registered
  const existingByPhone = Object.values(db.users).find(u => u.phone === phone);
  if (existingByPhone) {
    // Return existing user (phone is unique)
    return res.json({ userId: existingByPhone.id, username: existingByPhone.username, phone: existingByPhone.phone });
  }

  // Check username uniqueness
  const existingByName = Object.values(db.users).find(u => u.username.toLowerCase() === username.toLowerCase());
  if (existingByName) {
    return res.status(409).json({ error: 'Username already taken. Try another.' });
  }

  const userId = uuidv4();
  db.users[userId] = { id: userId, phone, username, createdAt: Date.now() };
  saveDb();
  console.log(`[USER] Registered: ${username} (${phone})`);
  res.json({ userId, username, phone });
});

// === Lookup user by username ===
app.get('/api/lookup', (req, res) => {
  const q = (req.query.username || '').trim().toLowerCase();
  if (!q) return res.json({ found: false });

  const user = Object.values(db.users).find(u => u.username.toLowerCase() === q);
  if (!user) return res.json({ found: false, message: 'User not found' });

  res.json({
    found: true,
    user: { userId: user.id, username: user.username, online: onlineUsers.has(user.id) }
  });
});

// === Get online users ===
app.get('/api/online', (req, res) => {
  const online = Array.from(onlineUsers.keys())
    .map(id => db.users[id])
    .filter(Boolean)
    .map(u => ({ userId: u.id, username: u.username }));
  res.json({ users: online });
});

// === WebSocket ===
const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const userId = url.searchParams.get('userId');

  if (!userId || !db.users[userId]) {
    ws.send(JSON.stringify({ type: 'error', message: 'Invalid user' }));
    ws.close();
    return;
  }

  const user = db.users[userId];
  onlineUsers.set(userId, ws);
  ws.userId = userId;
  ws.roomCode = null;

  console.log(`[WS] ${user.username} connected. Online: ${onlineUsers.size}`);

  // Notify friends that user is online
  broadcastPresence(userId, true);

  ws.on('message', (data) => {
    try {
      const msg = JSON.parse(data.toString());
      handleMessage(ws, userId, msg);
    } catch (e) {}
  });

  ws.on('close', () => {
    onlineUsers.delete(userId);
    // Leave room if in one
    if (ws.roomCode && db.rooms[ws.roomCode]) {
      leaveRoom(ws, userId);
    }
    console.log(`[WS] ${user.username} disconnected. Online: ${onlineUsers.size}`);
    broadcastPresence(userId, false);
  });
});

function broadcastPresence(userId, online) {
  // Notify all online users about presence change
  for (const [uid, ws] of onlineUsers.entries()) {
    if (uid !== userId && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({ type: 'presence', userId, online }));
    }
  }
}

function handleMessage(ws, senderId, msg) {
  const sender = db.users[senderId];
  if (!sender) return;
  const { type } = msg;

  // === Call invite (ring target) ===
  if (type === 'call-invite') {
    const targetWs = onlineUsers.get(msg.targetUserId);
    if (!targetWs || targetWs.readyState !== targetWs.OPEN) {
      ws.send(JSON.stringify({ type: 'call-failed', reason: 'User offline' }));
      return;
    }

    // Create a room
    const roomCode = uuidv4().slice(0, 8);
    db.rooms[roomCode] = {
      code: roomCode,
      hostId: senderId,
      hostName: sender.username,
      isVideo: !!msg.isVideo,
      members: [{ userId: senderId, name: sender.username }],
      createdAt: Date.now(),
    };

    // Add sender to room
    ws.roomCode = roomCode;

    // Ring the target
    targetWs.send(JSON.stringify({
      type: 'incoming-call',
      fromUserId: senderId,
      fromName: sender.username,
      isVideo: !!msg.isVideo,
      roomCode,
    }));

    ws.send(JSON.stringify({ type: 'call-ringing', targetUserId: msg.targetUserId, roomCode }));
    console.log(`[CALL] ${sender.username} calling ${db.users[msg.targetUserId]?.username} (room ${roomCode})`);
    return;
  }

  // === Call accept ===
  if (type === 'call-accept') {
    const room = db.rooms[msg.roomCode];
    if (!room) return;
    room.members.push({ userId: senderId, name: sender.username });
    ws.roomCode = msg.roomCode;

    // Notify host that call was accepted
    const hostWs = onlineUsers.get(room.hostId);
    if (hostWs) {
      hostWs.send(JSON.stringify({
        type: 'call-accepted',
        roomCode: msg.roomCode,
        userId: senderId,
        name: sender.username,
        members: room.members,
      }));
    }

    // Tell new member about existing members
    ws.send(JSON.stringify({
      type: 'room-joined',
      roomCode: msg.roomCode,
      members: room.members.filter(m => m.userId !== senderId),
      isVideo: room.isVideo,
    }));

    // Notify all room members
    broadcastToRoom(msg.roomCode, senderId, {
      type: 'user-joined',
      userId: senderId,
      name: sender.username,
    });

    console.log(`[CALL] ${sender.username} joined room ${msg.roomCode} (${room.members.length} members)`);
    return;
  }

  // === Call reject ===
  if (type === 'call-reject') {
    const targetWs = onlineUsers.get(msg.targetUserId);
    if (targetWs) {
      targetWs.send(JSON.stringify({ type: 'call-rejected', fromUserId: senderId }));
    }
    return;
  }

  // === Call end (leave room) ===
  if (type === 'call-end') {
    if (ws.roomCode) leaveRoom(ws, senderId);
    return;
  }

  // === Invite more users to existing room (group call) ===
  if (type === 'invite-to-room') {
    const room = db.rooms[msg.roomCode];
    if (!room) return;
    const targetWs = onlineUsers.get(msg.targetUserId);
    if (!targetWs) {
      ws.send(JSON.stringify({ type: 'invite-failed', reason: 'User offline' }));
      return;
    }
    targetWs.send(JSON.stringify({
      type: 'incoming-call',
      fromUserId: senderId,
      fromName: sender.username,
      isVideo: room.isVideo,
      roomCode: msg.roomCode,
    }));
    ws.send(JSON.stringify({ type: 'invite-sent', targetUserId: msg.targetUserId }));
    return;
  }

  // === In-call text chat ===
  if (type === 'chat-message') {
    const room = ws.roomCode ? db.rooms[ws.roomCode] : null;
    if (!room) return;
    broadcastToRoom(ws.roomCode, senderId, {
      type: 'chat-message',
      fromUserId: senderId,
      fromName: sender.username,
      text: msg.text,
      timestamp: Date.now(),
    });
    return;
  }

  // === DM (direct message) ===
  if (type === 'dm') {
    const targetWs = onlineUsers.get(msg.targetUserId);
    const dmMsg = {
      type: 'dm',
      fromUserId: senderId,
      fromName: sender.username,
      toUserId: msg.targetUserId,
      text: msg.text || null,
      mediaType: msg.mediaType || 'text',
      mediaData: msg.mediaData || null,
      duration: msg.duration || null,
      timestamp: Date.now(),
    };
    // Send to target
    if (targetWs && targetWs.readyState === targetWs.OPEN) {
      targetWs.send(JSON.stringify(dmMsg));
    }
    // Echo back to sender (for confirmation)
    ws.send(JSON.stringify(dmMsg));
    return;
  }

  // === Typing indicator (DM) ===
  if (type === 'dm-typing') {
    const targetWs = onlineUsers.get(msg.targetUserId);
    if (targetWs && targetWs.readyState === targetWs.OPEN) {
      targetWs.send(JSON.stringify({
        type: 'dm-typing',
        fromUserId: senderId,
        isTyping: msg.isTyping,
      }));
    }
    return;
  }

  // === WebRTC signaling (offer/answer/ICE) ===
  if (['call-offer', 'call-answer', 'ice-candidate'].includes(type)) {
    if (msg.targetUserId) {
      const targetWs = onlineUsers.get(msg.targetUserId);
      if (targetWs && targetWs.readyState === targetWs.OPEN) {
        targetWs.send(JSON.stringify({
          ...msg,
          fromUserId: senderId,
          fromName: sender.username,
        }));
      }
    }
    return;
  }
}

function leaveRoom(ws, userId) {
  const roomCode = ws.roomCode;
  const room = db.rooms[roomCode];
  if (!room) return;

  room.members = room.members.filter(m => m.userId !== userId);
  ws.roomCode = null;

  // Notify others
  broadcastToRoom(roomCode, userId, { type: 'user-left', userId });

  // Delete room if empty
  if (room.members.length === 0) {
    delete db.rooms[roomCode];
    console.log(`[ROOM] ${roomCode} deleted (empty)`);
  }
}

function broadcastToRoom(roomCode, excludeUserId, msg) {
  const room = db.rooms[roomCode];
  if (!room) return;
  for (const member of room.members) {
    if (member.userId === excludeUserId) continue;
    const ws = onlineUsers.get(member.userId);
    if (ws && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify(msg));
    }
  }
}

// === Start ===
loadDb();
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n╔══════════════════════════════════════════════╗`);
  console.log(`║     MEG LOVE v2.0 — Username Call Server      ║`);
  console.log(`╚══════════════════════════════════════════════╝`);
  console.log(`\n🌐 http://localhost:${PORT} | WS: ws://localhost:${PORT}/ws\n`);
});

process.on('uncaughtException', (err) => console.error('[UNCAUGHT]', err));
