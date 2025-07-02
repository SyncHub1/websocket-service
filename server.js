import { WebSocketServer } from 'ws';
import Redis from 'ioredis';
import mongoose from 'mongoose';
import dotenv from 'dotenv';
import http from 'http';
import Message from './models/Message.model.js';
import Group from './models/Group.model.js';
import User from './models/User.model.js';


dotenv.config();

const PORT = process.env.PORT || 3002;
const HEALTH_PORT = process.env.HEALTH_PORT || 3003;

const MONGODB_URI = process.env.MONGODB_URI;
const REDIS_HOST = process.env.REDIS_HOST;
const REDIS_PORT = process.env.REDIS_PORT;
const REDIS_PASSWORD = process.env.REDIS_PASSWORD;


// Initialize Redis with error handling
let redis, pub, sub;
try {
  redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    password: REDIS_PASSWORD,
    retryDelayOnFailover: 100,
    maxRetriesPerRequest: 3
  });
  pub = redis;
  sub = redis.duplicate();

  redis.on('connect', () => console.log('✅ Redis connected'));
  pub.on('connect', () => console.log('✅ Redis PUB connected'));
  sub.on('connect', () => console.log('✅ Redis SUB connected'));
  redis.on('error', (err) => console.error('❌ Redis error', err));
  pub.on('error', (err) => console.error('❌ Redis PUB error', err));
  sub.on('error', (err) => console.error('❌ Redis SUB error', err));
} catch (error) {
  console.warn('⚠️ Redis not available, running without Redis');
  redis = null;
  pub = null;
  sub = null;
}

// Connect to MongoDB with error handling
async function connectToMongoDB() {
  try {
    if (!MONGODB_URI) {
      console.warn('⚠️ MONGODB_URI not set, running without database');
      return;
    }
    console.log('MONGODB_URI:', process.env.MONGODB_URI);
    await mongoose.connect(MONGODB_URI, {
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    });
      
    console.log('✅ WebSocket: Connected to MongoDB'); 
  } catch (error) {
    console.warn('⚠️ MongoDB not available, running without database validation');
    console.error('MongoDB connection error:', error.message);
  }
}

// Connect to MongoDB
await connectToMongoDB();



// Handle MongoDB connection events
mongoose.connection.on('connected', () => {
  console.log('✅ MongoDB connection established');
});

mongoose.connection.on('error', (err) => {
  console.error('❌ MongoDB connection error:', err);
});

mongoose.connection.on('disconnected', () => {
  console.log('⚠️ MongoDB connection disconnected');
});

// Check if we can use database operations
function canUseDatabase() {
  return mongoose.connection.readyState === 1; // 1 = connected
}

const wss = new WebSocketServer({ port: PORT });
console.log(`WebSocket server running on ws://localhost:${PORT}`);

// Map userId to WebSocket
const clients = new Map();

// Track online users
const onlineUsers = new Set();

// Helper function to validate user exists (with fallback)
async function validateUser(userId) {
  try {
    // Skip validation in development by default
    if (process.env.SKIP_USER_VALIDATION === 'true' || !canUseDatabase()) {
      console.log('⚠️ Skipping user validation for development');
      return true;
    }
    
    console.log('🔍 Validating user in database:', userId);
    const user = await User.findById(userId);
    const exists = user !== null;
    console.log('🔍 User validation result:', exists);
    return exists;
  } catch (error) {
    console.error('Error validating user:', error);
    // If database is not available, allow connection
    return true;
  }
}

// Helper function to send error message
function sendError(ws, error, type = 'error') {
  ws.send(JSON.stringify({ type, error: error.message || error }));
}

wss.on('connection', (ws, req) => {
  // For demo: userId in query string
  const url = new URL(req.url, `http://${req.headers.host}`);
  const userId = String(url.searchParams.get('userId'));
  
  console.log('🔌 New WebSocket connection attempt for userId:', userId);
  
  if (!userId) {
    console.log('❌ Connection rejected: Missing userId');
    ws.close(4001, 'Missing userId');
    return;
  }
  
  // Validate user exists before allowing connection
  validateUser(userId).then(userExists => {
    if (!userExists) {
      console.log('❌ Connection rejected: User not found in database:', userId);
      sendError(ws, 'User not found in database. Please ensure the user exists.');
      ws.close(4002, 'User not found');
      return;
    }
    
    console.log('✅ User validated successfully:', userId);
    clients.set(userId, ws);
    ws.userId = userId;
    console.log('User connected:', userId, typeof userId, 'clients keys:', Array.from(clients.keys()));

    onlineUsers.add(userId);
    
    // Send connection confirmation
    ws.send(JSON.stringify({ 
      type: 'connected', 
      userId,
      onlineUsers: Array.from(onlineUsers)
    }));
    
    // Broadcast online status
    for (const [uid, client] of clients.entries()) {
      if (client.readyState === ws.OPEN && uid !== userId) {
        client.send(JSON.stringify({ type: 'online', userId }));
      }
    }
  }).catch(error => {
    console.error('❌ Error during user validation:', error);
    sendError(ws, 'Database connection error during user validation');
    ws.close(4003, 'Database error');
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data);
      console.log(`[WS] Received message from ${userId}:`, msg.type);
      
      if (msg.type === 'send_message') {
        // DM or group message
        const messageType = msg.messageType || 'text'; // Use 'messageType' for content type, default to 'text'
        if (msg.groupId) {
          // Group message
          const message = await Message.create({
            senderId: userId,
            groupId: msg.groupId,
            content: msg.content,
            type: messageType,
            fileUrl: msg.fileUrl,
            fileName: msg.fileName,
            fileSize: msg.fileSize,
            clientId: msg.clientId
          });
          const group = await Group.findById(msg.groupId);
          for (const memberId of group.members) {
            if (String(memberId) !== userId) {
              const memberWs = clients.get(String(memberId));
              if (memberWs && memberWs.readyState === ws.OPEN) {
                memberWs.send(JSON.stringify({
                  eventType: 'group_message',
                  groupId: msg.groupId,
                  senderId: userId,
                  content: msg.content,
                  type: messageType,
                  fileUrl: msg.fileUrl,
                  fileName: msg.fileName,
                  fileSize: msg.fileSize,
                  timestamp: message.timestamp,
                  messageId: message._id,
                  clientId: message.clientId
                }));
              }
            }
          }
          // Optionally, send delivery confirmation to sender
          ws.send(JSON.stringify({
            eventType: 'message_sent',
            messageId: message._id,
            groupId: msg.groupId,
            delivered: true,
            timestamp: message.timestamp,
            clientId: message.clientId
          }));
        } else {
          // Direct message
          const recipientOnline = clients.has(msg.receiverId);
          const message = await Message.create({
            senderId: userId,
            receiverId: msg.receiverId,
            content: msg.content,
            type: messageType,
            fileUrl: msg.fileUrl,
            fileName: msg.fileName,
            fileSize: msg.fileSize,
            clientId: msg.clientId
          });
          ws.send(JSON.stringify({
            eventType: 'message_sent',
            messageId: message._id,
            to: msg.receiverId,
            delivered: recipientOnline,
            timestamp: message.timestamp,
            clientId: message.clientId
          }));
          // Emit to BOTH sender and receiver
          const payload = {
            eventType: 'new_message',
            senderId: userId,
            receiverId: msg.receiverId,
            content: msg.content,
            type: messageType,
            fileUrl: msg.fileUrl,
            fileName: msg.fileName,
            fileSize: msg.fileSize,
            timestamp: message.timestamp,
            messageId: message._id,
            clientId: message.clientId
          };
          if (clients.has(userId) && clients.get(userId).readyState === ws.OPEN) {
            clients.get(userId).send(JSON.stringify(payload));
          }
          if (clients.has(msg.receiverId) && clients.get(msg.receiverId).readyState === ws.OPEN) {
            clients.get(msg.receiverId).send(JSON.stringify(payload));
          } else if (pub) {
            await pub.publish(`chat:${msg.receiverId}`, JSON.stringify(payload));
          }
        }
      } else if (msg.type === 'get_history') {
        // Fetch chat history (DM or group)
        if (msg.groupId) {
          const messages = await Message.find({
            groupId: msg.groupId,
            deletedFor: { $ne: userId },
            deletedForEveryone: { $ne: true }
          }).sort({ timestamp: 1 });
          ws.send(JSON.stringify({ type: 'history', messages: messages.map(m => ({ ...m.toObject(), clientId: m.clientId })) }));
        } else {
          const messages = await Message.find({
            $or: [
              { senderId: userId, receiverId: msg.with },
              { senderId: msg.with, receiverId: userId }
            ],
            deletedFor: { $ne: userId },
            deletedForEveryone: { $ne: true }
          }).sort({ timestamp: 1 });
          ws.send(JSON.stringify({ type: 'history', messages: messages.map(m => ({ ...m.toObject(), clientId: m.clientId })) }));
        }
      } else if (msg.type === 'seen') {
        // Seen status for DM or group
        const message = await Message.findById(msg.messageId);
        if (message && !message.seenBy.map(id => id.toString()).includes(userId)) {
          message.seenBy.push(userId);
          await message.save();
          // Notify sender (DM) or all group members (group)
          if (message.groupId) {
            const group = await Group.findById(message.groupId);
            for (const memberId of group.members) {
              if (String(memberId) !== userId) {
                const memberWs = clients.get(String(memberId));
                if (memberWs && memberWs.readyState === ws.OPEN) {
                  memberWs.send(JSON.stringify({ type: 'seen', groupId: message.groupId, messageId: message._id, seenBy: message.seenBy, clientId: message.clientId }));
                }
              }
            }
          } else {
            const senderWs = clients.get(String(message.senderId));
            if (senderWs && senderWs.readyState === ws.OPEN) {
              senderWs.send(JSON.stringify({ type: 'seen', messageId: message._id, seenBy: message.seenBy, clientId: message.clientId }));
            }
          }
        }
      } else if (msg.type === 'typing') {
        // Typing indicator for DM
        const toWs = clients.get(msg.receiverId);
        if (toWs && toWs.readyState === ws.OPEN) {
          toWs.send(JSON.stringify({ type: 'typing', from: userId, clientId: msg.clientId }));
        }
      } else if (msg.type === 'group_typing') {
        // Typing indicator for group
        const group = await Group.findById(msg.groupId);
        for (const memberId of group.members) {
          if (String(memberId) !== userId) {
            const memberWs = clients.get(String(memberId));
            if (memberWs && memberWs.readyState === ws.OPEN) {
              memberWs.send(JSON.stringify({ type: 'group_typing', groupId: msg.groupId, from: userId, clientId: msg.clientId }));
            }
          }
        }
      } else if (msg.type === 'delete_message') {
        // WhatsApp-style delete
        const { messageId, forEveryone } = msg;
        const message = await Message.findById(messageId);
        if (!message) return;
        if (forEveryone) {
          message.deletedForEveryone = true;
        } else {
          if (!message.deletedFor.includes(userId)) {
            message.deletedFor.push(userId);
          }
        }
        await message.save();
        // Notify all relevant users
        if (message.groupId) {
          const group = await Group.findById(message.groupId);
          for (const memberId of group.members) {
            const memberWs = clients.get(String(memberId));
            if (memberWs && memberWs.readyState === ws.OPEN) {
              memberWs.send(JSON.stringify({
                type: 'message_deleted',
                messageId,
                forEveryone,
                clientId: message.clientId
              }));
            }
          }
        } else {
          [String(message.senderId), String(message.receiverId)].forEach(uid => {
            if (clients.has(uid) && clients.get(uid).readyState === ws.OPEN) {
              clients.get(uid).send(JSON.stringify({
                type: 'message_deleted',
                messageId,
                forEveryone,
                clientId: message.clientId
              }));
            }
          });
        }
      } else if (msg.type === 'get_online_users') {
        // Return list of online users
        ws.send(JSON.stringify({ 
          type: 'online_users', 
          users: Array.from(onlineUsers),
          clientId: msg.clientId
        }));
      } else if (msg.type === 'read') {
        // Read receipt for DM
        const toWs = clients.get(msg.receiverId);
        if (toWs && toWs.readyState === ws.OPEN) {
          toWs.send(JSON.stringify({ type: 'read', from: userId, messageId: msg.messageId, clientId: msg.clientId }));
        }
      } else if (msg.type === 'create_group') {
        // Create group
        const group = await Group.create({
          name: msg.name,
          avatar: msg.avatar,
          members: msg.members,
          admins: [userId]
        });
        // Notify all members
        for (const memberId of msg.members) {
          const memberWs = clients.get(memberId);
          if (memberWs && memberWs.readyState === ws.OPEN) {
            memberWs.send(JSON.stringify({ type: 'group_created', group, clientId: msg.clientId }));
          }
        }
      } else {
        sendError(ws, 'Unknown message type', 'error');
      }
    } catch (err) {
      console.error('[WS] Error processing message:', err);
      sendError(ws, err.message);
    }
  });

  ws.on('close', () => {
    clients.delete(userId);
    onlineUsers.delete(userId);
    // Broadcast offline status
    for (const [uid, client] of clients.entries()) {
      if (client.readyState === ws.OPEN) {
        client.send(JSON.stringify({ type: 'offline', userId, clientId: msg.clientId }));
      }
    }
    console.log(`User ${userId} disconnected`);
  });
});

// Subscribe to all chat channels (only if Redis is available)
if (sub) {
  sub.psubscribe('chat:*');
  sub.on('pmessage', (pattern, channel, message) => {
    const toUserId = String(channel.split(':')[1]);
    console.log('[WS] Redis pmessage for channel:', channel, 'toUserId:', toUserId, 'clients keys:', Array.from(clients.keys()));
    const ws = clients.get(toUserId);
    if (ws && ws.readyState === ws.OPEN) {
      ws.send(JSON.stringify({ type: 'new_message', ...JSON.parse(message), clientId: JSON.parse(message).clientId }));
      console.log('[WS] Relaying message to user', toUserId);
    } else {
      console.log('[WS] No client found for', toUserId, 'clients keys:', Array.from(clients.keys()));
    }
  });
} else {
  console.log('⚠️ Redis not available, skipping chat channel subscription');
}

// Health check endpoint
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'healthy',
      timestamp: new Date().toISOString(),
      service: 'websocket-service',
      version: '1.0.0',
      connections: wss.clients.size,
      authenticatedConnections: clients.size
    }));
  } else {
    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      error: 'Not found',
      message: 'Route not found'
    }));
  }
});

healthServer.listen(HEALTH_PORT, () => {
  console.log(`🏥 Health check server running on port ${HEALTH_PORT}`);
});

// Redis connection error handling
redis?.on('error', (error) => {
  console.error('❌ Redis connection error:', error);
});

redis?.on('connect', () => {
  console.log('✅ Redis connected');
});

// Graceful shutdown
const shutdown = async () => {
  console.log('🛑 Shutting down WebSocket server...');
  
  try {
    // Close WebSocket server
    wss.close(() => {
      console.log('✅ WebSocket server closed');
    });
    
    // Disconnect Redis
    await redis?.disconnect();
    console.log('✅ Redis disconnected');
    
    // Close health server
    healthServer.close(() => {
      console.log('✅ Health server closed');
      process.exit(0);
    });
  } catch (error) {
    console.error('❌ Error during shutdown:', error);
    process.exit(1);
  }
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  console.error('❌ Uncaught Exception:', error);
  shutdown();
});

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
  shutdown();
});

console.log(`🏥 Health check: http://localhost:${HEALTH_PORT}/health`); 