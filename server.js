import { WebSocketServer, WebSocket } from 'ws';
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

// Subscribe to Redis events for real-time message deletion
if (sub) {
  try {
    await sub.subscribe('chat:delete');
    console.log('✅ Subscribed to Redis chat:delete events');
    
    sub.on('message', async (channel, message) => {
      if (channel === 'chat:delete') {
        try {
          const deleteEvent = JSON.parse(message);
          console.log('📨 Received Redis delete event:', deleteEvent);
          console.log('📨 Available clients:', Array.from(clients.keys()));
          
          const { messageId, groupId, senderId, receiverId, forEveryone } = deleteEvent;
          
          if (forEveryone) {
            // Delete for everyone - notify all relevant users
            if (groupId) {
              // Group message deletion
              try {
                const group = await Group.findById(groupId);
                if (group) {
                  console.log('📨 Group members:', group.members);
                  for (const memberId of group.members) {
                    const memberWs = clients.get(String(memberId));
                    console.log(`📨 Member ${memberId}: WebSocket exists: ${!!memberWs}, readyState: ${memberWs?.readyState}`);
                    if (memberWs && memberWs.readyState === WebSocket.OPEN) {
                      const deleteMessage = {
                        type: 'message_deleted',
                        messageId,
                        forEveryone: true,
                        groupId
                      };
                      console.log('📨 Sending delete message to group member:', memberId, deleteMessage);
                      memberWs.send(JSON.stringify(deleteMessage));
                    }
                  }
                }
              } catch (error) {
                console.error('Error handling group message deletion:', error);
              }
            } else {
              // Direct message deletion
              const senderWs = clients.get(String(senderId));
              const receiverWs = clients.get(String(receiverId));
              
              console.log(`📨 Sender ${senderId}: WebSocket exists: ${!!senderWs}, readyState: ${senderWs?.readyState}`);
              console.log(`📨 Receiver ${receiverId}: WebSocket exists: ${!!receiverWs}, readyState: ${receiverWs?.readyState}`);
              
              if (senderWs && senderWs.readyState === WebSocket.OPEN) {
                const deleteMessage = {
                  type: 'message_deleted',
                  messageId,
                  forEveryone: true
                };
                console.log('📨 Sending delete message to sender:', senderId, deleteMessage);
                senderWs.send(JSON.stringify(deleteMessage));
              }
              
              if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
                const deleteMessage = {
                  type: 'message_deleted',
                  messageId,
                  forEveryone: true
                };
                console.log('📨 Sending delete message to receiver:', receiverId, deleteMessage);
                receiverWs.send(JSON.stringify(deleteMessage));
              }
            }
          } else {
            // Delete for me - only notify the specific user
            const userId = deleteEvent.userId;
            const userWs = clients.get(String(userId));
            console.log(`📨 User ${userId}: WebSocket exists: ${!!userWs}, readyState: ${userWs?.readyState}`);
            if (userWs && userWs.readyState === WebSocket.OPEN) {
              const deleteMessage = {
                type: 'message_deleted',
                messageId,
                forEveryone: false
              };
              console.log('📨 Sending delete for me message to user:', userId, deleteMessage);
              userWs.send(JSON.stringify(deleteMessage));
            }
          }
        } catch (error) {
          console.error('Error processing Redis delete event:', error);
        }
      }
    });
  } catch (error) {
    console.error('❌ Failed to subscribe to Redis events:', error);
  }
}

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
      const messageType = msg.type || msg.eventType;
      
      if (messageType === 'send_message') {
        // DM or group message
        const messageType = msg.messageType || 'text'; // Use 'messageType' for content type, default to 'text'
        
        // Validate recipient exists for direct messages
        if (!msg.groupId && !msg.receiverId) {
          sendError(ws, 'Missing receiverId for direct message', 'message_error');
          return;
        }
        
        if (msg.groupId) {
          // Group message
          try {
            const message = await Message.create({
              senderId: userId,
              groupId: msg.groupId,
              content: msg.content,
              type: messageType,
              fileUrl: msg.fileUrl,
              fileName: msg.fileName,
              fileSize: msg.fileSize,
              clientId: msg.clientId,
              ...(msg.replyTo ? { replyTo: msg.replyTo } : {}) // NEW: support replyTo
            });
            
            // Update group's lastMessage and updatedAt
            await Group.findByIdAndUpdate(
              msg.groupId,
              {
                lastMessage: {
                  _id: message._id,
                  senderId: message.senderId,
                  content: message.content,
                  type: message.type,
                  fileUrl: message.fileUrl,
                  fileName: message.fileName,
                  fileSize: message.fileSize,
                  timestamp: message.timestamp
                },
                $set: { updatedAt: new Date() }
              }
            );
            
            const group = await Group.findById(msg.groupId);
            if (!group) {
              sendError(ws, 'Group not found', 'message_error');
              return;
            }
            
            let deliveredCount = 0;
            for (const memberId of group.members) {
              if (String(memberId) !== userId) {
                const memberWs = clients.get(String(memberId));
                if (memberWs && memberWs.readyState === ws.OPEN) {
                  // Emit MESSAGE_RECEIVED_EVENT
                  memberWs.send(JSON.stringify({
                    type: 'message_received',
                    groupId: msg.groupId,
                    message: {
                      ...message.toObject(),
                      clientId: message.clientId
                    }
                  }));
                  deliveredCount++;
                }
              }
            }
            
            // Send delivery confirmation to sender
            ws.send(JSON.stringify({
              type: 'message_sent',
              messageId: message._id,
              groupId: msg.groupId,
              delivered: deliveredCount > 0,
              deliveredCount,
              totalMembers: group.members.length - 1,
              timestamp: message.timestamp,
              clientId: message.clientId
            }));
          } catch (error) {
            console.error('Error creating group message:', error);
            sendError(ws, 'Failed to send group message', 'message_error');
          }
        } else {
          // Direct message
          try {
            // Check if recipient is online
            const recipientOnline = clients.has(msg.receiverId);
            
            const message = await Message.create({
              senderId: userId,
              receiverId: msg.receiverId,
              content: msg.content,
              type: messageType,
              fileUrl: msg.fileUrl,
              fileName: msg.fileName,
              fileSize: msg.fileSize,
              clientId: msg.clientId,
              ...(msg.replyTo ? { replyTo: msg.replyTo } : {}) // NEW: support replyTo
            });
            
            // TODO: If you want to track DMs in a chat list, implement a Chat/DirectChat model and update lastMessage/updatedAt here as well.

            // Emit MESSAGE_RECEIVED_EVENT to recipient
            const recipientWs = clients.get(msg.receiverId);
            if (recipientWs && recipientWs.readyState === ws.OPEN) {
              recipientWs.send(JSON.stringify({
                type: 'message_received',
                senderId: userId,
                receiverId: msg.receiverId,
                message: {
                  ...message.toObject(),
                  clientId: message.clientId
                }
              }));
            } else if (pub) {
              // Store for offline delivery via Redis
              await pub.publish(`chat:${msg.receiverId}`, JSON.stringify({
                type: 'message_received',
                senderId: userId,
                receiverId: msg.receiverId,
                message: {
                  ...message.toObject(),
                  clientId: message.clientId
                }
              }));
            }
            // Emit MESSAGE_RECEIVED_EVENT to sender (so sender sees their own message in real time)
            const senderWs = clients.get(userId);
            if (senderWs && senderWs.readyState === ws.OPEN) {
              senderWs.send(JSON.stringify({
                type: 'message_received',
                senderId: userId,
                receiverId: msg.receiverId,
                message: {
                  ...message.toObject(),
                  clientId: message.clientId
                }
              }));
            }
            
            // Send confirmation to sender
            ws.send(JSON.stringify({
              type: 'message_sent',
              messageId: message._id,
              to: msg.receiverId,
              delivered: recipientOnline,
              timestamp: message.timestamp,
              clientId: message.clientId
            }));
          } catch (error) {
            console.error('Error creating direct message:', error);
            sendError(ws, 'Failed to send message', 'message_error');
          }
        }
      } else if (messageType === 'get_history') {
        // Fetch chat history (DM or group)
        try {
          if (msg.groupId) {
            const messages = await Message.find({
              groupId: msg.groupId,
              hiddenFor: { $ne: userId },
              isDeleted: { $ne: true }
            }).sort({ timestamp: 1 });
            ws.send(JSON.stringify({ type: 'history', messages: messages.map(m => ({ ...m.toObject(), clientId: m.clientId })) }));
          } else {
            const messages = await Message.find({
              $or: [
                { senderId: userId, receiverId: msg.with },
                { senderId: msg.with, receiverId: userId }
              ],
              hiddenFor: { $ne: userId },
              isDeleted: { $ne: true }
            }).sort({ timestamp: 1 });
            ws.send(JSON.stringify({ type: 'history', messages: messages.map(m => ({ ...m.toObject(), clientId: m.clientId })) }));
          }
        } catch (error) {
          console.error('Error fetching history:', error);
          sendError(ws, 'Failed to fetch chat history', 'error');
        }
      } else if (messageType === 'seen') {
        // Seen status for DM or group
        try {
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
        } catch (error) {
          console.error('Error marking message as seen:', error);
        }
      } else if (messageType === 'typing') {
        // Typing indicator for DM
        const toWs = clients.get(msg.receiverId);
        if (toWs && toWs.readyState === ws.OPEN) {
          toWs.send(JSON.stringify({ type: 'typing', from: userId, clientId: msg.clientId }));
        }
      } else if (messageType === 'group_typing') {
        // Typing indicator for group
        try {
          const group = await Group.findById(msg.groupId);
          for (const memberId of group.members) {
            if (String(memberId) !== userId) {
              const memberWs = clients.get(String(memberId));
              if (memberWs && memberWs.readyState === ws.OPEN) {
                memberWs.send(JSON.stringify({ type: 'group_typing', groupId: msg.groupId, from: userId, clientId: msg.clientId }));
              }
            }
          }
        } catch (error) {
          console.error('Error sending group typing indicator:', error);
        }
      } else if (messageType === 'delete_message') {
        // WhatsApp-style delete
        try {
          const { messageId, forEveryone } = msg;
          const message = await Message.findById(messageId);
          if (!message) {
            sendError(ws, 'Message not found', 'error');
            return;
          }
          
          if (forEveryone) {
            // Only sender can delete for everyone
            if (String(message.senderId) !== String(userId)) {
              sendError(ws, 'Only the sender can delete for everyone', 'error');
              return;
            }
            message.isDeleted = true;
          } else {
            // Delete for me - add user to hiddenFor array
            if (!message.hiddenFor.includes(userId)) {
              message.hiddenFor.push(userId);
            }
          }
          await message.save();
          
          // Notify all relevant users
          if (message.groupId) {
            const group = await Group.findById(message.groupId);
            for (const memberId of group.members) {
              const memberWs = clients.get(String(memberId));
              if (memberWs && memberWs.readyState === WebSocket.OPEN) {
                memberWs.send(JSON.stringify({
                  type: 'message_deleted',
                  messageId,
                  forEveryone,
                  groupId: message.groupId
                }));
              }
            }
          } else {
            [String(message.senderId), String(message.receiverId)].forEach(uid => {
              if (clients.has(uid) && clients.get(uid).readyState === WebSocket.OPEN) {
                clients.get(uid).send(JSON.stringify({
                  type: 'message_deleted',
                  messageId,
                  forEveryone
                }));
              }
            });
          }
        } catch (error) {
          console.error('Error deleting message:', error);
          sendError(ws, 'Failed to delete message', 'error');
        }
      } else if (messageType === 'get_online_users') {
        // Return list of online users
        ws.send(JSON.stringify({ 
          type: 'online_users', 
          users: Array.from(onlineUsers),
          clientId: msg.clientId
        }));
      } else if (messageType === 'read') {
        // Read receipt for DM
        const toWs = clients.get(msg.receiverId);
        if (toWs && toWs.readyState === ws.OPEN) {
          toWs.send(JSON.stringify({ type: 'read', from: userId, messageId: msg.messageId, clientId: msg.clientId }));
        }
      } else if (messageType === 'create_group') {
        // Create group
        try {
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
        } catch (error) {
          console.error('Error creating group:', error);
          sendError(ws, 'Failed to create group', 'error');
        }
      } else if (messageType === 'group_created') {
        // Handle group creation notification from frontend
        console.log('[WS] Group creation notification:', msg);
        
        // Notify all group members about the new group
        for (const memberId of msg.members) {
          if (String(memberId) !== String(userId)) { // Don't notify the creator
            const memberWs = clients.get(String(memberId));
            if (memberWs && memberWs.readyState === ws.OPEN) {
              memberWs.send(JSON.stringify({ 
                type: 'group_created', 
                group: msg.group,
                members: msg.members,
                createdBy: msg.createdBy,
                clientId: msg.clientId 
              }));
            }
          }
        }
      } else if (messageType === 'edit_message') {
        // Edit a message
        const message = await Message.findById(msg.messageId);
        if (!message) return sendError(ws, 'Message not found', 'error');
        if (message.senderId.toString() !== userId) return sendError(ws, 'Not authorized', 'error');
        message.content = msg.newContent;
        message.isEdited = true;
        await message.save();
        // Notify all relevant users
        if (message.groupId) {
          const group = await Group.findById(message.groupId);
          for (const memberId of group.members) {
            const memberWs = clients.get(String(memberId));
            if (memberWs && memberWs.readyState === ws.OPEN) {
              memberWs.send(JSON.stringify({ type: 'message_edited', messageId: message._id, newContent: msg.newContent }));
            }
          }
        } else {
          [String(message.senderId), String(message.receiverId)].forEach(uid => {
            if (clients.has(uid) && clients.get(uid).readyState === ws.OPEN) {
              clients.get(uid).send(JSON.stringify({ type: 'message_edited', messageId: message._id, newContent: msg.newContent }));
            }
          });
        }
      } else if (messageType === 'react_message') {
        // Add or remove a reaction
        const message = await Message.findById(msg.messageId);
        if (!message) return sendError(ws, 'Message not found', 'error');
        const existing = message.reactions.find(r => r.userId.toString() === userId && r.emoji === msg.emoji);
        if (existing) {
          // Remove reaction
          message.reactions = message.reactions.filter(r => !(r.userId.toString() === userId && r.emoji === msg.emoji));
        } else {
          // Add reaction
          message.reactions.push({ userId, emoji: msg.emoji });
        }
        await message.save();
        // Notify all relevant users
        if (message.groupId) {
          const group = await Group.findById(message.groupId);
          for (const memberId of group.members) {
            const memberWs = clients.get(String(memberId));
            if (memberWs && memberWs.readyState === ws.OPEN) {
              memberWs.send(JSON.stringify({ type: 'message_reacted', messageId: message._id, reactions: message.reactions }));
            }
          }
        } else {
          [String(message.senderId), String(message.receiverId)].forEach(uid => {
            if (clients.has(uid) && clients.get(uid).readyState === ws.OPEN) {
              clients.get(uid).send(JSON.stringify({ type: 'message_reacted', messageId: message._id, reactions: message.reactions }));
            }
          });
        }
      } else if (messageType === 'pin_message') {
        // Pin or unpin a message
        const message = await Message.findById(msg.messageId);
        if (!message) return sendError(ws, 'Message not found', 'error');
        message.pinned = !!msg.pinned;
        await message.save();
        // Notify all relevant users
        if (message.groupId) {
          const group = await Group.findById(message.groupId);
          for (const memberId of group.members) {
            const memberWs = clients.get(String(memberId));
            if (memberWs && memberWs.readyState === ws.OPEN) {
              memberWs.send(JSON.stringify({ type: 'message_pinned', messageId: message._id, pinned: message.pinned }));
            }
          }
        } else {
          [String(message.senderId), String(message.receiverId)].forEach(uid => {
            if (clients.has(uid) && clients.get(uid).readyState === ws.OPEN) {
              clients.get(uid).send(JSON.stringify({ type: 'message_pinned', messageId: message._id, pinned: message.pinned }));
            }
          });
        }
      } else if (messageType === 'star_message') {
        // Star or unstar a message for a user
        const message = await Message.findById(msg.messageId);
        if (!message) return sendError(ws, 'Message not found', 'error');
        if (msg.starred) {
          if (!message.starredBy.includes(userId)) message.starredBy.push(userId);
        } else {
          message.starredBy = message.starredBy.filter(uid => uid.toString() !== userId);
        }
        await message.save();
        ws.send(JSON.stringify({ type: 'message_starred', messageId: message._id, starred: msg.starred }));
      } else if (messageType === 'reply_message') {
        // Send a reply message
        // Just like send_message, but with replyTo
        // ... (reuse send_message logic, add replyTo: msg.replyTo)
        // (For brevity, not duplicating full send_message logic here)
      } else if (messageType === 'forward_message') {
        // Forward a message
        // ... (reuse send_message logic, add forwardedFrom: msg.forwardedFrom)
      } else if (messageType === 'update_group_roles') {
        // Update group roles (admin/member)
        const group = await Group.findById(msg.groupId);
        if (!group) return sendError(ws, 'Group not found', 'error');
        // Only admins can update roles
        if (!group.admins.map(a => a.toString()).includes(userId)) return sendError(ws, 'Not authorized', 'error');
        const member = group.roles.find(r => r.userId.toString() === msg.userId);
        if (member) member.role = msg.role;
        else group.roles.push({ userId: msg.userId, role: msg.role });
        await group.save();
        // Notify all group members
        for (const memberId of group.members) {
          const memberWs = clients.get(String(memberId));
          if (memberWs && memberWs.readyState === ws.OPEN) {
            memberWs.send(JSON.stringify({ type: 'group_role_updated', groupId: group._id, userId: msg.userId, role: msg.role }));
          }
        }
      } else if (messageType === 'block_user') {
        // Block a user
        const user = await User.findById(userId);
        if (!user) return sendError(ws, 'User not found', 'error');
        if (!user.blockedUsers.includes(msg.blockUserId)) user.blockedUsers.push(msg.blockUserId);
        await user.save();
        ws.send(JSON.stringify({ type: 'user_blocked', userId: msg.blockUserId }));
      } else if (messageType === 'mute_chat') {
        // Mute a chat (group or DM)
        const user = await User.findById(userId);
        if (!user) return sendError(ws, 'User not found', 'error');
        if (msg.groupId && !user.mutedChats.includes(msg.groupId)) user.mutedChats.push(msg.groupId);
        await user.save();
        ws.send(JSON.stringify({ type: 'chat_muted', groupId: msg.groupId }));
      } else if (messageType === 'archive_chat') {
        // Archive a chat (group or DM)
        const user = await User.findById(userId);
        if (!user) return sendError(ws, 'User not found', 'error');
        if (msg.groupId && !user.archivedChats.includes(msg.groupId)) user.archivedChats.push(msg.groupId);
        await user.save();
        ws.send(JSON.stringify({ type: 'chat_archived', groupId: msg.groupId }));
      } else if (messageType === 'update_status') {
        // Update user status (online, offline, away, busy)
        const user = await User.findById(userId);
        if (!user) return sendError(ws, 'User not found', 'error');
        user.status = msg.status;
        user.lastSeen = new Date();
        await user.save();
        // Broadcast to all contacts
        for (const [uid, client] of clients.entries()) {
          if (client.readyState === ws.OPEN) {
            client.send(JSON.stringify({ type: 'user_status_updated', userId, status: msg.status, lastSeen: user.lastSeen }));
          }
        }
      } else {
        sendError(ws, `Unknown message type: ${messageType}`, 'error');
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
        client.send(JSON.stringify({ type: 'offline', userId }));
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