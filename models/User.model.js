import mongoose from 'mongoose';

const UserSchema = new mongoose.Schema({
  name: { type: String, required: true },
  username: { type: String, required: true, unique: true },
  email: { type: String, required: true, unique: true },
  avatar: { type: String },
  password: { type: String, required: true },
  createdAt: { type: Date, default: Date.now },
  blockedUsers: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  mutedChats: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Group' }],
  archivedChats: [{ type: mongoose.Schema.Types.ObjectId, ref: 'Group' }],
  lastSeen: { type: Date, default: Date.now },
  status: { type: String, enum: ['online', 'offline', 'away', 'busy'], default: 'offline' }
});

const User = mongoose.model('User', UserSchema);
export default User; 