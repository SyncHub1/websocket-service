import mongoose from 'mongoose';

const GroupSchema = new mongoose.Schema({
  name: { type: String, required: true },
  avatar: { type: String },
  members: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  admins: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  roles: [{ userId: { type: mongoose.Schema.Types.ObjectId, ref: 'User' }, role: { type: String, enum: ['admin', 'member'], default: 'member' } }],
  description: { type: String, default: '' },
  isArchived: { type: Boolean, default: false },
  isMuted: { type: Boolean, default: false },
  pinnedMessage: { type: mongoose.Schema.Types.ObjectId, ref: 'Message', default: null },
  isTyping: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  lastMessage: {
    type: Object, // Can be changed to message ID if preferred
    default: null
  }
}, {
  timestamps: true // Adds createdAt and updatedAt
});

const Group = mongoose.model('Group', GroupSchema);
export default Group; 