import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import { createRedisClient } from '../shared/config/redis.js';
import notificationRoutes from './src/routes/notificationRoutes.js';

const app = express();
const PORT = process.env.PORT || 8080;

// Wrap express into an HTTP core framework layer
const server = http.createServer(app);

// Initialize Socket.IO with open CORS access rules for local dev testing
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.json());
app.use('/api/v1/notifications', notificationRoutes);

// --- MULTI-CONTAINER REDIS PUB/SUB GATEWAY MAPPING ---
const subClient = createRedisClient('api_sub_gateway');
const PUB_SUB_CHANNEL = 'job:status:updates';

// Configure the client connection to listen to the cross-container communication channel
subClient.subscribe(PUB_SUB_CHANNEL, (err) => {
  if (err) console.error('[Socket Gateway] Failed to wire up Redis Pub/Sub subscription:', err.message);
  else console.log(`[Socket Gateway] Subscribed cleanly to cross-container channel: ${PUB_SUB_CHANNEL}`);
});

// Intercept streaming updates from the background worker and push them over live websockets
subClient.on('message', (channel, message) => {
  if (channel === PUB_SUB_CHANNEL) {
    const { jobId, status, ...details } = JSON.parse(message);
    console.log(`[Socket Gateway] Pushing real-time packet update for room [${jobId}] -> Status: ${status}`);
    
    // Push the state message straight to the specific room named after the jobId
    io.to(jobId).emit('job_update', { jobId, status, details });
  }
});

// --- CLIENT SOCKET CONNECTION ROUTER ---
io.on('connection', (socket) => {
  console.log(`[Socket Client Connected] Session ID string mapped: ${socket.id}`);

  // Allow clients to subscribe to updates for a specific jobId
  socket.on('subscribe_job', (jobId) => {
    socket.join(jobId);
    console.log(`[Socket Gateway] Client [${socket.id}] joined room tracking target ID: ${jobId}`);
  });

  socket.on('disconnect', () => {
    console.log(`[Socket Client Disconnected] Left active context: ${socket.id}`);
  });
});

app.get('/health', (req, res) => {
  return res.status(200).json({ status: 'UP', service: 'Producer API Streaming Ingestion Gateway' });
});

// IMPORTANT: Fire up the core http server wrapper instance, not the raw express app instance!
server.listen(PORT, () => {
  console.log(`[Producer API] Streaming ingestion engine listening natively on port ${PORT}`);
});