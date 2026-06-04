import express from 'express';
import { createRedisClient } from '../shared/config/redis.js';
import notificationRoutes from './src/routes/notificationRoutes.js';

const app = express();
const PORT = process.env.PORT || 8080;

app.use(express.json());

// Mount notification ingestion routes
app.use('/api/v1/notifications', notificationRoutes);

// Initialize a dedicated client for our Producer API operations
const redisProducerClient = createRedisClient('producer_api');

app.get('/health', async (req, res) => {
  try {
    // Basic ping to verify infrastructure viability
    await redisProducerClient.ping();
    return res.status(200).json({ status: 'UP', service: 'Producer API' });
  } catch (error) {
    return res.status(500).json({ status: 'DOWN', error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`[Producer API] Ingestion engine listening natively on port ${PORT}`);
});