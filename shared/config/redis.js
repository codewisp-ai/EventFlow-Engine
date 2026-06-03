import Redis from 'ioredis';

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

const baseOptions = {
  // Prevent application memory bloat if Redis drops connection under heavy load
  enableOfflineQueue: false, 
  maxRetriesPerRequest: 3,
  retryStrategy(times) {
    // Exponential back-off with a maximum delay cap of 2 seconds
    const delay = Math.min(times * 50, 2000);
    return delay;
  }
};

/**
 * Creates a unique Redis Client instance.
 * @param {string} clientName - Identifies the connection purpose within Redis client list
 */
export const createRedisClient = (clientName) => {
  const client = new Redis(REDIS_URL, {
    ...baseOptions,
    clientName: `event_engine:${clientName}`
  });

  client.on('connect', () => {
    console.log(`[Redis] Connection initialized for: ${clientName}`);
  });

  client.on('error', (err) => {
    console.error(`[Redis] Critical error on client [${clientName}]:`, err.message);
  });

  return client;
};