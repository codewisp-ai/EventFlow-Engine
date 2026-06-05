import { initializeConsumerGroup, startConsumerLoop } from './src/services/consumerEngine.js';

console.log('[Worker] Launching headless consumer background processes...');

const startWorkerContext = async () => {
  try {
    // 1. Establish structural cluster group state
    await initializeConsumerGroup();

    // 2. Fire up the non-blocking infinite polling engine loop
    startConsumerLoop();
    
  } catch (error) {
    console.error('[Worker Boot Error]: Critical initialization halt:', error.message);
    process.exit(1);
  }
};

startWorkerContext();








/*import { createRedisClient } from '../shared/config/redis.js';

console.log('[Worker] Launching headless consumer background process...');

// Initialize dedicated clients for processing and stream monitoring loops
const redisWorkerClient = createRedisClient('worker_main');

// Graceful shutdown protocol
const handleShutdown = async (signal) => {
  console.log(`[Worker] Received ${signal}. Terminating Redis clients cleanly...`);
  try {
    await redisWorkerClient.quit();
    console.log('[Worker] Redis contexts cleanly disconnected. Safe exit.');
    process.exit(0);
  } catch (err) {
    console.error('[Worker] Error during explicit connection termination:', err);
    process.exit(1);
  }
};

process.on('SIGTERM', () => handleShutdown('SIGTERM'));
process.on('SIGINT', () => handleShutdown('SIGINT'));

// Keep process running smoothly
setInterval(() => {}, 1000); 
*/