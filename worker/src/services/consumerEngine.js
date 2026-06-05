import { createRedisClient } from '../../../shared/config/redis.js';
import { processNotificationJob } from '../processors/notificationProcessor.js';

const redis = createRedisClient('consumer_worker_stream');

const STREAM_KEY = 'notifications:stream';
const CONSUMER_GROUP = 'notification_workers_group';
const CONSUMER_NAME = process.env.HOSTNAME || 'worker_node_alpha';

/**
 * Ensures our named Consumer Group exists within the Redis Stream boundary.
 * Connection-aware wrapper to eliminate Docker container startup race conditions.
 */
export const initializeConsumerGroup = () => {
  return new Promise((resolve, reject) => {
    
    // Internal function execution logic once connection is verified writeable
    const executeGroupCreation = async () => {
      try {
        await redis.xgroup('CREATE', STREAM_KEY, CONSUMER_GROUP, '$', 'MKSTREAM');
        console.log(`[Consumer Engine] Consumer group [${CONSUMER_GROUP}] established successfully.`);
        resolve();
      } catch (error) {
        if (error.message.includes('BUSYGROUP')) {
          console.log(`[Consumer Engine] Consumer group [${CONSUMER_GROUP}] verified active.`);
          resolve();
        } else {
          console.error('[Consumer Engine] Critical group initialization failure:', error.message);
          reject(error);
        }
      }
    };

    // If the socket connection is already open and writeable, run immediately
    if (redis.status === 'ready') {
      executeGroupCreation();
    } else {
      // Otherwise, subscribe to the ioredis ready event before executing commands
      redis.once('ready', () => {
        console.log('[Consumer Engine] Redis client status verified READY. Proceeding with setup...');
        executeGroupCreation();
      });

      // Fail fast if the client throws an error during the connection handshake phase
      redis.once('error', (err) => {
        reject(new Error(`Redis connection dropped during initial engine boot handshake: ${err.message}`));
      });
    }
  });
};

/**
 * Continuous blocking poll loop execution engine.
 */
export const startConsumerLoop = async () => {
  console.log(`[Consumer Engine] Poller active. Listening as node: ${CONSUMER_NAME}`);

  while (true) {
    try {
      // XREADGROUP COUNT 1 BLOCK 2000 STREAMS notifications:stream >
      // '>' means: Give me messages that have NEVER been delivered to any other consumer.
      // BLOCK 2000 means: If no messages are there, hold the connection open for 2 seconds before recycling.
      const result = await redis.xreadgroup(
        'GROUP', CONSUMER_GROUP, CONSUMER_NAME,
        'COUNT', '1',
        'BLOCK', '2000',
        'STREAMS', STREAM_KEY,
        '>'
      );

      // If the result array is null or empty, the block timed out with no new items. Continue polling.
      if (!result || result.length === 0) continue;

      const [streamName, messages] = result[0];
      const [messageId, fieldsArray] = messages[0];

      // Reconstruct the flat array ['type', 'email', 'payload', '...'] into a clean JavaScript Object
      const fields = {};
      for (let i = 0; i < fieldsArray.length; i += 2) {
        fields[fieldsArray[i]] = fieldsArray[i + 1];
      }

      const notificationType = fields.type;
      const payload = JSON.parse(fields.payload);

      console.log(`[Consumer Engine] Acquired Job ID: ${messageId}. Processing...`);

      // Pass the job to our dedicated processor strategy
      await processNotificationJob(notificationType, payload);

      // THE HANDSHAKE SETTLEMENT --------------------------------------
      // Acknowledge the message was processed cleanly to remove it from our PEL
      await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
      console.log(`[Consumer Engine] Job ID: ${messageId} successfully SETTLED (XACK).`);

    } catch (error) {
      console.error('[Consumer Engine Loop Error]: Failed to handle stream element.', error.message);
      // Backoff briefly on error to avoid spinning aggressively if infrastructure issues arise
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
};