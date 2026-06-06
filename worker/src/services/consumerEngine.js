import { createRedisClient } from '../../../shared/config/redis.js';
import { processNotificationJob } from '../processors/notificationProcessor.js';

const redis = createRedisClient('consumer_worker_stream');

const STREAM_KEY = 'notifications:stream';
const DLQ_STREAM_KEY = 'notifications:dlq';
const CONSUMER_GROUP = 'notification_workers_group';
const CONSUMER_NAME = process.env.HOSTNAME || 'worker_node_alpha';

const MAX_ATTEMPTS = 3;

export const initializeConsumerGroup = () => {
  return new Promise((resolve, reject) => {
    const executeGroupCreation = async () => {
      try {
        await redis.xgroup('CREATE', STREAM_KEY, CONSUMER_GROUP, '$', 'MKSTREAM');
        console.log(`[Consumer Engine] Consumer group [${CONSUMER_GROUP}] verified/established.`);
        resolve();
      } catch (error) {
        if (error.message.includes('BUSYGROUP')) {
          resolve();
        } else {
          reject(error);
        }
      }
    };

    if (redis.status === 'ready') {
      executeGroupCreation();
    } else {
      redis.once('ready', executeGroupCreation);
    }
  });
};

/**
 * Routes a permanently broken notification job to the Dead Letter Queue stream buffer.
 */
const routeToDeadLetterQueue = async (messageId, type, payload, errorMessage) => {
  console.error(`[DLQ Router] CRITICAL: Job ID ${messageId} reached max retries. Routing to DLQ...`);
  
  try {
    await redis.xadd(
      DLQ_STREAM_KEY,
      'MAXLEN', '~', 50000, // Keep DLQ bounded to save memory caps
      '*',
      'originalJobId', messageId,
      'type', type,
      'payload', JSON.stringify(payload),
      'failedAt', Date.now().toString(),
      'errorReason', errorMessage
    );
    console.log(`[DLQ Router] Job ID ${messageId} successfully isolated in ${DLQ_STREAM_KEY}`);
  } catch (dlqError) {
    console.error('[DLQ Router] CRITICAL EXCEPTION: Failed to write to DLQ stream:', dlqError.message);
  }
};

export const startConsumerLoop = async () => {
  console.log(`[Consumer Engine] Resilient poller active. Listening as: ${CONSUMER_NAME}`);

  while (true) {
    let currentMessageId = null;
    let notificationType = null;
    let payload = null;

    try {
      const result = await redis.xreadgroup(
        'GROUP', CONSUMER_GROUP, CONSUMER_NAME,
        'COUNT', '1',
        'BLOCK', '2000',
        'STREAMS', STREAM_KEY,
        '>'
      );

      if (!result || result.length === 0) continue;

      const [_, messages] = result[0];
      const [messageId, fieldsArray] = messages[0];
      currentMessageId = messageId;

      // Unpack flat Redis Stream array into object structures
      const fields = {};
      for (let i = 0; i < fieldsArray.length; i += 2) {
        fields[fieldsArray[i]] = fieldsArray[i + 1];
      }

      notificationType = fields.type;
      payload = JSON.parse(fields.payload);

      // Track execution attempts using a Redis tracking key specific to this message ID
      const trackingKey = `job:attempts:${messageId}`;
      const attempts = await redis.incr(trackingKey);
      await redis.expire(trackingKey, 86400); // 24-hour expiration safety valve

      console.log(`[Consumer Engine] Processing Job ID: ${messageId} (Attempt ${attempts}/${MAX_ATTEMPTS})`);

      try {
        // Execute core business processor block inside a secondary execution sandbox
        await processNotificationJob(notificationType, payload);

        // Success: Clean settlement handshake
        await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
        await redis.del(trackingKey); // Clean up the tracking counter key
        console.log(`[Consumer Engine] Job ID: ${messageId} processed and settled cleanly (XACK).`);

      } catch (jobExecutionError) {
        console.error(`[Consumer Engine] Execution failure on Job ID ${messageId}:`, jobExecutionError.message);

        if (attempts >= MAX_ATTEMPTS) {
          // Max attempts reached: Route payload to the DLQ stream
          await routeToDeadLetterQueue(messageId, notificationType, payload, jobExecutionError.message);
          
          // Settle the job out of the main queue's PEL to prevent it from getting stuck forever
          await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
          await redis.del(trackingKey);
        } else {
          // Re-throw the error to trigger our loop back-off without acknowledging the item
          throw jobExecutionError;
        }
      }

    } catch (loopError) {
      console.error('[Consumer Engine Loop Backoff]:', loopError.message);
      // Wait briefly before reclaiming to avoid hammering infrastructure during systemic errors
      await new Promise((resolve) => setTimeout(resolve, 1500));
    }
  }
};