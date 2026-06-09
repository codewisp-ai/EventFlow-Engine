import { createRedisClient } from '../../../shared/config/redis.js';
import { processNotificationJob } from '../processors/notificationProcessor.js';

const redis = createRedisClient('consumer_worker_stream');

const STREAM_KEY = 'notifications:stream';
const DELAYED_SET_KEY = 'notifications:delayed';
const DLQ_STREAM_KEY = 'notifications:dlq';
const CONSUMER_GROUP = 'notification_workers_group';
const CONSUMER_NAME = process.env.HOSTNAME || 'worker_node_alpha';

const MAX_ATTEMPTS = 5;
const BASE_DELAY_MS = 1000; // Start with 1 second base delay
const MAX_DELAY_MS = 60000; // Cap backoff intervals at 60 seconds

const pubClient = createRedisClient('worker_pub_bus');
const PUB_SUB_CHANNEL = 'job:status:updates';

/**
 * Atomically broadcasts job lifecycle updates onto our cross-container infrastructure bus.
 */
const broadcastJobStatus = async (jobId, status, details = {}) => {
  try {
    const payload = JSON.stringify({ jobId, status, ...details });
    await pubClient.publish(PUB_SUB_CHANNEL, payload);
    console.log(`[Pub/Sub Bus] Broadcasted status update for Job ${jobId}: ${status}`);
  } catch (err) {
    console.error('[Pub/Sub Bus] Critical failure broadcasting event state update:', err.message);
  }
};

export const initializeConsumerGroup = () => {
  return new Promise((resolve, reject) => {
    const executeGroupCreation = async () => {
      try {
        await redis.xgroup('CREATE', STREAM_KEY, CONSUMER_GROUP, '$', 'MKSTREAM');
        resolve();
      } catch (error) {
        if (error.message.includes('BUSYGROUP')) resolve();
        else reject(error);
      }
    };
    if (redis.status === 'ready') executeGroupCreation();
    else redis.once('ready', executeGroupCreation);
  });
};

/**
 * Schedules a message for future execution inside the Redis ZSET delay buffer.
 */
const scheduleExponentialBackoff = async (messageId, type, payload, attempt) => {
  // Formula: delay = min(baseDelay * 2^attempt + jitter, maxDelay)
  const exponentialDelay = Math.min(BASE_DELAY_MS * Math.pow(2, attempt), MAX_DELAY_MS);
  const jitter = Math.floor(Math.random() * 500); // 0-500ms randomized jitter
  const finalDelay = exponentialDelay + jitter;
  
  const targetExecutionTime = Date.now() + finalDelay;
  
  // Pack the original job parameters into a JSON transport payload
  const delayedJobMetadata = JSON.stringify({ originalJobId: messageId, type, payload });

  // Store inside Sorted Set: Score = Target Epoch Timestamp
  await redis.zadd(DELAYED_SET_KEY, targetExecutionTime, delayedJobMetadata);
  
  console.warn(`[Backoff Engine] Job ID ${messageId} failed. Scheduled retry #${attempt} in ${finalDelay}ms.`);
};

const routeToDeadLetterQueue = async (messageId, type, payload, errorMessage) => {
  console.error(`[DLQ Router] CRITICAL: Job ID ${messageId} reached absolute limit (${MAX_ATTEMPTS}). Dumping to DLQ...`);
  await redis.xadd(
    DLQ_STREAM_KEY, 'MAXLEN', '~', 50000, '*',
    'originalJobId', messageId,
    'type', type,
    'payload', JSON.stringify(payload),
    'failedAt', Date.now().toString(),
    'errorReason', errorMessage
  );
};



/**
 * Periodically queries Redis to update the live DLQ depth gauge telemetry metric.
 */
const updateDlqMetrics = async () => {
  try {
    const streamInfo = await redis.xinfo('STREAM', DLQ_STREAM_KEY).catch(() => null);
    const depth = streamInfo ? streamInfo.length : 0;
    dlqDepthGauge.set(depth);
  } catch (err) {
    // Suppress errors if DLQ stream doesn't exist on Redis yet
  }
};

export const startConsumerLoop = async () => {
  console.log(`[Consumer Engine] Resilient telemetry poller online. Listening as: ${CONSUMER_NAME}`);
  
  // Start tracking the live background DLQ metric gauge sweep on a steady timer
  setInterval(updateDlqMetrics, 10000);

  while (true) {
      console.log(`[Consumer Engine] Acquired Job ID: ${messageId} (Attempt ${attempts}/${MAX_ATTEMPTS})`);
      
      // State Mutation 1: Notify client that the task has been pulled out of the queue and is now running
      await broadcastJobStatus(messageId, 'processing');

      activeJobsGauge.inc();
      const endTimer = notificationProcessingDuration.startTimer({ type: notificationType });

      try {
        await processNotificationJob(notificationType, payload);
        
        endTimer();
        activeJobsGauge.dec();
        notificationsProcessedTotal.inc({ status: 'success', type: notificationType });

        await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
        await redis.del(trackingKey);
        
        // State Mutation 2: Notify client that processing succeeded and the job is settled
        await broadcastJobStatus(messageId, 'delivered', { recipient: payload.recipient });
        console.log(`[Consumer Engine] Job ID: ${messageId} settled successfully (XACK).`);

      } catch (jobExecutionError) {
        endTimer();
        activeJobsGauge.dec();
        notificationsProcessedTotal.inc({ status: 'failed', type: notificationType });

        if (attempts >= MAX_ATTEMPTS) {
          await routeToDeadLetterQueue(messageId, notificationType, payload, jobExecutionError.message);
          await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
          await redis.del(trackingKey);
          await updateDlqMetrics();
          
          // State Mutation 3: Notify client that retry attempts failed and the item is isolated in the DLQ
          await broadcastJobStatus(messageId, 'failed_dlq', { reason: jobExecutionError.message });
        } else {
          await scheduleExponentialBackoff(messageId, notificationType, payload, attempts);
          await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
          
          // State Mutation 4: Notify client that a temporary error occurred and a delayed retry is scheduled
          await broadcastJobStatus(messageId, 'retrying', { attempt: attempts, nextIn: `${Math.min(BASE_DELAY_MS * Math.pow(2, attempts), MAX_DELAY_MS)}ms` });
        }
      }
    let currentMessageId = null;
    let notificationType = null;
    let payload = null;

    try {
      const result = await redis.xreadgroup(
        'GROUP', CONSUMER_GROUP, CONSUMER_NAME,
        'COUNT', '1', 'BLOCK', '2000',
        'STREAMS', STREAM_KEY, '>'
      );

      if (!result || result.length === 0) continue;

      const [_, messages] = result[0];
      const [messageId, fieldsArray] = messages[0];
      currentMessageId = messageId;

      const fields = {};
      for (let i = 0; i < fieldsArray.length; i += 2) {
        fields[fieldsArray[i]] = fieldsArray[i + 1];
      }

      notificationType = fields.type;
      payload = JSON.parse(fields.payload);

      const trackingKey = `job:attempts:${messageId}`;
      const attempts = await redis.incr(trackingKey);
      await redis.expire(trackingKey, 86400);

      console.log(`[Consumer Engine] Acquired Job ID: ${messageId} (Attempt ${attempts}/${MAX_ATTEMPTS})`);

      // Set Telemetry State: Increment active concurrent worker execution allocations
      activeJobsGauge.inc();
      
      // Start processing performance duration stopwatch tracking
      const endTimer = notificationProcessingDuration.startTimer({ type: notificationType });

      try {
        await processNotificationJob(notificationType, payload);
        
        // Success Path Telemetry Allocation
        endTimer();
        activeJobsGauge.dec();
        notificationsProcessedTotal.inc({ status: 'success', type: notificationType });

        await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
        await redis.del(trackingKey);
        console.log(`[Consumer Engine] Job ID: ${messageId} settled successfully (XACK).`);

      } catch (jobExecutionError) {
        // Failure Path Telemetry Allocation
        endTimer();
        activeJobsGauge.dec();
        notificationsProcessedTotal.inc({ status: 'failed', type: notificationType });

        if (attempts >= MAX_ATTEMPTS) {
          await routeToDeadLetterQueue(messageId, notificationType, payload, jobExecutionError.message);
          await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
          await redis.del(trackingKey);
          await updateDlqMetrics(); // Instantly refresh the DLQ gauge metric state
        } else {
          await scheduleExponentialBackoff(messageId, notificationType, payload, attempts);
          await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
        }
      }

    } catch (loopError) {
      console.error('[Consumer Engine Generic Error]:', loopError.message);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
};