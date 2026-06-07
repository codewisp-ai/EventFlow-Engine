import { createRedisClient } from '../../../shared/config/redis.js';

const redis = createRedisClient('recovery_agent_service');

const STREAM_KEY = 'notifications:stream';
const DLQ_STREAM_KEY = 'notifications:dlq';
const CONSUMER_GROUP = 'notification_workers_group';
const RECOVERY_AGENT_NAME = 'recovery_agent_node';

const MIN_IDLE_TIME_MS = 60000; // 60 seconds stalled threshold
const MAX_RECLAIM_ATTEMPTS = 3;  // Protect against infinite loop "poison pills"

/**
 * Sweeps the consumer group PEL, claims stalled messages, and handles retries or DLQ shifts.
 */
export const runRecoverySweep = async () => {
  let startId = '0-0'; // Start scanning from the absolute beginning of the PEL history

  try {
    console.log('[Recovery Service] Initiating stalled job scan across PEL...');

    // Execute atomic XAUTOCLAIM (Redis 6.2+)
    // Returns: [nextStartId, [ [messageId, [fields...] ], ... ], [deletedMessageIds...] ]
    const result = await redis.xautoclaim(
      STREAM_KEY,
      CONSUMER_GROUP,
      RECOVERY_AGENT_NAME,
      MIN_IDLE_TIME_MS,
      startId,
      'COUNT', '10'
    );

    if (!result) return;

    const [nextStartId, claimedMessages] = result;
    
    if (claimedMessages.length === 0) {
      console.log('[Recovery Service] Sweep complete: Zero stalled messages detected.');
      return;
    }

    for (const message of claimedMessages) {
      const [messageId, fieldsArray] = message;
      
      // Transform flat Redis array mapping into a clear JavaScript Object
      const fields = {};
      for (let i = 0; i < fieldsArray.length; i += 2) {
        fields[fieldsArray[i]] = fieldsArray[i + 1];
      }

      const trackingKey = `job:attempts:${messageId}`;
      
      // Fetch current attempt state from our established counter tracking system
      const currentAttemptsRaw = await redis.get(trackingKey);
      const currentAttempts = currentAttemptsRaw ? parseInt(currentAttemptsRaw, 10) : 1;

      console.warn(`[Recovery Service] Captured Stalled Job [${messageId}]. Total claims: ${currentAttempts}`);

      if (currentAttempts >= MAX_RECLAIM_ATTEMPTS) {
        console.error(`[Recovery Service] Job [${messageId}] exceeded limits. Diverting to DLQ.`);
        
        // Atomically shift data directly into the DLQ stream
        await redis.xadd(
          DLQ_STREAM_KEY,
          'MAXLEN', '~', 50000,
          '*',
          'originalJobId', messageId,
          'type', fields.type || 'unknown',
          'payload', fields.payload || '{}',
          'failedAt', Date.now().toString(),
          'errorReason', 'Stalled Job Recovery: Max Reclaim Attempts Exceeded'
        );

        // Acknowledge out of the main stream to clear out the blocked pipeline state
        await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
        await redis.del(trackingKey);
      } else {
        // Increment processing counter to maintain accurate state representation
        await redis.incr(trackingKey);
        
        // Re-inject back into the main stream processing buffer to trigger available live workers
        await redis.xadd(
          STREAM_KEY,
          'MAXLEN', '~', 100000,
          '*',
          'type', fields.type,
          'payload', fields.payload,
          'timestamp', Date.now().toString()
        );

        // Remove the old stalled message entry from the PEL to complete the transfer lifecycle cleanly
        await redis.xack(STREAM_KEY, CONSUMER_GROUP, messageId);
        console.log(`[Recovery Service] Job [${messageId}] successfully re-enqueued under new task allocation.`);
      }
    }
  } catch (error) {
    console.error('[Recovery Service Error]: Execution failure during PEL sweep:', error.message);
  }
};

/**
 * Initializes the autonomous recovery background loop daemon.
 */
export const startRecoveryAgent = () => {
  console.log('[Recovery Service] Starting detached recovery daemon loop (30s interval)...');
  setInterval(async () => {
    await runRecoverySweep();
  }, 30000);
};