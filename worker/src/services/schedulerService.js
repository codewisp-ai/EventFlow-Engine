import { createRedisClient } from '../../../shared/config/redis.js';

const redis = createRedisClient('delayed_job_scheduler');

const STREAM_KEY = 'notifications:stream';
const DELAYED_SET_KEY = 'notifications:delayed';

/**
 * Scans the delay buffer for expired timestamps and feeds them back to active workers.
 */
export const pollDelayedJobs = async () => {
  try {
    const now = Date.now();

    // Query Sorted Set for any values whose score is between 0 (absolute past) and the current millisecond
    // Fetch only 1 message at a time to keep execution tightly bounded and atomic
    const jobs = await redis.zrangebyscore(DELAYED_SET_KEY, 0, now, 'LIMIT', 0, 1);

    if (!jobs || jobs.length === 0) return;

    const jobDataRaw = jobs[0];
    const { originalJobId, type, payload } = JSON.parse(jobDataRaw);

    console.log(`[Scheduler Engine] Job ID ${originalJobId} delay expired. Moving back to active stream...`);

    // Remove from Sorted Set first to ensure no other scheduler double-allocates it
    const removed = await redis.zrem(DELAYED_SET_KEY, jobDataRaw);
    
    if (removed > 0) {
      // Re-inject the job parameters into the primary ingestion stream
      await redis.xadd(
        STREAM_KEY, 'MAXLEN', '~', 100000, '*',
        'type', type,
        'payload', JSON.stringify(payload),
        'timestamp', Date.now().toString()
      );
      console.log(`[Scheduler Engine] Job ID ${originalJobId} successfully re-queued.`);
    }

  } catch (error) {
    console.error('[Scheduler Engine Error]: Failure processing delayed set keys:', error.message);
  }
};

/**
 * Initializes the high-frequency clock daemon.
 */
export const startSchedulerDaemon = () => {
  console.log('[Scheduler Engine] Launching high-frequency delay poller (1s intervals)...');
  setInterval(async () => {
    await pollDelayedJobs();
  }, 1000);
};