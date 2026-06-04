import { createRedisClient } from '../../../shared/config/redis.js';

// Initialize our exclusive producer channel client
const redis = createRedisClient('producer_stream_service');

const STREAM_KEY = 'notifications:stream';
const MAX_STREAM_MAXLEN = 100000; // Evict older messages to preserve memory caps

export const appendToNotificationStream = async (notificationType, payload) => {
  const messageData = [
    'type', notificationType,
    'payload', JSON.stringify(payload),
    'timestamp', Date.now().toString()
  ];

  // Execute an atomic O(1) Stream Append
  // 'MAXLEN ~ 100000' keeps the stream bounded to the latest 100k items efficiently
  const messageId = await redis.xadd(
    STREAM_KEY,
    'MAXLEN', '~', MAX_STREAM_MAXLEN,
    '*', // Instructs Redis to generate a time-series ID automatically
    ...messageData
  );

  return messageId;
};