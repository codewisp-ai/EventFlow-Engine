import { appendToNotificationStream } from '../services/notificationService.js';

export const ingestNotification = async (req, res) => {
  try {
    const { type, payload } = req.body;

    if (!type || !payload) {
      return res.status(400).json({
        error: 'Invalid Payload Structure',
        details: 'Fields "type" and "payload" are strictly required.'
      });
    }

    const validTypes = ['email', 'sms', 'webhook', 'push'];
    if (!validTypes.includes(type)) {
      return res.status(422).json({
        error: 'Unprocessable Entity',
        details: `Notification type "${type}" is unsupported. Valid types: ${validTypes.join(', ')}`
      });
    }

    // Ingest asynchronously into the Redis Broker Stream Buffer
    const jobId = await appendToNotificationStream(type, payload);

    // Return 202 Accepted: The request is stored safely, processing is decoupled
    return res.status(202).json({
      success: true,
      message: 'Notification event accepted and queued successfully.',
      jobId
    });

  } catch (error) {
    console.error('[Ingestion Controller Error]:', error.message);
    return res.status(500).json({
      error: 'Internal Ingestion Failure',
      details: error.message
    });
  }
};