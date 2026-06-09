import client from 'prom-client';
import http from 'http';

// Create a custom tracking registry context
const registry = new client.Registry();

// Enable standard Node.js runtime collection metrics (CPU, Memory Heap, Event Loop lag)
client.collectDefaultMetrics({ register: registry, prefix: 'vortex_mq_' });

// --- SYSTEM WORKSPACE CUSTOM METRICS ---

export const notificationsProcessedTotal = new client.Counter({
  name: 'vortex_mq_notifications_processed_total',
  help: 'Cumulative count of notification events processed by this worker node array',
  labelNames: ['status', 'type'],
  registers: [registry]
});

export const notificationProcessingDuration = new client.Histogram({
  name: 'vortex_mq_notification_processing_duration_seconds',
  help: 'High-fidelity duration tracking histogram for worker job executions',
  labelNames: ['type'],
  buckets: [0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10], // Execution time testing windows
  registers: [registry]
});

export const activeJobsGauge = new client.Gauge({
  name: 'vortex_mq_worker_active_jobs',
  help: 'Current active concurrent notifications running inside this runtime thread',
  registers: [registry]
});

export const dlqDepthGauge = new client.Gauge({
  name: 'vortex_mq_dlq_depth',
  help: 'Total count of poison pill notification events isolated inside the DLQ stream',
  registers: [registry]
});

/**
 * Boots a lightweight telemetry HTTP server inside the worker container 
 * to expose raw Prometheus metrics to the scrape bus.
 */
export const startMetricsServer = (port = 9100) => {
  const server = http.createServer(async (req, res) => {
    if (req.url === '/metrics') {
      try {
        res.setHeader('Content-Type', registry.contentType);
        res.end(await registry.metrics());
      } catch (err) {
        res.statusCode = 500;
        res.end(err.message);
      }
    } else {
      res.statusCode = 404;
      res.end('Not Found');
    }
  });

  server.listen(port, () => {
    console.log(`[Metrics Server] Telemetry stream actively broadcast on port ${port}/metrics`);
  });
};