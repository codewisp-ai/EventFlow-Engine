# EventFlow Engine

## Overview

EventFlow Engine is a production-inspired distributed event processing and notification delivery platform built using Node.js, Redis Streams, Consumer Groups, and Docker.

The system demonstrates how modern backend services handle asynchronous workloads using Event-Driven Architecture, horizontal worker scaling, fault tolerance mechanisms, retry management, and Dead Letter Queues.

Instead of processing requests synchronously, incoming notification events are immediately persisted into Redis Streams and processed asynchronously by distributed worker nodes. This architecture improves throughput, resiliency, and scalability under heavy traffic conditions.

---

## Architecture

```text
                   ┌────────────────────┐
                   │   Producer API     │
                   └─────────┬──────────┘
                             │
                             │ XADD
                             ▼

                 Redis Stream Buffer
                 notifications:stream
                             │
                             │ XREADGROUP
                             ▼

     ┌────────────┬────────────┬────────────┐
     │            │            │            │
     ▼            ▼            ▼            ▼

 Worker 1     Worker 2     Worker 3     Worker N

     │            │            │
     └────────────┴────────────┘
                  │
                  ▼

        Notification Processors
         Email / SMS / Webhook

                  │
                  ▼

             Success → XACK

             Failure
                  │
                  ▼

      Retry Tracking & DLQ Routing

                  │
                  ▼

        notifications:dlq
```

---

## Core Features

### Event-Driven Architecture

Implements asynchronous event processing using Redis Streams.

### Producer-Consumer Pattern

Producer API ingests notification requests while independent worker nodes process events in the background.

### Redis Streams

Provides durable append-only event storage with O(1) ingestion performance.

### Consumer Groups

Enables horizontal worker scaling while guaranteeing that each event is delivered to only one consumer within a group.

### Pending Entries List (PEL)

Tracks unacknowledged messages and provides the foundation for fault recovery.

### Retry Management

Tracks processing attempts using Redis atomic counters.

### Dead Letter Queue (DLQ)

Routes permanently failing jobs into a dedicated Redis Stream for inspection and remediation.

### Horizontal Scaling

Workers can be scaled independently using Docker Compose.

### Fault Tolerance

Supports resilient event processing through acknowledgment-based delivery guarantees.

### Dockerized Infrastructure

Runs as isolated services connected through an internal bridge network.

---

## Technologies Used

### Backend

* Node.js
* Express.js

### Messaging Infrastructure

* Redis Streams
* Redis Consumer Groups

### Distributed Systems Concepts

* Event Driven Architecture
* Producer Consumer Pattern
* Competing Consumers Pattern
* At-Least-Once Delivery
* Dead Letter Queues
* Retry Management
* Fault Isolation

### Infrastructure

* Docker
* Docker Compose

---

## Project Structure

```text
project2-event-engine/

├── api/
│   ├── src/
│   │   ├── controllers/
│   │   ├── routes/
│   │   └── services/
│   ├── package.json
│   └── server.js

├── worker/
│   ├── src/
│   │   ├── processors/
│   │   └── services/
│   ├── package.json
│   └── index.js

├── shared/
│   └── config/
│       └── redis.js

├── docker/
│   └── redis.conf

└── docker-compose.yml
```

---

## Key Redis Data Structures

### Redis Streams

Used as the primary ingestion buffer.

```text
notifications:stream
```

Stores incoming notification events.

---

### Dead Letter Queue Stream

```text
notifications:dlq
```

Stores permanently failing jobs.

---

### Redis Strings

Used for retry tracking.

```text
job:attempts:<messageId>
```

Maintains atomic execution attempt counts.

---

## Running the Project

### Build Containers

```bash
docker compose build
```

### Start Infrastructure

```bash
docker compose up
```

### Scale Workers

```bash
docker compose up --scale consumer_worker=3
```

### Stop Infrastructure

```bash
docker compose down
```

---

## API Endpoint

### Queue Notification

```http
POST /api/v1/notifications/trigger
```

Request:

```json
{
  "type": "email",
  "payload": {
    "recipient": "john@example.com"
  }
}
```

Response:

```json
{
  "success": true,
  "jobId": "1717350000000-0"
}
```

---

## Failure Simulation

Submitting a notification with:

```json
{
  "recipient": "fail@example.com"
}
```

simulates an upstream provider failure and triggers retry handling.

After maximum retry attempts are reached, the event is routed into the Dead Letter Queue.

---

## Backend Engineering Concepts Demonstrated

* Event Driven Architecture
* Redis Streams
* Consumer Groups
* Pending Entries List (PEL)
* Producer Consumer Pattern
* Horizontal Scaling
* Fault Tolerance
* Retry Strategies
* Dead Letter Queues
* Distributed Worker Architecture
* Containerized Infrastructure
* At-Least-Once Processing
* High Throughput System Design

---

## Future Improvements

* XAUTOCLAIM-based stalled job recovery
* Redis Sorted Sets for delayed jobs
* Exponential Backoff Retries
* BullMQ Integration
* Distributed Locks
* WebSocket Event Broadcasting
* Prometheus Metrics
* Grafana Dashboards
* Kubernetes Deployment
* Multi-Region Event Processing

---

## Learning Outcomes

This project demonstrates how distributed backend systems process large volumes of asynchronous events safely, reliably, and efficiently using modern event-driven architectural patterns commonly found in large-scale production systems.
