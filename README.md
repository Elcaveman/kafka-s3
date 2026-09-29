# kafka-s3-backup

A reliable data pipeline that continuously backs up Apache Kafka topics to Amazon S3. It consumes messages from one or more topics, batches them by size or time window, and writes them to a single S3 Object (topic/date/<uuid>-datalake.txt). Offsets are committed only after a successful upload, giving at-least-once delivery guarantees.

## Features

- Multi-topic and regex topic subscription
- Configurable batching (by size, message count, or time interval)
- Output formats: text with the same encoding as the received kafka records
- Time- and offset-based S3 key partitioning for easy querying (Athena, Glue)
- Offset tracking and consumer group management
- Restore/replay tooling to republish archived data back into Kafka
- S3 lifecycle-friendly layout for cost-efficient long-term retention
- Metrics and health checks (Prometheus-compatible)
- Configurable via environment variables or YAML

## Use cases

- Disaster recovery and long-term retention of event streams
- Compliance and audit archiving
- Feeding a data lake for analytics
- Reprocessing historical events

## Architecture

check Architecture.md