# kafka-s3-backup: Architecture (simplified, agent-executable)

> **Agent protocol (token saver):** read §0-§3 once, then only the current phase in §5. After each phase: run its acceptance check, commit, add one line to `PROGRESS.md`, stop.

## 0. Goal & invariants

Hourly backup of Kafka topics to S3, **zero event loss**, minimal compute.

| # | Invariant |
|---|-----------|
| I1 | Offsets are committed to Kafka **only after** both the data file and its manifest are in S3. |
| I2 | Kafka never writes to S3 directly: Kafka → small batches → local file → S3. |
| I3 | A sealed file is never written to; batches always append to a fresh active file. |
| I4 | S3 keys are deterministic, so re-uploading overwrites (idempotent). |

Delivery is at-least-once: a crash may cause a replay, never a loss.

## 1. Stack

Java 17, Spring Boot 3.5, Spring Kafka 3.5, Spring Cloud Vault (AppRole auth), AWS SDK v2 (`s3`, sync), Micrometer/Prometheus, Maven. No database: state is files in `work-dir`.

## 2. Flow

```
Kafka ──poll(≤N rec / ≤B bytes)──▶ append to active.log (flush+fsync)
                                        │  hourly tick (lock)
                                        ▼
                          sealed/<topic>_<window>.log  + .manifest.json
                                        │  Uploader
                                        ▼
        S3 put data ─▶ S3 put manifest ─▶ commitSync(max offset+1) ─▶ delete local
```

No compression. No `.done` marker. No separate HEAD verify: the S3 `PutObject` is sent with a SHA-256 checksum, and S3 rejects the upload if it does not match.

### Time windows
Tick at `HH:00:05` (cron `5 0 * * * *`, UTC). Tick `T` seals the window `[T-1h, T)`; the date and hour in S3 keys come from `T-1h`.
- run 00:00 on day D → `D-1`, hour 23
- run 01:00 → `D`, hour 00

Windows are based on consume time, not event time.

### S3 layout
```
<application_name>/<yyyy-MM-dd>/<topic>-<UUID>.log           # flat data
<application_name>/<yyyy-MM-dd>/<topic>-<UUID>.manifest.json # manifest
```
Each upload uses a unique UUID in the filename; manifests share the same base name as data files with `.manifest.json` suffix.

### Data file (flat)
Raw message value, one message per line, in offset order. Nothing else is written (no key, headers or timestamp).

### Manifest
```json
{
  "topic": "orders",
  "window": "2026-09-28T23",
  "file": "my-app/2026-09-28/orders-a1b2c3d4.log",
  "records": 5,
  "offsets": { "0": [100,101,102], "1": [40,41] }
}
```
`offsets` lists every offset present in the file, per partition, in the order the lines appear (line *i* of a partition's messages ↔ its *i*-th offset). Manifest is uploaded **after** the data file, so its presence proves the data is there.

## 3. Components

| Component | Job | Thread |
|---|---|---|
| `ConsumerLoop` | poll, hand batch to writer; no auto-commit; pause if disk is full | 1 dedicated |
| `BatchWriter` | append batch to active file under lock, flush+fsync, record offsets in memory | same thread |
| `FileRotator` | on tick: lock → close active → rename to `sealed/` → write manifest → open fresh active → unlock | scheduler |
| `Uploader` | upload data, then manifest, then commit, then delete | 1 worker |
| `RecoveryService` | on startup, before consuming | main |

### Locking (I3)
One `ReentrantLock` guards the active file handle.
- append: `lock → write → fsync → unlock`
- rotate: `lock → close → rename → open new → unlock` (milliseconds)
- Uploader reads only `sealed/`; it never takes the lock and never touches the active file.

```
work-dir/
  active/<topic>.log
  sealed/<topic>_<yyyyMMddHH>.log
  sealed/<topic>_<yyyyMMddHH>.manifest.json
```
A file still in `sealed/` = not yet fully uploaded and committed.

### Offset commit (I1)
Consumer keeps polling while commits lag until the upload finishes. After data + manifest are in S3: `commitSync` the max offset + 1 per partition, then delete the local pair. Kafka retention must be ≥ 24h. Use a single instance with a static `group.instance.id`.

### Failure handling
| Failure | Behavior |
|---|---|
| S3 upload fails | retry with backoff, forever; file stays in `sealed/`; no commit; consumer keeps writing to the new active file |
| Crash with data only in `active/` | `active/` is discarded on restart; consumer resumes from last commit and replays (nothing lost) |
| Crash with files in `sealed/` | Recovery re-uploads them (idempotent keys), commits, deletes, then starts the consumer |
| Disk > 80 % | `pause()` consumer; resume < 70 % |
| Offset gap in a batch (`next != prev+1`) | pause, alert, no commit (disable for compacted topics) |

## 4. Metrics (Micrometer → Prometheus, tag `topic`)

- Kafka: `kb_records_consumed_total`, `kb_consumer_lag`, `kb_oldest_uncommitted_age_seconds`, `kb_offset_gap_total`, `kb_commit_failure_total`
- Disk: `kb_active_file_bytes`, `kb_sealed_files_pending`, `kb_disk_used_ratio`
- S3: `kb_upload_duration`, `kb_upload_bytes_total`, `kb_upload_success_total`, `kb_upload_failure_total`, `kb_last_success_epoch`
- Alerts: upload failing > 15 min, oldest uncommitted > 90 min, any offset gap, disk > 80 %

## 5. Phases

Package root `com.example.kafkabackup`.

**P0 Scaffold**: Boot 4 Maven project, deps, `application.yml` (§6), `PROGRESS.md`. Accept: `mvn -q verify` passes.

**P1 Config & keys**: `BackupProperties`, `WindowCalculator`, `S3KeyBuilder`, `Manifest` model. Accept: unit tests: 00:00 D → D-1 hour 23; 01:00 → D hour 00; correct `ingestion/` and `logs/` keys.

**P2 Consume + write**: `ConsumerLoop`, `BatchWriter` (append, fsync, offsets in memory, gap check). Accept: Testcontainers Kafka, produce 10 000 → active file has 10 000 lines; no commit.

**P3 Rotate + lock**: `FileRotator`, lock, manifest writer. Accept: concurrent test with producer running during 50 rotations: total lines across sealed + active == produced; manifest offsets match file lines.

**P4 Upload + commit**: `Uploader` (data → manifest → commit → delete, backoff). Accept: LocalStack test: both objects at expected keys, committed offset == max+1; injected S3 failure → no commit, then success.

**P5 Recovery + metrics + E2E**: `RecoveryService`, metrics, Dockerfile. Accept: kill -9 mid-batch, post-seal and post-upload-pre-commit → every produced offset present in S3 manifests at least once.

## 6. Config

```yaml
spring:
  config:
    import: vault://
  cloud:
    vault:
      authentication: APPROLE
      app-role:
        role-id: ${VAULT_ROLE_ID}
        secret-id: ${VAULT_SECRET_ID}
      uri: ${VAULT_URI:http://localhost:8200}
      kv:
        version: 2
        backend: secret
      application-name: kafka-s3-backup
  kafka:
    bootstrap-servers: ${spring.kafka.bootstrap-servers}
    consumer:
      group-id: kafka-s3-backup
      enable-auto-commit: false
      auto-offset-reset: earliest
      properties: { group.instance.id: ${HOSTNAME:local} }

backup:
  topics: [orders, payments]
  zone: UTC
  rotate-cron: "5 0 * * * *"
  work-dir: /data/kafka-backup
  application-name: my-app
  batch: { max-records: 500, max-bytes: 4MB, poll-timeout: 1s }
  disk: { pause-above: 0.80, resume-below: 0.70 }
  gap-check: true
  s3: { bucket: my-backup, region: eu-west-3 }
```

**Vault KV structure** (v2, path `secret/data/kafka-s3-backup/`):
```json
{
  "spring": {
    "kafka": {
      "bootstrap-servers": "kafka:9092"
    }
  },
  "aws": {
    "access-key-id": "AKIA...",
    "secret-access-key": "..."
  }
}
```
