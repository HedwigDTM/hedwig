# Rollback strategies

Each backend picks how rollback data is captured. Choose per backend via the
config:

```ts
import {
  S3RollbackStrategyType,
  RedisRollbackStrategyType,
} from '@hedwig-team/dtm';

new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    rollbackStrategy: S3RollbackStrategyType.DUPLICATE_FILE,
  },
  redisConfig: {
    url: 'redis://localhost:6379',
    rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
  },
});
```

## S3

### `IN_MEMORY`

Objects are downloaded into the process' memory before the transaction
mutates them; restore writes the bytes back.

- Best for: small objects, short transactions, tests.
- Watch out: memory use grows with the size of every backed-up object;
  restore writes only bytes, so object metadata (Content-Type, tags) is not
  restored.

### `DUPLICATE_FILE`

Backups are copied into a dedicated backup bucket (default
`hedwig-backups`, configurable via `s3Config.backupBucketName`) under
per-transaction keys: `<transactionID>/<Bucket>/<Key>`. Two concurrent
transactions never overwrite each other's backups.

- Best for: production workloads and larger objects.
- Watch out: **the first transaction using the strategy creates the backup
  bucket in your account** (needs `s3:CreateBucket`); it is removed again if
  that same transaction created it. Backup/restore doubles the S3 calls for
  every mutating operation. Keys with special characters must be
  URL-representable (CopySource is built unencoded).

## Redis

### `IN_MEMORY`

Pre-transaction values are kept in a per-client map. Best for tests and
single-shot scripts.

### `DUPLICATE_FILE`

Snapshots are stored as JSON records in a per-transaction Redis hash:
`<backupHashName>:<transactionID>` (default prefix `Hedwig-Backups`). The
hash is deleted when the transaction cleans up, so nothing accumulates.
Records explicitly capture whether the key existed, so rollback deletes keys
that did not exist before the transaction and restores exact values
(including empty strings) for keys that did.

- Best for: production.
- Watch out: backup requires one extra round trip per mutating operation on
  the same connection.

## What every strategy guarantees

- Backups are namespaced per transaction — no cross-transaction clobbering.
- Cleanup removes only the artifacts the transaction created.
- A rollback failure does not abort the remaining rollback actions; the
  aggregated failures are thrown as a `RollbackError` (see
  [errors](./errors.md)).
