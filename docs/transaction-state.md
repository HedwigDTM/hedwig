# Transaction state & recovery

By default, hedwig's rollback bookkeeping lives in memory: if the process
dies mid-transaction, orphaned half-applied writes cannot be recovered.
Configuring a **transaction state store** fixes that.

## How it works

1. With a `stateStore` configured, the manager persists a transaction
   record (`in-flight`, started-at) before any write happens.
2. Every mutating action is recorded in the store.
3. On completion the store marks the transaction `committed` (success) or
   `rolled-back` (clean rollback).
4. If the process crashes mid-transaction, the record stays `in-flight` —
   and a recovery pass replays the durable backups that the
   `DUPLICATE_FILE` strategies wrote (S3 backup keys under
   `<transactionID>/`, Redis backup hashes) and marks the transaction
   `rolled-back`.

```ts
import {
  TransactionManager,
  RedisStateStore,
  S3RollbackStrategyType,
  recoverInFlightTransactions,
} from '@hedwig-team/dtm';
import { createClient } from 'redis';

const redisConnection = await createClient({
  url: 'redis://localhost:6379',
}).connect();

const manager = new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    rollbackStrategy: S3RollbackStrategyType.DUPLICATE_FILE,
  },
  stateStore: new RedisStateStore(redisConnection),
});
```

## Recovery

Run a recovery pass on startup (or via a scheduled job) to roll back
transactions orphaned by a crash:

```ts
const summary = await recoverInFlightTransactions({
  store: new RedisStateStore(redisConnection),
  olderThan: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
  s3: { connection: s3Client },
  redis: { connection: redisConnection },
});

// summary.recovered: transaction ids rolled back
// summary.failed: transactions left in-flight for a later retry
```

Recovery drives rollback from the durable backup locations, not from
process memory:

- **S3**: every backup object under `<transactionID>/` in the backup bucket
  is copied back to its source and removed.
- **Redis**: every entry in `<backupHashName>:<transactionID>` is applied —
  keys that existed before the transaction are restored to their exact
  value, keys that did not exist are deleted — and the backup hash is
  removed.

## Guarantees

- **Committed transactions are never re-rolled-back** by recovery.
- Transactions whose rollback had cleanup failures stay `in-flight`
  deliberately, so a later recovery pass can retry the failed steps.
- Failed recoveries are reported and left in flight — safe to retry.
- `IN_MEMORY` strategies keep backups in process memory and are therefore
  **not** crash-recoverable; use `DUPLICATE_FILE` for durability.

## Implementations

- `InMemoryStateStore` — process-local, for tests and single-instance use.
- `RedisStateStore` — survives crashes; requires a Redis connection.

The `TransactionStateStore` interface is deliberately small (`start`,
`recordAction`, `markCommitted`, `markRolledBack`, `listInFlight`) so other
backends (SQL, DynamoDB) can plug in.
