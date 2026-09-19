# API reference

Everything below is importable from the package root:

```ts
import {
  TransactionManager,
  S3RollbackClient,
  RedisRollbackClient,
  RollbackableClient,
  S3RollbackStrategyType,
  RedisRollbackStrategyType,
  HedwigError,
  RollbackError,
  S3BackupError,
  S3RestoreError,
} from '@hedwig-team/dtm';
```

Types (all exported too): `S3Config`, `S3ObjectParams`, `S3BucketParams`,
`RedisConfig`, `RedisBackupRecord`, `RedisConnection`,
`RollbackableClients`, `RollbackableClientsFor`,
`TransactionCallbackFunction`, `TransactionManagerConfig`.

## TransactionManager

Generic over its config: a client is non-optional in the callback exactly
when its config was provided.

```ts
constructor(config: C extends TransactionManagerConfig)
```

### transaction

```ts
transaction<Result>(
  callback: (clients: RollbackableClientsFor<C>) => Promise<Result>
): Promise<Result>
```

```ts
const result = await manager.transaction(async ({ S3Client, RedisClient }) => {
  await RedisClient.set('k', 'v');
  await S3Client.putObject({ Bucket: 'b', Key: 'k', Body: Buffer.from('v') });
  return 'ok'; // typed as Promise<'ok'>
});
```

On callback failure: rollback runs (reverse order, resilient), the original
error propagates with `cleanupFailures` attached. On success with failed
cleanup: rejects with `RollbackError`. See [errors](./errors.md).

## S3RollbackClient

Constructed by the manager; one instance per transaction.

| Method                                     | Notes                                                       |
| ------------------------------------------ | ----------------------------------------------------------- |
| `putObject(params: S3ObjectParams)`        | heads first; only 404 means "new object"                    |
| `deleteObject(params: S3ObjectParams)`     | heads first; missing objects delete with no rollback record |
| `createBucket(params: S3BucketParams)`     | recognizes `BucketAlreadyOwnedByYou`/`BucketAlreadyExists`  |
| `deleteBucket(params: S3BucketParams)`     | empty buckets only; rollback recreates the bucket           |
| `getObject(params: S3ObjectParams)`        | passthrough read, no rollback record                        |
| `listBuckets(params?, continuationToken?)` | paginated listing, no rollback record                       |
| `rollback()`                               | single-shot, resilient, aggregates failures                 |
| `closeTransaction()`                       | removes this transaction's backup artifacts                 |

```ts
await S3Client.putObject({
  Bucket: 'my-bucket',
  Key: 'reports/2026.json',
  Body: buffer,
});
```

`S3ObjectParams` is `{ Bucket: string; Key: string; Body?: Buffer }`;
`S3BucketParams` is `{ Bucket: string; CreateBucketConfiguration?:
CreateBucketConfiguration }`.

## RedisRollbackClient

Constructed by the manager; one instance per transaction.

| Method                     | Rollback behavior                                                     |
| -------------------------- | --------------------------------------------------------------------- |
| `set(key, value)`          | restores exact previous value, or deletes the key if it did not exist |
| `del(key)`                 | restores previous value, or nothing if the key did not exist          |
| `incr(key)` / `decr(key)`  | restores the exact pre-transaction value                              |
| `get(key)` / `exists(key)` | reads only, no rollback record                                        |
| `rollback()`               | single-shot, resilient, aggregates failures                           |
| `closeTransaction()`       | deletes this transaction's backup hash (duplicate strategy)           |

```ts
const count = await RedisClient.incr('visits'); // rollback restores prior value
await RedisClient.del('stale:key'); // rollback brings the old value back
```

## Errors

```ts
import {
  HedwigError,
  RollbackError,
  S3BackupError,
  S3RestoreError,
} from '@hedwig-team/dtm';

try {
  await manager.transaction(async ({ RedisClient }) => {
    await RedisClient.set('k', 'v');
  });
} catch (error) {
  if (error instanceof RollbackError) {
    for (const failure of error.rollbackFailures) {
      console.error('rollback step failed:', failure);
    }
  }
}
```

- `HedwigError` — base of everything hedwig throws
- `RollbackError` — rollback/cleanup aggregation, `rollbackFailures: unknown[]`
- `S3BackupError` / `S3RestoreError` — S3 backup and restore failures

## Transaction state & recovery

```ts
import { RedisStateStore, recoverInFlightTransactions } from '@hedwig-team/dtm';

const manager = new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    rollbackStrategy: S3RollbackStrategyType.DUPLICATE_FILE,
  },
  stateStore: new RedisStateStore(redisConnection),
});

// after a crash:
const summary = await recoverInFlightTransactions({
  store: new RedisStateStore(redisConnection),
  olderThan: '2026-01-01T00:00:00Z',
  s3: { connection: s3 },
  redis: { connection: redisConnection },
});
```

See [transaction state](./transaction-state.md) for the full semantics.
