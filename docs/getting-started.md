# Getting started

Hedwig wraps AWS S3 and Redis operations in compensating transactions: every
mutating operation records a rollback action, and if your transaction
callback throws, all recorded actions are rolled back in reverse order.

## Install

```bash
npm install @hedwig-team/dtm
```

Peer dependencies (`@aws-sdk/client-s3`, `redis`) are expected in your project.

## Run a transaction

```ts
import {
  TransactionManager,
  S3RollbackStrategyType,
  RedisRollbackStrategyType,
} from '@hedwig-team/dtm';

const manager = new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    rollbackStrategy: S3RollbackStrategyType.DUPLICATE_FILE,
  },
  redisConfig: {
    url: 'redis://localhost:6379',
    rollbackStrategy: RedisRollbackStrategyType.DUPLICATE_FILE,
  },
});

const result = await manager.transaction(async ({ S3Client, RedisClient }) => {
  // S3Client and RedisClient are non-optional here: both configs were provided
  await S3Client.putObject({
    Bucket: 'my-bucket',
    Key: 'report.json',
    Body: buffer,
  });
  await RedisClient.set('report:latest', 'report.json');

  return 'done'; // the callback's return value is what transaction() resolves to
});
```

If the callback throws, every S3 and Redis action taken inside is rolled
back (the S3 object is deleted/restored, the Redis key is deleted/restored)
and the original error propagates. Cleanup failures never mask the original
error — they are attached to it as `error.cleanupFailures`.

## Only one backend

Configure only what you need; the other client stays absent from the
callback (and the type system knows it):

```ts
const redisOnlyManager = new TransactionManager({
  redisConfig: {
    url: 'redis://localhost:6379',
    rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
  },
});

await redisOnlyManager.transaction(async ({ RedisClient }) => {
  await RedisClient.incr('visits'); // RedisClient is non-optional; S3Client is not present
});
```

## Bring your own connections

By default the manager creates (and cleanly closes) its own Redis
connection per transaction. Pass `connection` in `redisConfig` to use
yours — hedwig never disconnects a connection it did not create:

```ts
const manager = new TransactionManager({
  redisConfig: {
    url: 'redis://localhost:6379',
    connection: myRedisClient, // owned by you
    rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
  },
});
```

## Next steps

- [Strategies](./strategies.md) — choosing and configuring rollback strategies
- [Errors](./errors.md) — what throws, what rolls back, what gets attached
- [Limitations](./limitations.md) — what the transaction does and does not guarantee
- [API reference](./api.md) — every public operation
