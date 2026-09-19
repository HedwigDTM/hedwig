[![CI](https://github.com/HedwigDTM/hedwig/actions/workflows/ci.yml/badge.svg)](https://github.com/HedwigDTM/hedwig/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE.md)

---

**Hedwig** is a distributed transaction manager, designed to simplify complex workflows involving interactions with multiple third-party resources. It provides a reliable, simplified approach to orchestrating and managing transactions across distributed systems, ensuring efficient and consistent operations.

```typescript
import {
  TransactionManager,
  S3RollbackStrategyType,
  RedisRollbackStrategyType,
} from '@hedwig-team/dtm';

const manager = new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    endpoint: 'http://localhost:4566',
    rollbackStrategy: S3RollbackStrategyType.DUPLICATE_FILE,
    forcePathStyle: true,
  },
  redisConfig: {
    url: 'redis://localhost:6379',
    rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
  },
});

(async () => {
  const result = await manager.transaction(
    async ({ S3Client, RedisClient }) => {
      // S3Client and RedisClient are non-optional: both configs were provided
      await S3Client.putObject({
        Bucket: 'my-local-bucket',
        Key: 'V1',
        Body: Buffer.from('value1', 'utf-8'),
      });
      await RedisClient.set('key1', 'value1');
      return 'done';
    }
  );
})();
```

If the callback throws, every action inside is rolled back and the original
error propagates — cleanup failures are attached as `error.cleanupFailures`
and never mask it.

## Documentation

- [Getting started](./docs/getting-started.md)
- [Rollback strategies](./docs/strategies.md)
- [Error handling](./docs/errors.md)
- [Limitations](./docs/limitations.md)
- [API reference](./docs/api.md)

## License

This project is licensed under the MIT License. See the [LICENSE](./LICENSE.md) file for details.

## Contact

For any questions or support, please reach out one of the contributors.
