<p align="center">
  <a href="https://github.com/HedwigDTM/hedwig">
    <img src="https://raw.githubusercontent.com/HedwigDTM/hedwig/main/logo.png" alt="Hedwig" width="520">
  </a>
</p>

<p align="center">
  <a href="https://github.com/HedwigDTM/hedwig/actions/workflows/ci.yml"><img src="https://github.com/HedwigDTM/hedwig/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://www.npmjs.com/package/@hedwig-team/dtm"><img src="https://img.shields.io/npm/v/@hedwig-team/dtm" alt="npm version"></a>
  <a href="https://github.com/HedwigDTM/hedwig/blob/main/LICENSE.md"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
</p>

<p align="center">
  <a href="https://github.com/HedwigDTM/hedwig"><b>GitHub</b></a> ·
  <a href="https://github.com/HedwigDTM/hedwig/tree/main/docs">Docs</a> ·
  <a href="https://github.com/HedwigDTM/hedwig/blob/main/CHANGELOG.md">Changelog</a> ·
  <a href="https://github.com/HedwigDTM/hedwig/issues">Issues</a>
</p>

---

**Hedwig** is a distributed transaction manager, designed to simplify complex workflows involving interactions with multiple third-party resources. It provides a reliable, simplified approach to orchestrating and managing transactions across distributed systems, ensuring efficient and consistent operations.

## Install

```bash
npm install @hedwig-team/dtm @aws-sdk/client-s3 redis
```

`@aws-sdk/client-s3` and `redis` are peer dependencies: install the ones for
the backends you use.

## Usage

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

- [Getting started](https://github.com/HedwigDTM/hedwig/blob/main/docs/getting-started.md)
- [Rollback strategies](https://github.com/HedwigDTM/hedwig/blob/main/docs/strategies.md)
- [Error handling](https://github.com/HedwigDTM/hedwig/blob/main/docs/errors.md)
- [Custom actions](https://github.com/HedwigDTM/hedwig/blob/main/docs/custom-actions.md)
- [Transaction state & recovery](https://github.com/HedwigDTM/hedwig/blob/main/docs/transaction-state.md) (beta)
- [Limitations](https://github.com/HedwigDTM/hedwig/blob/main/docs/limitations.md)
- [API reference](https://github.com/HedwigDTM/hedwig/blob/main/docs/api.md)

## License

This project is licensed under the MIT License. See the [LICENSE](https://github.com/HedwigDTM/hedwig/blob/main/LICENSE.md) file for details.

## Contact

For any questions or support, please reach out one of the contributors.
