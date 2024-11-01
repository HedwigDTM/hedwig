![Hedwig Header](./logo.png)

---

**Hedwig** is a distributed transaction manager, designed to simplify complex workflows involving interactions with multiple third-party resources. It provides a reliable, simplified approach to orchestrating and managing transactions across distributed systems, ensuring efficient and consistent operations.

```typescript
import TransactionManager from './TransactionManager/TransactionManager';
import { RedisRollbackStrategyType } from './Types/Redis/RedisRollbackStrategy';
import { S3RollbackStrategyType } from './Types/S3/S3RollBackStrategy';

const manager = new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    endpoint: 'http://localhost:4566',
    rollbackStrategy: S3RollbackStrategyType.DUPLICATE_FILE,
    forcePathStyle: true,
  },
  redisConfig: {
    url: 'localhost',
    rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
  },
});

(async () => {
  await manager.transaction(async ({ S3Client, RedisClient }) => {
    if (S3Client) {
      await S3Client.putObject({
        Bucket: 'my-local-bucket',
        Key: 'V1',
        Body: Buffer.from('value1', 'utf-8'),
      });
    }

    if (RedisClient) {
      await RedisClient.set('key1', 'value1');
    }
  });
})();

```

## License

This project is licensed under the [MIT License](./LICENSE).

## Contact

For any questions or support, please reach out one of the contributors.