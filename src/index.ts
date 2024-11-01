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
