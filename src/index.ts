import TransactionManager from './TransactionManager/TransactionManager';
import { RedisRollbackStrategyType } from './Types/Redis/RedisRollbackStrategy';
import { S3RollbackStrategyType } from './Types/S3/S3RollBackStrategy';

const manager = new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    endpoint: 'http://localhost:4566',
    rollbackStrategy: S3RollbackStrategyType.DUPLICATE_FILE,
    forcePathStyle: true,
    credentials: {
      accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
      secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    },
  },
  redisConfig: {
    url: 'redis://localhost:6379',
    rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
  },
});

const main = async () => {
  await manager.transaction(async ({ S3Client, RedisClient }) => {
    if (S3Client) {
      try {
        await S3Client.createBucket({
          Bucket: 'my-new-bucket',
        });
        await S3Client.putObject({
          Bucket: 'my-new-bucket',
          Key: 'V1',
          Body: Buffer.from('value1', 'utf-8'),
        });
      } catch (error) {
        console.error('Error while putting object in S3:', error);
      }
    }

    console.log(
      (await S3Client?.getObject({ Bucket: 'my-new-bucket', Key: 'V1' }))
        ?.Metadata
    );

    if (RedisClient) {
      await RedisClient.set('mykey1', 'value1');
    }

    // throw new Error('dfkgd');
  });
};
main();
