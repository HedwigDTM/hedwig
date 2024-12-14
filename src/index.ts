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
        // const object = await S3Client.deleteObject({
        //   Bucket: 'my-local-bucket-1',
        //   Key: 'V1',
        // });
        // await S3Client.putObject({
        //   Bucket: 'my-local-bucket',
        //   Key: 'V1',
        //   Body: Buffer.from('hello world', 'utf-8'),
        // });
        // await S3Client.createBucket({
        //   Bucket: 'my-local-bucket',
        // });
        // await S3Client.putObject({
        //   Bucket: 'my-local-bucket',
        //   Key: 'V1',
        //   Body: Buffer.from('hello world', 'utf-8'),
        // });
        // await S3Client.deleteObject({
        //   Bucket: 'my-local-bucket',
        //   Key: 'V1',
        // });
        await S3Client.deleteObject({
          Bucket: 'my-local-bucket',
          Key: 'V1',
        });
        await S3Client.deleteBucket({
          Bucket: 'my-local-bucket',
        });
      } catch (error) {
        console.error('Error while putting object in S3:', error);
      }
    }

    // console.log(
    //   (await S3Client?.getObject({ Bucket: 'my-local-bucket', Key: 'V1' }))
    //     ?.Metadata
    // );

    if (RedisClient) {
      await RedisClient.set('key1', 'value1');
    }

    // throw new Error('dfkgd');

    // process.exit();
  });
};
main();
