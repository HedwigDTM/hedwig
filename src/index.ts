import TransactionManager from './TransactionManager/TransactionManager';
import { S3RollbackStrategyType } from './Types/S3/S3RollBackStrategy';

const manager = new TransactionManager({
  s3Config: {
    region: 'us-east-1',
    endpoint: 'http://localhost:4566',
    rollbackStrategy: S3RollbackStrategyType.IN_MEMORY,
    forcePathStyle: true,
  },
});

(async () => {
  await manager.transaction(async ({ S3Client }) => {
    if (S3Client) {
      await S3Client.putObject({
        Bucket: 'my-local-bucket',
        Key: 'V1',
        Body: Buffer.from('Noder', 'utf-8'),
      });
      await S3Client.putObject({
        Bucket: 'my-local-bucket',
        Key: 'V2',
        Body: Buffer.from('Neder', 'utf-8'),
      });
    }
  });
})();
