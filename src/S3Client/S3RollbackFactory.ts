import { InMemoryStrategy } from './S3Strategies/InMemoryStrategy';
import { DuplicateStrategy } from './S3Strategies/DuplicateStrategy';
import { S3Client as AWSClient } from '@aws-sdk/client-s3';
import { S3RollBackStrategy } from './S3RollbackStrategy';
import { S3RollbackStrategyType } from '../types/s3';

const DEFAULT_BACKUP_BUCKET_NAME = 'hedwig-backups';

export const S3RollbackFactory = (
  connection: AWSClient,
  strategy: S3RollbackStrategyType,
  transactionID: string,
  backupBucketName?: string
): S3RollBackStrategy => {
  switch (strategy) {
    case S3RollbackStrategyType.IN_MEMORY: {
      return new InMemoryStrategy(connection);
    }
    case S3RollbackStrategyType.DUPLICATE_FILE: {
      return new DuplicateStrategy(
        connection,
        transactionID,
        backupBucketName ? backupBucketName : DEFAULT_BACKUP_BUCKET_NAME
      );
    }
    default:
      throw new Error('Rollback strategy type was not found!');
  }
};
