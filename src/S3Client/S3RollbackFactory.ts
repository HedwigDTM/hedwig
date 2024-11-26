import { InMemoryStrategy } from './S3Strategies/InMemoryStrategy';
import { DuplicateStrategy } from './S3Strategies/DuplicateStrategy';
import { S3Client as AWSClient } from '@aws-sdk/client-s3';
import { S3RollBackStrategy } from './S3RollbackStrategy';
import { S3RollbackStrategyType } from '../Types/S3/S3RollBackStrategy';

/**
 * Custom error class for backup operations.
 */
export class S3BackupError extends Error {
  constructor(message = '') {
    super(message);
    this.name = 'BackupError';
  }
}

/**
 * Custom error class for restore operations.
 */
export class S3RestoreError extends Error {
  constructor(message = '') {
    super(message);
    this.name = 'RestoreError';
  }
}

export const S3RollbackFactory = (
  connection: AWSClient,
  strategy: S3RollbackStrategyType,
  backupBucketName?: string
): S3RollBackStrategy => {
  switch (strategy) {
    case S3RollbackStrategyType.IN_MEMORY: {
      return new InMemoryStrategy(connection);
    }
    case S3RollbackStrategyType.DUPLICATE_FILE: {
      return new DuplicateStrategy(
        connection,
        backupBucketName ? backupBucketName : 'hedwig-backups'
      );
    }
    default:
      throw new Error('Rollback strategy type was not found!');
  }
};
