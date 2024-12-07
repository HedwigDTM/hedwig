import { S3Client as AWSClient } from '@aws-sdk/client-s3';
import { S3BucketParams, S3ObjectParams } from './S3Client';

export abstract class S3RollBackStrategy {
  protected connection: AWSClient;

  constructor(_connection: AWSClient) {
    this.connection = _connection;
  }

  public abstract backupFile(params: S3ObjectParams): Promise<void>;
  public abstract restoreFile(params: S3ObjectParams): Promise<void>;
  public abstract backupBucket(params: S3BucketParams): Promise<string>;
  public abstract restoreBucket(params: S3BucketParams): Promise<void>;
  public abstract closeTransaction(): Promise<void>;
  public abstract purgeBucket(bucketName: string): Promise<void>;
}
