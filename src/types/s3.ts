import { CreateBucketConfiguration, S3ClientConfig } from '@aws-sdk/client-s3';

export enum S3RollbackStrategyType {
  IN_MEMORY,
  DUPLICATE_FILE,
}

export type S3Config = S3ClientConfig & {
  rollbackStrategy?: S3RollbackStrategyType;
  backupBucketName?: string;
};

export interface S3ObjectParams {
  Bucket: string;
  Key: string;
  Body?: Buffer;
}

export interface S3BucketParams {
  Bucket: string;
  CreateBucketConfiguration?: CreateBucketConfiguration;
}
