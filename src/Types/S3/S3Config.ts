import { S3ClientConfig } from '@aws-sdk/client-s3';
import { S3RollbackStrategyType } from './S3RollBackStrategy';

export type S3Config = S3ClientConfig & {
  rollbackStrategy?: S3RollbackStrategyType;
};
