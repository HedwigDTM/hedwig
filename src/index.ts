import TransactionManager from './TransactionManager/TransactionManager';
import { S3RollbackClient } from './S3Client/S3Client';
import { RedisRollbackClient } from './RedisClient/RedisClient';
import RollbackableClient from './RollbackableClient/RollbackableClient';
import { S3RollbackStrategyType } from './types/s3';
import { RedisRollbackStrategyType } from './types/redis';
import {
  HedwigError,
  RollbackError,
  S3BackupError,
  S3RestoreError,
} from './errors';

export {
  TransactionManager,
  S3RollbackClient,
  RedisRollbackClient,
  RollbackableClient,
  S3RollbackStrategyType,
  RedisRollbackStrategyType,
  HedwigError,
  RollbackError,
  S3BackupError,
  S3RestoreError,
};

export type { S3Config, S3ObjectParams, S3BucketParams } from './types/s3';
export type { RedisBackupRecord } from './types/redis';
export type { RedisConnection } from './RedisClient/RedisConnection';
export type {
  RollbackableClients,
  RollbackableClientsFor,
  TransactionCallbackFunction,
  TransactionManagerConfig,
} from './types/transaction-manager';
