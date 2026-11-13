import TransactionManager from './TransactionManager/TransactionManager';
import { S3RollbackClient } from './S3Client/S3Client';
import { RedisRollbackClient } from './RedisClient/RedisClient';
import { CustomActionRegistry } from './CustomAction/CustomActionRegistry';
import RollbackableClient from './RollbackableClient/RollbackableClient';
import { S3RollbackStrategyType } from './types/s3';
import { RedisRollbackStrategyType } from './types/redis';
import {
  HedwigError,
  RollbackError,
  S3BackupError,
  S3RestoreError,
} from './errors';
import { InMemoryStateStore } from './state-store/InMemoryStateStore';
import { RedisStateStore } from './state-store/RedisStateStore';
import {
  recoverInFlightTransactions,
  RecoveryOptions,
  RecoverySummary,
} from './recovery/recovery';

export {
  TransactionManager,
  S3RollbackClient,
  RedisRollbackClient,
  CustomActionRegistry,
  RollbackableClient,
  S3RollbackStrategyType,
  RedisRollbackStrategyType,
  HedwigError,
  RollbackError,
  S3BackupError,
  S3RestoreError,
  InMemoryStateStore,
  RedisStateStore,
  recoverInFlightTransactions,
};

export type { S3Config, S3ObjectParams, S3BucketParams } from './types/s3';
export type { RedisConfig, RedisBackupRecord } from './types/redis';
export type { RedisConnection } from './RedisClient/RedisConnection';
export type {
  RollbackableClients,
  RollbackableClientsFor,
  TransactionCallbackFunction,
  TransactionManagerConfig,
} from './types/transaction-manager';
export type {
  ICustomAction,
  CustomActionsApi,
} from './CustomAction/ICustomAction';
export type {
  TransactionStateStore,
  TransactionRecord,
  RecordedAction,
  TransactionStatus,
  ClientKind,
} from './types/transaction-state';
export type { RecoveryOptions, RecoverySummary } from './recovery/recovery';
