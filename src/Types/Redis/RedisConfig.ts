import { RedisClientOptions, RedisClientType } from 'redis';
import { RedisRollbackStrategyType } from './RedisRollbackStrategy';

export type RedisConfig = RedisClientOptions & {
  rollbackStrategy?: RedisRollbackStrategyType;
  backupHashName?: string;
  connection?: RedisClientType
};
