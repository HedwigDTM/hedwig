import { RedisClientOptions } from 'redis';
import { RedisRollbackStrategyType } from './RedisRollbackStrategy';

export type RedisConfig = RedisClientOptions & {
  rollbackStrategy?: RedisRollbackStrategyType;
};
