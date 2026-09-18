import { RedisClientOptions, RedisClientType } from 'redis';

export enum RedisRollbackStrategyType {
  IN_MEMORY,
  DUPLICATE_FILE,
}

export type RedisConfig = RedisClientOptions & {
  rollbackStrategy?: RedisRollbackStrategyType;
  backupHashName?: string;
  connection?: RedisClientType;
};

/**
 * Snapshot of a key's state before a transaction touched it.
 * Uses a discriminated union so absence is explicit (never truthiness).
 */
export type RedisBackupRecord =
  | { existed: true; value: string }
  | { existed: false; value: null };
