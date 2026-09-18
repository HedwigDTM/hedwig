import { RedisConnection } from './RedisConnection';

/**
 * Snapshot of a key's state before a transaction touched it.
 * Uses a discriminated union so absence is explicit (never truthiness).
 */
export type RedisBackupRecord =
  | { existed: true; value: string }
  | { existed: false; value: null };
export abstract class RedisRollBackStrategy {
  protected connection: RedisConnection;

  constructor(_connection: RedisConnection) {
    this.connection = _connection;
  }

  public abstract backupItem(key: string): Promise<RedisBackupRecord>;
  public abstract restoreItem(key: string): Promise<void>;
  public abstract closeTransaction(): Promise<void>;
}
