import { RedisBackupRecord } from '../types/redis';
import { RedisConnection } from './RedisConnection';

export abstract class RedisRollBackStrategy {
  protected connection: RedisConnection;

  constructor(_connection: RedisConnection) {
    this.connection = _connection;
  }

  public abstract backupItem(key: string): Promise<RedisBackupRecord>;
  public abstract restoreItem(key: string): Promise<void>;
  public abstract closeTransaction(): Promise<void>;
}
