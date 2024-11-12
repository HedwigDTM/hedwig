import { RedisClientType } from 'redis';

export abstract class RedisRollBackStrategy {
  protected connection: RedisClientType;

  constructor(_connection: RedisClientType) {
    this.connection = _connection;
  }

  public abstract backupItem(key: string): Promise<void>;
  public abstract restoreItem(key: string): Promise<void>;
  public abstract closeTransaction(): Promise<void>;
}
