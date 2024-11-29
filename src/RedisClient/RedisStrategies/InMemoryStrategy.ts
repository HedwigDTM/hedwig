import { RedisClientType } from 'redis';
import { RedisRollBackStrategy } from '../RedisRollbackStrategy';
import RollbackError from '../../RollbackableClient/Errors/RollbackError';

/**
 * Class to handle in-memory delete, backup and restore of Redis objects.
 */
export class InMemoryStrategy extends RedisRollBackStrategy {
  private backup: Map<string, string> = new Map();

  constructor(_connection: RedisClientType) {
    super(_connection);
    this.backup = new Map<string, string>();
  }

  /**
   * Backs up a Redis object.
   *
   * @param key - The key of the object to backup.
   */
  public async backupItem(key: string): Promise<void> {
    const value = await this.connection.get(key);
    if (value) {
      this.backup.set(key, value);
    } else {
      throw new RollbackError(`Key ${key} does not exist in Redis.`);
    }
  }

  /**
   * Restores a Redis object.
   *
   * @param key - The key of the object to restore.
   */
  public async restoreItem(key: string): Promise<void> {
    const value = this.backup.get(key);
    if (value) {
      await this.connection.set(key, value);
    } else {
      throw new RollbackError(`Key ${key} does not exist in backup.`);
    }
  }

  public async closeTransaction(): Promise<void> {
    this.backup.clear();
  }
}
