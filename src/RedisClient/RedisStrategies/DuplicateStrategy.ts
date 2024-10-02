import { RedisClientType } from 'redis';
import { RedisRollBackStrategy } from '../RedisRollbackStrategy';
import RollbackError from '../../RollbackableClient/Errors/RollbackError';

/**
 * Class to handle duplicate delete, backup and restore of Redis objects.
 */
export class DuplicateStrategy extends RedisRollBackStrategy {
  private backupHashName: string;

  constructor(_connection: RedisClientType, backupHashName: string) {
    super(_connection);
    this.backupHashName = backupHashName;
  }

  /**
   * Backs up a Redis object.
   *
   * @param key - The key of the object to backup.
   */
  public async backupItem(key: string): Promise<void> {
    const value = await this.connection.get(key);
    if (value) {
      await this.connection.hSet(this.backupHashName, key, value);
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
    const value = await this.connection.hGet(this.backupHashName, key);
    if (value) {
      await this.connection.set(key, value);
    } else {
      throw new RollbackError(`Key ${key} does not exist in backup.`);
    }
  }
}
