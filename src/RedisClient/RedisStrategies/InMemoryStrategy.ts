import { RedisClientType } from 'redis';
import {
  RedisBackupRecord,
  RedisRollBackStrategy,
} from '../RedisRollbackStrategy';
import RollbackError from '../../RollbackableClient/Errors/RollbackError';

/**
 * Class to handle in-memory delete, backup and restore of Redis objects.
 */
export class InMemoryStrategy extends RedisRollBackStrategy {
  private backup: Map<string, RedisBackupRecord> = new Map();

  constructor(_connection: RedisClientType) {
    super(_connection);
    this.backup = new Map<string, RedisBackupRecord>();
  }

  /**
   * Backs up a Redis object.
   *
   * @param key - The key of the object to backup.
   */
  public async backupItem(key: string): Promise<RedisBackupRecord> {
    const value = await this.connection.get(key);
    const record: RedisBackupRecord =
      value === null
        ? { existed: false, value: null }
        : { existed: true, value };
    this.backup.set(key, record);
    return record;
  }

  /**
   * Restores a Redis object.
   *
   * @param key - The key of the object to restore.
   */
  public async restoreItem(key: string): Promise<void> {
    const record = this.backup.get(key);
    if (!record) {
      throw new RollbackError(`No backup recorded for key ${key}.`);
    }
    if (record.existed) {
      await this.connection.set(key, record.value);
    } else {
      await this.connection.del(key);
    }
  }

  public async closeTransaction(): Promise<void> {
    this.backup.clear();
  }
}
