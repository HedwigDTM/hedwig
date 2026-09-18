import { RedisConnection } from '../RedisConnection';
import { RedisRollBackStrategy } from '../RedisRollbackStrategy';
import { RedisBackupRecord } from '../../types/redis';
import { RollbackError } from '../../errors';

/**
 * Class to handle duplicate delete, backup and restore of Redis objects.
 */
export class DuplicateStrategy extends RedisRollBackStrategy {
  private backupHashName: string;

  constructor(
    _connection: RedisConnection,
    transactionID: string,
    backupHashName: string
  ) {
    super(_connection);
    this.backupHashName = `${backupHashName}:${transactionID}`;
  }

  /**
   * Backs up a Redis object as an explicit existence record.
   *
   * @param key - The key of the object to backup.
   */
  public async backupItem(key: string): Promise<RedisBackupRecord> {
    const value = await this.connection.get(key);
    const record: RedisBackupRecord =
      value === null
        ? { existed: false, value: null }
        : { existed: true, value };
    // JSON encoding round-trips absence and empty strings through the hash
    await this.connection.hSet(
      this.backupHashName,
      key,
      JSON.stringify(record)
    );
    return record;
  }

  /**
   * Restores a Redis object.
   *
   * @param key - The key of the object to restore.
   */
  public async restoreItem(key: string): Promise<void> {
    const raw = await this.connection.hGet(this.backupHashName, key);
    if (raw === null || raw === undefined) {
      throw new RollbackError(`No backup recorded for key ${key}.`);
    }
    // Records are written by backupItem as JSON
    const record = JSON.parse(raw) as RedisBackupRecord;
    if (record.existed) {
      await this.connection.set(key, record.value);
    } else {
      await this.connection.del(key);
    }
  }

  public async closeTransaction(): Promise<void> {
    await this.connection.del(this.backupHashName);
  }
}
