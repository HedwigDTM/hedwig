import { RedisConnection } from '../RedisConnection';
import { RedisRollBackStrategy } from '../RedisRollbackStrategy';
import { RedisBackupRecord } from '../../types/redis';
import { RollbackError } from '../../errors';

/**
 * Class to handle duplicate delete, backup and restore of Redis objects.
 */
function isRedisBackupRecord(value: unknown): value is RedisBackupRecord {
  if (typeof value !== 'object' || value === null) {
    return false;
  }
  if (!('existed' in value) || !('value' in value)) {
    return false;
  }
  if (typeof value.existed !== 'boolean') {
    return false;
  }
  return value.existed ? typeof value.value === 'string' : value.value === null;
}

export function parseBackupRecord(raw: string, key: string): RedisBackupRecord {
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!isRedisBackupRecord(parsed)) {
      throw new Error('invalid record shape');
    }
    return parsed;
  } catch {
    throw new RollbackError(`Corrupted backup record for key ${key}.`);
  }
}

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
    // Records are written by backupItem as JSON; hGet returns untrusted
    // bytes, so the record is validated instead of blindly cast
    const record = parseBackupRecord(raw, key);
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
