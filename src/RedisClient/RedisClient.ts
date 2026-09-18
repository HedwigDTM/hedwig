import RollbackableClient from '../RollbackableClient/RollbackableClient';
import { RedisConnection } from './RedisConnection';
import { RedisRollBackStrategy } from './RedisRollbackStrategy';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';
import { RedisRollbackFactory } from './RedisRollbackFactory';

export class RedisRollbackClient extends RollbackableClient {
  public closeTransaction(): Promise<void> {
    return this.rollbackStrategy.closeTransaction();
  }
  private connection: RedisConnection;
  private rollbackStrategy: RedisRollBackStrategy;

  constructor(
    transactionID: string,
    connection: RedisConnection,
    rollbackStrategyType: RedisRollbackStrategyType,
    backupHashName?: string
  ) {
    super(transactionID);
    this.connection = connection;
    this.rollbackStrategy = RedisRollbackFactory(
      this.connection,
      rollbackStrategyType,
      transactionID,
      backupHashName
    );
  }

  /**
   * GETs a value from the Redis database.
   *
   * @param key - The key to retrieve the value from.
   * @returns The value associated with the key, or null if the key does not exist.
   */
  public async get(key: string): Promise<string | null> {
    return this.connection.get(key);
  }

  /**
   * SETs a value in the Redis database.
   *
   * @param key - The key to set the value for.
   * @param value - The value to set.
   */
  public async set(key: string, value: string): Promise<string | null> {
    // Snapshot the current state first (awaited: a failed backup must fail
    // the operation instead of becoming an unhandled rejection later)
    await this.rollbackStrategy.backupItem(key);

    const rollbackAction = async () => {
      await this.rollbackStrategy.restoreItem(key);
    };

    this.rollbackActions.push(rollbackAction);
    return await this.connection.set(key, value);
  }

  /**
   * Deletes a key from the Redis database.
   *
   * @param key - The key to delete.
   */
  public async del(key: string): Promise<number> {
    // A missing key records { existed: false } so rollback deletes it
    // instead of throwing mid-rollback
    await this.rollbackStrategy.backupItem(key);
    const rollbackAction = async () => {
      await this.rollbackStrategy.restoreItem(key);
    };
    this.rollbackActions.push(rollbackAction);

    return await this.connection.del(key);
  }

  /**
   * Increments a key in the Redis database.
   *
   * Rollback restores the exact pre-transaction value via a snapshot (keys
   * that did not exist are deleted), so concurrent writers' increments are
   * not undone. Note: the GET snapshot and INCR are two round-trips, not an
   * atomic transaction with the increment itself.
   *
   * @param key - The key to increment.
   * @returns The new value of the key after incrementing.
   */
  public async incr(key: string): Promise<number> {
    await this.rollbackStrategy.backupItem(key);

    const rollbackAction = async () => {
      await this.rollbackStrategy.restoreItem(key);
    };
    this.rollbackActions.push(rollbackAction);

    return await this.connection.incr(key);
  }

  /**
   * Decrements a key in the Redis database.
   *
   * Rollback restores the exact pre-transaction value via a snapshot (see
   * incr for semantics and caveats).
   *
   * @param key - The key to decrement.
   * @returns The new value of the key after decrementing.
   */
  public async decr(key: string): Promise<number> {
    await this.rollbackStrategy.backupItem(key);

    const rollbackAction = async () => {
      await this.rollbackStrategy.restoreItem(key);
    };
    this.rollbackActions.push(rollbackAction);

    return await this.connection.decr(key);
  }
}
