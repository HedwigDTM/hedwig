import RollbackableClient from '../RollbackableClient/RollbackableClient';
import { RedisClientType } from 'redis';
import { RedisRollBackStrategy } from './RedisRollbackStrategy';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';
import { RedisRollbackFactory } from './RedisRollbackFactory';

export class RedisRollbackClient extends RollbackableClient {
  private connection: RedisClientType;
  private rollbackStrategy: RedisRollBackStrategy;

  constructor(
    transactionID: string,
    connection: RedisClientType,
    rollbackStrategyType: RedisRollbackStrategyType,
    backupHashName?: string
  ) {
    super(transactionID);
    this.connection = connection;
    this.rollbackStrategy = RedisRollbackFactory(
      this.connection,
      rollbackStrategyType,
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
    const itemExists = await this.connection.exists(key);

    let rollbackAction;

    if (itemExists) {
      rollbackAction = async () => {
        this.rollbackStrategy.restoreItem(key);
      };
      this.rollbackStrategy.backupItem(key);
    } else {
      rollbackAction = async () => {
        await this.connection.del(key);
      };
    }

    this.rollbackActions.push(rollbackAction);
    return await this.connection.set(key, value);
  }

  /**
   * Deletes a key from the Redis database.
   *
   * @param key - The key to delete.
   */
  public async del(key: string): Promise<number> {
    this.rollbackStrategy.backupItem(key);
    const rollbackAction = async () => {
      await this.rollbackStrategy.restoreItem(key);
    };
    this.rollbackActions.push(rollbackAction);

    return await this.connection.del(key);
  }

  /**
   * Increments a key in the Redis database.
   *
   * @param key - The key to increment.
   * @returns The new value of the key after incrementing.
   */
  public async incr(key: string): Promise<number> {
    const rollbackAction = async () => {
      await this.connection.decr(key);
    };
    this.rollbackActions.push(rollbackAction);

    return await this.connection.incr(key);
  }

  /**
   * Decrements a key in the Redis database.
   *
   * @param key - The key to decrement.
   * @returns The new value of the key after decrementing.
   */
  public async decr(key: string): Promise<number> {
    const rollbackAction = async () => {
      await this.connection.incr(key);
    };
    this.rollbackActions.push(rollbackAction);

    return await this.connection.decr(key);
  }
}
