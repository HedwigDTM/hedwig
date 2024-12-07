import { S3RollbackClient } from '../S3Client/S3Client';
import { v4 as uuidv4 } from 'uuid';
import { S3Config } from '../Types/S3/S3Config';
import { S3Client } from '@aws-sdk/client-s3';
import {
  TransactionCallbackFunction,
  TransactionManagerConfig,
} from '../Types/TransactionManger.ts';
import { S3RollbackStrategyType } from '../Types/S3/S3RollBackStrategy';
import { RedisConfig } from '../Types/Redis/RedisConfig';
import { RedisRollbackClient } from '../RedisClient/RedisClient';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';
import { createClient, RedisClientType } from 'redis';

/**
 * TransactionManager is responsible for managing distributed transactions
 * across multiple services or data sources. It allows the user to define
 * a sequence of actions, ensuring that all actions are either fully completed
 * or rolled back in case of any failure.
 */
export default class TransactionManager {
  private s3Config?: S3Config;
  private redisConfig?: RedisConfig;

  constructor({ s3Config, redisConfig }: TransactionManagerConfig) {
    this.s3Config = s3Config;
    this.redisConfig = redisConfig;
  }

  /**
   * Executes a transaction with the given actions.
   * @param callback - A callback function that receives an object with the clients.
   */
  public async transaction(
    callback: TransactionCallbackFunction
  ): Promise<void> {
    const transactionID = uuidv4();
    const clients: {
      S3Client?: S3RollbackClient;
      RedisClient?: RedisRollbackClient;
    } = {};

    if (this.s3Config) {
      clients.S3Client = new S3RollbackClient(
        transactionID,
        new S3Client(this.s3Config),
        this.s3Config.rollbackStrategy
          ? this.s3Config.rollbackStrategy
          : S3RollbackStrategyType.IN_MEMORY,
        this.s3Config.backupBucketName
      );
    }

    if (this.redisConfig) {
      clients.RedisClient = new RedisRollbackClient(
        transactionID,
        await (createClient(this.redisConfig) as RedisClientType).connect(),
        this.redisConfig.rollbackStrategy
          ? this.redisConfig.rollbackStrategy
          : RedisRollbackStrategyType.IN_MEMORY,
        this.redisConfig.backupHashName
      );
    }

    try {
      await callback(clients);
    } catch (error) {
      await Promise.all(
        Object.values(clients).map((client) => client.rollback())
      );
      throw error;
    } finally {
      await Promise.all(
        Object.values(clients).map((client) => client.closeTransaction())
      );
    }
  }
}
