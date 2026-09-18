import { S3RollbackClient } from '../S3Client/S3Client';
import { v4 as uuidv4 } from 'uuid';
import { S3Client } from '@aws-sdk/client-s3';
import { S3Config, S3RollbackStrategyType } from '../types/s3';
import { RedisConfig, RedisRollbackStrategyType } from '../types/redis';
import { RedisRollbackClient } from '../RedisClient/RedisClient';
import {
  TransactionCallbackFunction,
  TransactionManagerConfig,
} from '../types/transaction-manager';
import { createClient, RedisClientType } from 'redis';
import { RollbackError } from '../errors';

/**
 * Promise.allSettled reports status as a string-literal union; the enum keeps
 * the comparisons symbolic at the call sites.
 */
enum SettledStatus {
  Fulfilled = 'fulfilled',
  Rejected = 'rejected',
}
function isRejectedOutcome(
  outcome: PromiseSettledResult<unknown>
): outcome is PromiseRejectedResult {
  const status: string = outcome.status;
  return status === SettledStatus.Rejected;
}

function isErrorWithCleanup(
  error: unknown
): error is Error & { cleanupFailures?: unknown[] } {
  return error instanceof Error;
}

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
    let ownedRedisConnection: { quit(): Promise<unknown> } | null = null;

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
      const { connection, rollbackStrategy, backupHashName, ...clientOptions } =
        this.redisConfig;
      const redisClient =
        connection ?? (await createClient(clientOptions).connect());
      // Only connections created here are disconnected during cleanup;
      // a user-supplied connection stays owned by the caller
      if (!connection) {
        ownedRedisConnection = redisClient;
      }
      clients.RedisClient = new RedisRollbackClient(
        transactionID,
        redisClient,
        rollbackStrategy ?? RedisRollbackStrategyType.IN_MEMORY,
        backupHashName
      );
    }

    let transactionError: unknown;
    let transactionFailed = false;

    try {
      await callback(clients);
    } catch (error) {
      transactionFailed = true;
      transactionError = error;
    }

    const cleanupFailures: unknown[] = [];

    if (transactionFailed) {
      const rollbackOutcomes = await Promise.allSettled(
        Object.values(clients).map((client) => client.rollback())
      );
      for (const outcome of rollbackOutcomes) {
        if (isRejectedOutcome(outcome)) {
          cleanupFailures.push(outcome.reason);
        }
      }
    }

    const closeOutcomes = await Promise.allSettled(
      Object.values(clients).map((client) => client.closeTransaction())
    );
    for (const outcome of closeOutcomes) {
      if (isRejectedOutcome(outcome)) {
        cleanupFailures.push(outcome.reason);
      }
    }

    if (ownedRedisConnection) {
      try {
        await ownedRedisConnection.quit();
      } catch (error) {
        cleanupFailures.push(error);
      }
    }

    if (transactionFailed) {
      // The original error always wins - cleanup failures are attached, never thrown instead
      if (cleanupFailures.length > 0 && isErrorWithCleanup(transactionError)) {
        transactionError.cleanupFailures = cleanupFailures;
      }
      throw transactionError;
    }

    if (cleanupFailures.length > 0) {
      throw new RollbackError(
        'Transaction succeeded but cleanup failed',
        cleanupFailures
      );
    }
  }
}
