import { S3RollbackClient } from '../S3Client/S3Client';
import { v4 as uuidv4 } from 'uuid';
import { S3Client } from '@aws-sdk/client-s3';
import { S3Config, S3RollbackStrategyType } from '../types/s3';
import { RedisConfig, RedisRollbackStrategyType } from '../types/redis';
import { CustomActionRegistry } from '../CustomAction/CustomActionRegistry';
import { CustomActionsApi } from '../CustomAction/ICustomAction';
import { RedisRollbackClient } from '../RedisClient/RedisClient';
import {
  RollbackableClientsFor,
  TransactionCallbackFunction,
  TransactionManagerConfig,
} from '../types/transaction-manager';
import { createClient } from 'redis';
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
export default class TransactionManager<
  C extends TransactionManagerConfig = TransactionManagerConfig,
> {
  private s3Config?: S3Config;
  private redisConfig?: RedisConfig;
  private verbose: boolean;

  constructor(config: C) {
    this.s3Config = config.s3Config;
    this.redisConfig = config.redisConfig;
    this.verbose = config.verbose === true;
  }

  /**
   * Lifecycle logger: silent unless `verbose: true` was configured.
   */
  private log(message: string): void {
    if (this.verbose) {
      console.info(`[hedwig] ${message}`);
    }
  }

  /**
   * Executes a transaction with the given actions.
   * @param callback - A callback function that receives an object with the clients.
   */
  public async transaction<Result>(
    callback: TransactionCallbackFunction<
      RollbackableClientsFor<C> & { customActions: CustomActionsApi },
      Result
    >
  ): Promise<Result> {
    const transactionID = uuidv4();
    this.log(`transaction ${transactionID} started`);
    const clients: {
      S3Client?: S3RollbackClient;
      RedisClient?: RedisRollbackClient;
      CustomActions?: CustomActionRegistry;
    } = {};
    const customActions = new CustomActionRegistry(transactionID);
    clients.CustomActions = customActions;
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

    // Assigned exactly when the callback succeeds; the early throws below
    // guarantee the return statement is only reached on that path
    let transactionResult!: Result;
    let transactionError: unknown;
    let transactionFailed = false;

    try {
      // The public signature maps client optionality from the config type C.
      // TypeScript cannot verify a deferred conditional type from the runtime
      // bag, so this named boundary is the single assertion in the file; the
      // bag only ever holds clients this config created.
      const clientsForConfig = {
        ...(clients as RollbackableClientsFor<C>),
        customActions: customActions as CustomActionsApi,
      } as RollbackableClientsFor<C> & { customActions: CustomActionsApi };
      transactionResult = await callback(clientsForConfig);
    } catch (error) {
      transactionFailed = true;
      transactionError = error;
      this.log(
        `transaction ${transactionID} callback failed, rolling back: ${String(
          transactionError
        )}`
      );
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

    if (cleanupFailures.length > 0) {
      this.log(
        `transaction ${transactionID} rollback completed with ${cleanupFailures.length} failure(s)`
      );
    } else {
      this.log(`transaction ${transactionID} rolled back`);
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
      this.log(
        `transaction ${transactionID} rolled back with attached cleanup failures`
      );
      throw transactionError;
    }

    if (cleanupFailures.length > 0) {
      throw new RollbackError(
        'Transaction succeeded but cleanup failed',
        cleanupFailures
      );
    }

    this.log(`transaction ${transactionID} committed`);
    return transactionResult;
  }
}
