import { RedisRollbackClient } from '../RedisClient/RedisClient';
import { S3RollbackClient } from '../S3Client/S3Client';
import { RedisConfig } from './redis';
import { S3Config } from './s3';

export type RollbackableClients = {
  S3Client: S3RollbackClient;
  RedisClient: RedisRollbackClient;
};

export type TransactionCallbackFunction<
  Clients = Partial<RollbackableClients>,
  Result = void,
> = (clients: Clients) => Promise<Result>;

export type TransactionManagerConfig = {
  s3Config?: S3Config;
  redisConfig?: RedisConfig;
  /**
   * When enabled, hedwig logs transaction lifecycle events (start, rollback,
   * cleanup outcomes) to the console.
   */
  verbose?: boolean;
};

/**
 * The clients a configured TransactionManager exposes. A client is
 * non-optional exactly when its config was provided to the manager, so
 * callbacks never need "if configured" guards for configured clients.
 */
export type RollbackableClientsFor<C extends TransactionManagerConfig> = {
  S3Client: C['s3Config'] extends S3Config ? S3RollbackClient : undefined;
  RedisClient: C['redisConfig'] extends RedisConfig
    ? RedisRollbackClient
    : undefined;
};
