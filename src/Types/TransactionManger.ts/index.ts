import { RedisRollbackClient } from '../../RedisClient/RedisClient';
import { S3RollbackClient } from '../../S3Client/S3Client';
import { RedisConfig } from '../Redis/RedisConfig';
import { S3Config } from '../S3/S3Config';

export type RollbackableClients = {
  S3Client: S3RollbackClient;
  RedisClient: RedisRollbackClient;
};
export type TransactionCallbackFunction = (
  clients: Partial<RollbackableClients>
) => Promise<void>;

export type TransactionManagerConfig = {
  s3Config?: S3Config;
  redisConfig?: RedisConfig;
};
