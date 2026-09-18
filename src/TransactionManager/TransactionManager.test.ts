import { mock, MockProxy } from 'jest-mock-extended';
import { RedisClientType } from 'redis';
import TransactionManager from './TransactionManager';
import { S3RollbackClient } from '../S3Client/S3Client';
import { RedisRollbackClient } from '../RedisClient/RedisClient';
import { S3RollbackStrategyType } from '../Types/S3/S3RollBackStrategy';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';
import RollbackError from '../RollbackableClient/Errors/RollbackError';

jest.mock('../S3Client/S3Client');
jest.mock('../RedisClient/RedisClient');

const MockedS3 = jest.mocked(S3RollbackClient);
const MockedRedis = jest.mocked(RedisRollbackClient);

describe('TransactionManager', () => {
  let s3Instance: MockProxy<S3RollbackClient>;
  let redisInstance: MockProxy<RedisRollbackClient>;
  let manager: TransactionManager;

  beforeEach(() => {
    s3Instance = mock<S3RollbackClient>();
    redisInstance = mock<RedisRollbackClient>();
    MockedS3.mockImplementation(() => s3Instance);
    MockedRedis.mockImplementation(() => redisInstance);

    manager = new TransactionManager({
      s3Config: {
        region: 'us-east-1',
        rollbackStrategy: S3RollbackStrategyType.IN_MEMORY,
      },
      redisConfig: {
        url: 'redis://localhost:6379',
        rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
        connection: mock<RedisClientType>(),
      },
    });
  });

  it('should not mask the original error when a rollback fails', async () => {
    s3Instance.rollback.mockRejectedValue(
      new RollbackError('1 rollback action failed', [
        new Error('restore failed'),
      ])
    );
    redisInstance.rollback.mockResolvedValue();

    const caught: unknown = await manager
      .transaction(async () => {
        throw new Error('boom');
      })
      .then(
        () => {
          throw new Error('transaction should have rejected');
        },
        (error: unknown) => error
      );

    expect(caught).toBeInstanceOf(Error);
    expect((caught as Error).message).toBe('boom');
    const withCleanup = caught as Error & { cleanupFailures?: unknown[] };
    expect(withCleanup.cleanupFailures).toHaveLength(1);
    expect(withCleanup.cleanupFailures?.[0]).toBeInstanceOf(RollbackError);
  });

  it('should attempt rollback on every client even when one rollback fails', async () => {
    s3Instance.rollback.mockRejectedValue(new Error('s3 rollback failed'));
    redisInstance.rollback.mockResolvedValue();

    await manager
      .transaction(async () => {
        throw new Error('boom');
      })
      .catch(() => undefined);

    expect(s3Instance.rollback).toHaveBeenCalledTimes(1);
    expect(redisInstance.rollback).toHaveBeenCalledTimes(1);
  });

  it('should not mask the original error when closeTransaction fails', async () => {
    s3Instance.rollback.mockResolvedValue();
    redisInstance.rollback.mockResolvedValue();
    s3Instance.closeTransaction.mockRejectedValue(new Error('close failed'));
    redisInstance.closeTransaction.mockResolvedValue();

    const caught: unknown = await manager
      .transaction(async () => {
        throw new Error('boom');
      })
      .then(
        () => {
          throw new Error('transaction should have rejected');
        },
        (error: unknown) => error
      );

    expect((caught as Error).message).toBe('boom');
    const withCleanup = caught as Error & { cleanupFailures?: unknown[] };
    expect(withCleanup.cleanupFailures).toEqual([new Error('close failed')]);
  });

  it('should surface cleanup failures when the transaction succeeded', async () => {
    s3Instance.rollback.mockResolvedValue();
    redisInstance.rollback.mockResolvedValue();
    s3Instance.closeTransaction.mockRejectedValue(new Error('close failed'));
    redisInstance.closeTransaction.mockResolvedValue();

    const caught: unknown = await manager
      .transaction(async () => undefined)
      .then(
        () => {
          throw new Error('transaction should have rejected');
        },
        (error: unknown) => error
      );

    expect(caught).toBeInstanceOf(RollbackError);
    expect((caught as RollbackError).rollbackFailures).toHaveLength(1);
  });

  it('should resolve when the transaction and cleanup succeed', async () => {
    s3Instance.rollback.mockResolvedValue();
    redisInstance.rollback.mockResolvedValue();
    s3Instance.closeTransaction.mockResolvedValue();
    redisInstance.closeTransaction.mockResolvedValue();

    await expect(
      manager.transaction(async () => undefined)
    ).resolves.toBeUndefined();

    expect(s3Instance.closeTransaction).toHaveBeenCalledTimes(1);
    expect(redisInstance.closeTransaction).toHaveBeenCalledTimes(1);
  });
});
