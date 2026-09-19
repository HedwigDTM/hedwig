import { mock, MockProxy } from 'jest-mock-extended';
import { createClient, RedisClientType } from 'redis';
import TransactionManager from './TransactionManager';
import { S3RollbackClient } from '../S3Client/S3Client';
import { RedisRollbackClient } from '../RedisClient/RedisClient';
import { RedisRollbackStrategyType } from '../types/redis';
import { S3RollbackStrategyType } from '../types/s3';
import { RollbackError } from '../errors';

jest.mock('../S3Client/S3Client');
jest.mock('../RedisClient/RedisClient');
jest.mock('redis');

const MockedS3 = jest.mocked(S3RollbackClient);
const MockedRedis = jest.mocked(RedisRollbackClient);
const MockedCreateClient = jest.mocked(createClient);

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

  it('should log lifecycle events when verbose is enabled', async () => {
    const infoSpy = jest
      .spyOn(console, 'info')
      .mockImplementation(() => undefined);
    s3Instance.closeTransaction.mockResolvedValue();
    redisInstance.closeTransaction.mockResolvedValue();
    s3Instance.rollback.mockResolvedValue();
    redisInstance.rollback.mockResolvedValue();

    const verboseManager = new TransactionManager({
      s3Config: {
        region: 'us-east-1',
        rollbackStrategy: S3RollbackStrategyType.IN_MEMORY,
      },
      redisConfig: {
        url: 'redis://localhost:6379',
        rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
        connection: mock<RedisClientType>(),
      },
      verbose: true,
    });

    await verboseManager.transaction(async () => undefined);

    const messages = infoSpy.mock.calls.map((call) => call[0]);
    expect(messages.some((m) => m.includes('started'))).toBe(true);
    expect(messages.some((m) => m.includes('committed'))).toBe(true);
    infoSpy.mockRestore();
  });

  it('should stay silent when verbose is not enabled', async () => {
    const infoSpy = jest
      .spyOn(console, 'info')
      .mockImplementation(() => undefined);
    s3Instance.closeTransaction.mockResolvedValue();
    redisInstance.closeTransaction.mockResolvedValue();
    s3Instance.rollback.mockResolvedValue();
    redisInstance.rollback.mockResolvedValue();

    await manager.transaction(async () => undefined);

    expect(infoSpy).not.toHaveBeenCalled();
    infoSpy.mockRestore();
  });

  it('should return the callback result and expose configured clients as non-optional', async () => {
    s3Instance.closeTransaction.mockResolvedValue();
    redisInstance.closeTransaction.mockResolvedValue();
    s3Instance.rollback.mockResolvedValue();
    redisInstance.rollback.mockResolvedValue();

    const result = await manager.transaction(
      async ({ S3Client, RedisClient }) => {
        expect(S3Client).toBeDefined();
        expect(RedisClient).toBeDefined();
        return 42;
      }
    );

    expect(result).toBe(42);
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

  describe('Redis connection ownership', () => {
    let ownedConnection: MockProxy<RedisClientType>;
    let suppliedConnection: MockProxy<RedisClientType>;

    beforeEach(() => {
      ownedConnection = mock<RedisClientType>();
      ownedConnection.connect.mockResolvedValue(ownedConnection);
      ownedConnection.quit.mockResolvedValue('OK');
      MockedCreateClient.mockReturnValue(ownedConnection);
      suppliedConnection = mock<RedisClientType>();
      MockedCreateClient.mockClear();
    });

    it('should disconnect connections it created', async () => {
      s3Instance.rollback.mockResolvedValue();
      redisInstance.rollback.mockResolvedValue();
      s3Instance.closeTransaction.mockResolvedValue();
      redisInstance.closeTransaction.mockResolvedValue();

      const ownedManager = new TransactionManager({
        redisConfig: {
          url: 'redis://localhost:6379',
          rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
        },
      });

      await ownedManager.transaction(async () => undefined);

      expect(MockedCreateClient).toHaveBeenCalledWith({
        url: 'redis://localhost:6379',
      });
      expect(ownedConnection.quit).toHaveBeenCalledTimes(1);
    });

    it('should never disconnect a user-supplied connection', async () => {
      s3Instance.rollback.mockResolvedValue();
      redisInstance.rollback.mockResolvedValue();
      s3Instance.closeTransaction.mockResolvedValue();
      redisInstance.closeTransaction.mockResolvedValue();

      const suppliedManager = new TransactionManager({
        redisConfig: {
          url: 'redis://localhost:6379',
          rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
          connection: suppliedConnection,
        },
      });

      await suppliedManager.transaction(async () => undefined);

      expect(MockedCreateClient).not.toHaveBeenCalled();
      expect(suppliedConnection.quit).not.toHaveBeenCalled();
    });

    it('should not mask the original error when disconnecting fails', async () => {
      s3Instance.rollback.mockResolvedValue();
      redisInstance.rollback.mockResolvedValue();
      s3Instance.closeTransaction.mockResolvedValue();
      redisInstance.closeTransaction.mockResolvedValue();
      ownedConnection.quit.mockRejectedValue(new Error('quit failed'));

      const ownedManager = new TransactionManager({
        redisConfig: {
          url: 'redis://localhost:6379',
          rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
        },
      });

      const caught: unknown = await ownedManager
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
      expect(withCleanup.cleanupFailures).toEqual([new Error('quit failed')]);
    });
  });
});
