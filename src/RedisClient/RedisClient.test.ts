import { mock, MockProxy } from 'jest-mock-extended';
import { RedisClientType } from 'redis';
import { RedisRollbackClient } from './RedisClient';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';

describe('RedisClient', () => {
  let connection: MockProxy<RedisClientType>;

  beforeEach(() => {
    connection = mock<RedisClientType>();
  });

  it('Checking .get', async () => {
    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    mockRedisClient.get('key');
    await expect(connection.get).toHaveBeenCalledWith('key');
  });

  it('Checking .set - item doesnt exists - IN MEMORY', async () => {
    connection.exists.mockResolvedValueOnce(0);
    connection.set.mockResolvedValueOnce('OK');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.set('key', 'value');

    await expect(connection.exists).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'value');

    // Verifying that the right rollback action was set
    await mockRedisClient.rollback();
    await expect(connection.del).toHaveBeenCalledWith('key');
  });

  it('Checking .set - item exists - IN MEMORY', async () => {
    connection.exists.mockResolvedValueOnce(1);
    connection.set.mockResolvedValueOnce('OK');
    connection.get.mockResolvedValueOnce('previousValue');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.set('key', 'newValue');
    await mockRedisClient.rollback();

    await expect(connection.exists).toHaveBeenCalledWith('key');
    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'newValue');
    await expect(connection.set).toHaveBeenCalledWith('key', 'previousValue');
  });

  it('Checking .set - item doesnt exists - DUPLICATE FILE', async () => {
    connection.exists.mockResolvedValueOnce(0);
    connection.set.mockResolvedValueOnce('OK');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE
    );
    await mockRedisClient.set('key', 'value');

    await expect(connection.exists).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'value');

    // Verifying that the right rollback action was set
    await mockRedisClient.rollback();
    await expect(connection.del).toHaveBeenCalledWith('key');
  });

  it('Checking .set - item exists - DUPLICATE FILE', async () => {
    connection.exists.mockResolvedValueOnce(1);
    connection.set.mockResolvedValueOnce('OK');
    connection.get.mockResolvedValueOnce('previousValue');
    connection.hGet.mockResolvedValueOnce('previousValue');
    connection.hSet.mockResolvedValueOnce(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await mockRedisClient.set('key', 'newValue');
    mockRedisClient.rollback();

    await expect(connection.exists).toHaveBeenCalledWith('key');
    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.hSet).toHaveBeenCalledWith(
      'backupHashName',
      'key',
      'previousValue'
    );
    await expect(connection.set).toHaveBeenCalledWith('key', 'newValue');
    await expect(connection.hGet).toHaveBeenCalledWith('backupHashName', 'key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'previousValue');
  });

  it('Checking .del - item exists - IN MEMORY', async () => {
    connection.exists.mockResolvedValueOnce(1);
    connection.del.mockResolvedValueOnce(1);
    connection.get.mockResolvedValueOnce('value');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.del('key');
    await mockRedisClient.rollback();

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.del).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'value');
  });

  it('Checking .del - item exists - DUPLICATE FILE', async () => {
    connection.exists.mockResolvedValueOnce(1);
    connection.del.mockResolvedValueOnce(1);
    connection.get.mockResolvedValueOnce('value');
    connection.hGet.mockResolvedValueOnce('value');
    connection.hSet.mockResolvedValueOnce(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await mockRedisClient.del('key');
    await mockRedisClient.rollback();

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.del).toHaveBeenCalledWith('key');
    await expect(connection.hSet).toHaveBeenCalledWith(
      'backupHashName',
      'key',
      'value'
    );
    await expect(connection.hGet).toHaveBeenCalledWith('backupHashName', 'key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'value');
  });

  it('Checking .incr', async () => {
    connection.incr.mockResolvedValueOnce(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.incr('key');
    await mockRedisClient.rollback();

    await expect(connection.incr).toHaveBeenCalledWith('key');
    await expect(connection.decr).toHaveBeenCalledWith('key');
  });

  it('Checking .decr', async () => {
    connection.decr.mockResolvedValueOnce(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.decr('key');
    await mockRedisClient.rollback();

    await expect(connection.decr).toHaveBeenCalledWith('key');
    await expect(connection.incr).toHaveBeenCalledWith('key');
  });
});
