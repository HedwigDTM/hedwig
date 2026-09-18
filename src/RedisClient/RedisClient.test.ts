import { mock, MockProxy } from 'jest-mock-extended';
import { RedisClientType } from 'redis';
import { RedisRollbackClient } from './RedisClient';
import { RedisRollbackStrategyType } from '../types/redis';

describe('RedisClient', () => {
  let connection: MockProxy<RedisClientType>;

  beforeEach(() => {
    connection = mock<RedisClientType>();
  });

  it('Checking .get - should get the value', async () => {
    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    mockRedisClient.get('key');
    await expect(connection.get).toHaveBeenCalledWith('key');
  });

  it('Checking .set IN MEMORY - file doesnt exists - should set the value and delete it', async () => {
    connection.get.mockResolvedValueOnce(null);
    connection.set.mockResolvedValueOnce('OK');
    connection.del.mockResolvedValueOnce(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.set('key', 'value');

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'value');

    // Verifying that the right rollback action was set
    await mockRedisClient.rollback();
    await expect(connection.del).toHaveBeenCalledWith('key');
  });

  it('Checking .set IN MEMORY - file exists - should set the new value and rollback to old one', async () => {
    connection.set.mockResolvedValueOnce('OK');
    connection.get.mockResolvedValueOnce('previousValue');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );

    await mockRedisClient.set('key', 'newValue');
    await mockRedisClient.rollback();

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'newValue');
    await expect(connection.set).toHaveBeenCalledWith('key', 'previousValue');
  });

  it('Checking .set DUPLICATE FILE - file doesnt exists - should set the value and delete it', async () => {
    connection.get.mockResolvedValueOnce(null);
    connection.set.mockResolvedValueOnce('OK');
    connection.hGet.mockResolvedValueOnce(
      JSON.stringify({ existed: false, value: null })
    );
    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await mockRedisClient.set('key', 'value');

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', 'value');
    await expect(connection.hSet).toHaveBeenCalledWith(
      'backupHashName:test',
      'key',
      JSON.stringify({ existed: false, value: null })
    );

    // Verifying that the right rollback action was set
    await mockRedisClient.rollback();
    await expect(connection.del).toHaveBeenCalledWith('key');
  });

  it('Checking .set DUPLICATE FILE - file exists - should set the new value and rollback to old one', async () => {
    connection.set.mockResolvedValueOnce('OK');
    connection.get.mockResolvedValueOnce('previousValue');
    connection.hGet.mockResolvedValueOnce(
      JSON.stringify({ existed: true, value: 'previousValue' })
    );
    connection.hSet.mockResolvedValueOnce(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await mockRedisClient.set('key', 'newValue');
    await mockRedisClient.rollback();

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.hSet).toHaveBeenCalledWith(
      'backupHashName:test',
      'key',
      JSON.stringify({ existed: true, value: 'previousValue' })
    );
    await expect(connection.set).toHaveBeenCalledWith('key', 'newValue');
    await expect(connection.hGet).toHaveBeenCalledWith(
      'backupHashName:test',
      'key'
    );
    await expect(connection.set).toHaveBeenCalledWith('key', 'previousValue');
  });

  it('Checking .set DUPLICATE FILE - concurrent transactions use isolated backup hashes', async () => {
    connection.set.mockResolvedValue('OK');
    connection.get.mockResolvedValue('previousValue');
    connection.hSet.mockResolvedValue(1);

    const clientA = new RedisRollbackClient(
      'tx-a',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    const clientB = new RedisRollbackClient(
      'tx-b',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await clientA.set('key', 'newValue');
    await clientB.set('key', 'newValue');

    await expect(connection.hSet).toHaveBeenCalledWith(
      'backupHashName:tx-a',
      'key',
      JSON.stringify({ existed: true, value: 'previousValue' })
    );
    await expect(connection.hSet).toHaveBeenCalledWith(
      'backupHashName:tx-b',
      'key',
      JSON.stringify({ existed: true, value: 'previousValue' })
    );
  });

  it('Checking .closeTransaction() DUPLICATE FILE - should delete the transaction backup hash', async () => {
    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await mockRedisClient.closeTransaction();
    await expect(connection.del).toHaveBeenCalledWith('backupHashName:test');
  });

  it('Checking .del IN MEMORY - item exists - should delete and rollback to old value', async () => {
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

  it('Checking .del DUPLICATE FILE - item exists - should delete and rollback to old value', async () => {
    connection.del.mockResolvedValueOnce(1);
    connection.get.mockResolvedValueOnce('value');
    connection.hGet.mockResolvedValueOnce(
      JSON.stringify({ existed: true, value: 'value' })
    );
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
      'backupHashName:test',
      'key',
      JSON.stringify({ existed: true, value: 'value' })
    );
    await expect(connection.hGet).toHaveBeenCalledWith(
      'backupHashName:test',
      'key'
    );
    await expect(connection.set).toHaveBeenCalledWith('key', 'value');
  });

  it('Checking .incr - should restore the exact pre-transaction value on rollback', async () => {
    connection.get.mockResolvedValueOnce('5');
    connection.incr.mockResolvedValueOnce(6);
    connection.set.mockResolvedValue('OK');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.incr('key');
    await mockRedisClient.rollback();

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.incr).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', '5');
    await expect(connection.decr).not.toHaveBeenCalled();
  });

  it('Checking .incr - missing key - should delete the key on rollback instead of leaving it at zero', async () => {
    connection.get.mockResolvedValueOnce(null);
    connection.incr.mockResolvedValueOnce(1);
    connection.del.mockResolvedValue(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.incr('counter');
    await mockRedisClient.rollback();

    await expect(connection.del).toHaveBeenCalledWith('counter');
    await expect(connection.set).not.toHaveBeenCalled();
  });

  it('Checking .decr - should restore the exact pre-transaction value on rollback', async () => {
    connection.get.mockResolvedValueOnce('3');
    connection.decr.mockResolvedValueOnce(2);
    connection.set.mockResolvedValue('OK');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.decr('key');
    await mockRedisClient.rollback();

    await expect(connection.get).toHaveBeenCalledWith('key');
    await expect(connection.decr).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', '3');
    await expect(connection.incr).not.toHaveBeenCalled();
  });

  it('Checking .del IN MEMORY - missing key - should roll back to no key without failing', async () => {
    connection.get.mockResolvedValueOnce(null);
    connection.del.mockResolvedValue(1);

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.del('missing');
    await mockRedisClient.rollback();

    await expect(connection.del).toHaveBeenCalledWith('missing');
  });

  it('Checking .set IN MEMORY - empty string value - should restore the empty string on rollback', async () => {
    connection.get.mockResolvedValueOnce('');
    connection.set.mockResolvedValue('OK');

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    await mockRedisClient.set('key', 'newValue');
    await mockRedisClient.rollback();

    await expect(connection.set).toHaveBeenCalledWith('key', 'newValue');
    await expect(connection.set).toHaveBeenCalledWith('key', '');
  });

  it('Checking .set IN MEMORY - backup failure should reject instead of floating', async () => {
    connection.get.mockRejectedValue(new Error('connection lost'));

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );

    await expect(mockRedisClient.set('key', 'value')).rejects.toThrow(
      'connection lost'
    );
    expect(connection.set).not.toHaveBeenCalled();
  });

  it('Checking .set DUPLICATE FILE - empty string value - should restore the empty string on rollback', async () => {
    connection.get.mockResolvedValueOnce('');
    connection.set.mockResolvedValue('OK');
    connection.hSet.mockResolvedValue(1);
    connection.hGet.mockResolvedValueOnce(
      JSON.stringify({ existed: true, value: '' })
    );

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await mockRedisClient.set('key', 'newValue');
    await mockRedisClient.rollback();

    await expect(connection.hSet).toHaveBeenCalledWith(
      'backupHashName:test',
      'key',
      JSON.stringify({ existed: true, value: '' })
    );
    await expect(connection.set).toHaveBeenCalledWith('key', 'newValue');
    await expect(connection.set).toHaveBeenCalledWith('key', '');
  });

  it('Checking .del DUPLICATE FILE - empty string value - should restore the empty string on rollback', async () => {
    connection.get.mockResolvedValueOnce('');
    connection.del.mockResolvedValue(1);
    connection.hSet.mockResolvedValue(1);
    connection.hGet.mockResolvedValueOnce(
      JSON.stringify({ existed: true, value: '' })
    );

    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.DUPLICATE_FILE,
      'backupHashName'
    );
    await mockRedisClient.del('key');
    await mockRedisClient.rollback();

    await expect(connection.del).toHaveBeenCalledWith('key');
    await expect(connection.set).toHaveBeenCalledWith('key', '');
  });
});
