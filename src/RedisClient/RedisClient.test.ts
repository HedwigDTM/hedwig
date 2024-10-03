import { mock, MockProxy } from 'jest-mock-extended';
import { RedisClientType } from 'redis';
import { RedisRollbackClient } from './RedisClient';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';

describe('RedisClient', () => {
  let connection: MockProxy<RedisClientType>;

  beforeEach(() => {
    connection = mock<RedisClientType>();
  });

  it('Checking .get', () => {
    const mockRedisClient = new RedisRollbackClient(
      'test',
      connection,
      RedisRollbackStrategyType.IN_MEMORY
    );
    mockRedisClient.get('key');
    expect(connection.get).toHaveBeenCalledWith('key');
  });
});
