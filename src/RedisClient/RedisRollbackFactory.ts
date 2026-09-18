import { RedisConnection } from './RedisConnection';
import { RedisRollBackStrategy } from './RedisRollbackStrategy';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';
import { InMemoryStrategy } from './RedisStrategies/InMemoryStrategy';
import { DuplicateStrategy } from './RedisStrategies/DuplicateStrategy';

export const RedisRollbackFactory = (
  connection: RedisConnection,
  strategy: RedisRollbackStrategyType,
  transactionID: string,
  backupHashName?: string
): RedisRollBackStrategy => {
  switch (strategy) {
    case RedisRollbackStrategyType.IN_MEMORY: {
      return new InMemoryStrategy(connection);
    }
    case RedisRollbackStrategyType.DUPLICATE_FILE: {
      return new DuplicateStrategy(
        connection,
        transactionID,
        backupHashName ? backupHashName : 'Hedwig-Backups'
      );
    }
    default:
      throw new Error('Rollback strategy type was not found!');
  }
};
