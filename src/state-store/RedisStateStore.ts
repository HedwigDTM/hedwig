import { RedisConnection } from '../RedisClient/RedisConnection';
import {
  RecordedAction,
  TransactionRecord,
  TransactionStateStore,
} from '../types/transaction-state';

const TX_KEY_PREFIX = 'hedwig:tx:';

/**
 * Redis-backed transaction state store: transaction records live in
 * `hedwig:tx:<id>` hashes, recorded actions in `hedwig:tx:<id>:actions`.
 * State survives process crashes, which is what enables recovery.
 */
export class RedisStateStore implements TransactionStateStore {
  constructor(private readonly connection: RedisConnection) {}

  public async start(record: TransactionRecord): Promise<void> {
    await this.connection.hSet(
      TX_KEY_PREFIX + record.id,
      'status',
      record.status
    );
    await this.connection.hSet(
      TX_KEY_PREFIX + record.id,
      'startedAt',
      record.startedAt
    );
  }

  public async recordAction(id: string, action: RecordedAction): Promise<void> {
    const actionsKey = TX_KEY_PREFIX + id + ':actions';
    const field = `${action.client}:${action.kind}`;
    const existing = await this.connection.hGet(actionsKey, field);
    const seq = existing === undefined ? 0 : Number(existing) + 1;
    await this.connection.hSet(actionsKey, field, String(seq));
  }

  public async markCommitted(id: string): Promise<void> {
    await this.connection.hSet(TX_KEY_PREFIX + id, 'status', 'committed');
  }

  public async markRolledBack(id: string): Promise<void> {
    await this.connection.hSet(TX_KEY_PREFIX + id, 'status', 'rolled-back');
  }

  public async listInFlight(olderThan?: string): Promise<TransactionRecord[]> {
    const inFlight: TransactionRecord[] = [];
    for await (const key of this.connection.scanIterator({
      MATCH: `${TX_KEY_PREFIX}*`,
      COUNT: 100,
    })) {
      const keyString = String(key);
      if (keyString.includes(':actions')) {
        continue;
      }
      const record = await this.connection.hGetAll(keyString);
      if (record.status !== 'in-flight') {
        continue;
      }
      if (olderThan && record.startedAt >= olderThan) {
        continue;
      }
      inFlight.push({
        id: keyString.slice(TX_KEY_PREFIX.length),
        status: 'in-flight',
        startedAt: record.startedAt,
      });
    }
    return inFlight;
  }
}
