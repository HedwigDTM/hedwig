import { RedisConnection } from '../RedisClient/RedisConnection';
import { InMemoryStateStore } from './InMemoryStateStore';
import { RedisStateStore } from './RedisStateStore';
import { RecordedAction } from '../types/transaction-state';

const record = (id: string, startedAt = '2026-01-01T00:00:00Z') => ({
  id,
  status: 'in-flight' as const,
  startedAt,
});

const connectionMock = (): RedisConnection =>
  ({
    get: jest.fn(),
    set: jest.fn(),
    del: jest.fn(),
    exists: jest.fn(),
    incr: jest.fn(),
    decr: jest.fn(),
    hGet: jest.fn(),
    hSet: jest.fn(),
    scanIterator: jest.fn(),
    hGetAll: jest.fn(),
  }) as unknown as RedisConnection;

describe('InMemoryStateStore', () => {
  it('should record and list transactions in flight', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('tx-1'));
    await store.start(record('tx-2', '2026-06-01T00:00:00Z'));

    const inFlight = await store.listInFlight();

    expect(inFlight.map((r) => r.id)).toEqual(['tx-1', 'tx-2']);
  });

  it('should filter by age when listing', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('old', '2026-01-01T00:00:00Z'));
    await store.start(record('new', '2026-06-01T00:00:00Z'));

    const inFlight = await store.listInFlight('2026-03-01T00:00:00Z');

    expect(inFlight.map((r) => r.id)).toEqual(['old']);
  });

  it('should only list in-flight transactions', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('a'));
    await store.start(record('b'));

    await store.markCommitted('a');
    const inFlight = await store.listInFlight();

    expect(inFlight.map((r) => r.id)).toEqual(['b']);
  });

  it('should record actions and expose them', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('tx'));
    const action: RecordedAction = { client: 'redis', kind: 'set' };

    await store.recordAction('tx', action);

    expect(store.getActions('tx')).toEqual([action]);
    expect(store.getActions('tx', 'redis')).toEqual([action]);
    expect(store.getActions('tx', 's3')).toEqual([]);
  });

  it('should reject action recording without a transaction record', async () => {
    const store = new InMemoryStateStore();

    await expect(
      store.recordAction('missing', { client: 's3', kind: 'putObject' })
    ).rejects.toThrow('No transaction record');
  });
});

describe('RedisStateStore', () => {
  let connection: RedisConnection;
  let store: RedisStateStore;

  beforeEach(() => {
    connection = connectionMock();
    store = new RedisStateStore(connection);
  });

  it('should persist the transaction record as a hash', async () => {
    await store.start(record('tx-1'));

    const hSetMock = connection.hSet as jest.Mock;
    expect(hSetMock).toHaveBeenCalledWith(
      'hedwig:tx:tx-1',
      'status',
      'in-flight'
    );
    expect(hSetMock).toHaveBeenCalledWith(
      'hedwig:tx:tx-1',
      'startedAt',
      '2026-01-01T00:00:00Z'
    );
  });

  it('should record actions with a per-kind sequence', async () => {
    (connection.hGet as jest.Mock)
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce('0');

    await store.recordAction('tx', { client: 'redis', kind: 'set' });
    await store.recordAction('tx', { client: 'redis', kind: 'set' });

    const hSetMock = connection.hSet as jest.Mock;
    expect(hSetMock).toHaveBeenNthCalledWith(
      1,
      'hedwig:tx:tx:actions',
      'redis:set',
      '0'
    );
    expect(hSetMock).toHaveBeenNthCalledWith(
      2,
      'hedwig:tx:tx:actions',
      'redis:set',
      '1'
    );
  });

  it('should mark statuses on the transaction hash', async () => {
    await store.markCommitted('tx');
    await store.markRolledBack('tx');

    const hSetMock = connection.hSet as jest.Mock;
    expect(hSetMock).toHaveBeenCalledWith(
      'hedwig:tx:tx',
      'status',
      'committed'
    );
    expect(hSetMock).toHaveBeenCalledWith(
      'hedwig:tx:tx',
      'status',
      'rolled-back'
    );
  });

  it('should list in-flight transactions from scanned records', async () => {
    (connection.scanIterator as jest.Mock).mockImplementation(
      async function* () {
        yield 'hedwig:tx:live';
        yield 'hedwig:tx:live:actions';
        yield 'hedwig:tx:done';
      }
    );
    (connection.hGetAll as jest.Mock).mockImplementation(
      async (key: unknown): Promise<Record<string, string>> => {
        const k = String(key);
        if (k === 'hedwig:tx:live') {
          return { status: 'in-flight', startedAt: '2026-01-01T00:00:00Z' };
        }
        if (k === 'hedwig:tx:done') {
          return { status: 'committed', startedAt: '2026-01-01T00:00:00Z' };
        }
        return {};
      }
    );

    const inFlight = await store.listInFlight();

    expect(inFlight).toEqual([
      { id: 'live', status: 'in-flight', startedAt: '2026-01-01T00:00:00Z' },
    ]);
  });
});
