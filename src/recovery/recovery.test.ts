import {
  CopyObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
  S3Client,
  ServiceInputTypes,
  ServiceOutputTypes,
  S3ClientResolvedConfig,
} from '@aws-sdk/client-s3';
import { RedisClientType } from 'redis';
import { mockClient, AwsStub } from 'aws-sdk-client-mock';
import 'aws-sdk-client-mock-jest';
import { RedisConnection } from '../RedisClient/RedisConnection';
import { InMemoryStateStore } from '../state-store/InMemoryStateStore';
import { recoverInFlightTransactions } from './recovery';

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
    hGetAll: jest.fn(async () => ({})),
  }) as unknown as RedisConnection;

describe('recoverInFlightTransactions', () => {
  let s3Mock: AwsStub<
    ServiceInputTypes,
    ServiceOutputTypes,
    S3ClientResolvedConfig
  >;

  beforeEach(() => {
    s3Mock = mockClient(S3Client);
  });

  it('should recover orphaned redis transactions from their backup hash', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('tx-crash'));
    await store.recordAction('tx-crash', { client: 'redis', kind: 'set' });

    const connection: RedisConnection = connectionMock();
    (connection.hGetAll as jest.Mock).mockImplementation(
      async (key: unknown): Promise<Record<string, string>> =>
        String(key) === 'Hedwig-Backups:tx-crash'
          ? {
              key1: JSON.stringify({ existed: true, value: 'old' }),
              key2: JSON.stringify({ existed: false, value: null }),
            }
          : {}
    );

    const summary = await recoverInFlightTransactions({
      store,
      redis: { connection },
    });

    expect(summary.recovered).toEqual(['tx-crash']);
    expect(summary.failed).toEqual([]);
    const setMock = connection.set as jest.Mock;
    const delMock = connection.del as jest.Mock;
    expect(setMock).toHaveBeenCalledWith('key1', 'old');
    expect(delMock).toHaveBeenCalledWith('key2');
    expect(delMock).toHaveBeenCalledWith('Hedwig-Backups:tx-crash');
    expect((await store.listInFlight()).length).toBe(0);
  });

  it('should recover orphaned S3 transactions from backup keys', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('tx-s3'));

    s3Mock.on(ListObjectsV2Command).resolves({
      $metadata: { httpStatusCode: 200 },
      Contents: [{ Key: 'tx-s3/bucketA/key1' }],
      IsTruncated: false,
    });
    s3Mock
      .on(CopyObjectCommand)
      .resolves({ $metadata: { httpStatusCode: 200 } });
    s3Mock
      .on(DeleteObjectCommand)
      .resolves({ $metadata: { httpStatusCode: 200 } });

    const connection = new S3Client({ region: 'us-east-1' });

    const summary = await recoverInFlightTransactions({
      store,
      s3: { connection },
    });

    expect(summary.recovered).toEqual(['tx-s3']);
    expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
      Bucket: 'bucketA',
      Key: 'key1',
      CopySource: 'hedwig-backups/tx-s3/bucketA/key1',
    });
    expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
      Bucket: 'hedwig-backups',
      Key: 'tx-s3/bucketA/key1',
    });
  });

  it('should never touch committed transactions', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('tx-done'));
    await store.markCommitted('tx-done');

    const connection: RedisConnection = connectionMock();

    const summary = await recoverInFlightTransactions({
      store,
      redis: { connection },
    });

    expect(summary.recovered).toEqual([]);
    expect(connection.set as jest.Mock).not.toHaveBeenCalled();
    expect(connection.del as jest.Mock).not.toHaveBeenCalled();
  });

  it('should honor the olderThan cutoff', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('tx-old', '2026-01-01T00:00:00Z'));
    await store.start(record('tx-new', '2026-06-01T00:00:00Z'));

    const connection: RedisConnection = connectionMock();

    const summary = await recoverInFlightTransactions({
      store,
      olderThan: '2026-03-01T00:00:00Z',
      redis: { connection },
    });

    expect(summary.recovered).toEqual(['tx-old']);
    expect(connection.set as jest.Mock).not.toHaveBeenCalled();
  });

  it('should report failures and leave those transactions in flight', async () => {
    const store = new InMemoryStateStore();
    await store.start(record('tx-fail'));

    const connection: RedisConnection = connectionMock();
    (connection.hGetAll as jest.Mock).mockRejectedValue(
      new Error('redis down')
    );

    const summary = await recoverInFlightTransactions({
      store,
      redis: { connection },
    });

    expect(summary.recovered).toEqual([]);
    expect(summary.failed).toEqual([
      { id: 'tx-fail', error: expect.any(String) },
    ]);
    // still in flight so a later pass can retry
    expect((await store.listInFlight()).map((r) => r.id)).toEqual(['tx-fail']);
  });
});
