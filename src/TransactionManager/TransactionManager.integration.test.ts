import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  HeadObjectCommand,
  ServiceInputTypes,
  ServiceOutputTypes,
  S3ClientResolvedConfig,
} from '@aws-sdk/client-s3';
import { mock, MockProxy } from 'jest-mock-extended';
import { RedisClientType } from 'redis';
import TransactionManager from './TransactionManager';
import { S3RollbackStrategyType } from '../Types/S3/S3RollBackStrategy';
import { RedisRollbackStrategyType } from '../Types/Redis/RedisRollbackStrategy';
import { S3Config } from '../Types/S3/S3Config';
import { RedisConfig } from '../Types/Redis/RedisConfig';
import { mockClient, AwsStub } from 'aws-sdk-client-mock';
import { Readable } from 'stream';
import { sdkStreamMixin } from '@smithy/util-stream';
import 'aws-sdk-client-mock-jest';

// Test configuration
const TEST_BUCKET = 'test-transaction-bucket';
const TEST_KEY = 'test-key';
const TEST_VALUE = 'test-value';
const TEST_REDIS_KEY = 'test-redis-key';
const TEST_REDIS_VALUE = 'test-redis-value';

// S3 configuration for testing
const s3Config: S3Config = {
  region: process.env.AWS_REGION || 'us-east-1',
  rollbackStrategy: S3RollbackStrategyType.IN_MEMORY,
  backupBucketName: 'test-backup-bucket',
};

describe('TransactionManager Integration Tests', () => {
  let transactionManager: TransactionManager;
  let s3Mock: AwsStub<
    ServiceInputTypes,
    ServiceOutputTypes,
    S3ClientResolvedConfig
  >;
  let redisConnection: MockProxy<RedisClientType>;

  beforeEach(() => {
    // Initialize mocked S3 client
    s3Mock = mockClient(S3Client);
    s3Mock.reset();

    // Initialize mocked Redis connection
    redisConnection = mock<RedisClientType>({
      connect: jest.fn().mockResolvedValue(redisConnection),
      get: jest.fn().mockResolvedValue(null),
      set: jest.fn().mockResolvedValue('OK'),
      del: jest.fn().mockResolvedValue(1),
      exists: jest.fn().mockResolvedValue(0),
    });

    // Redis configuration for testing
    const redisConfig: RedisConfig = {
      url: process.env.REDIS_URL || 'redis://localhost:6379',
      rollbackStrategy: RedisRollbackStrategyType.IN_MEMORY,
      backupHashName: 'test-backup-hash',
      connection: redisConnection,
    };

    // Create TransactionManager with mocked clients
    transactionManager = new TransactionManager({
      s3Config,
      redisConfig,
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
    s3Mock.reset();
  });

  it('should successfully commit a transaction with both S3 and Redis operations', async () => {
    // Mock S3 responses
    const mockStream = new Readable();
    mockStream.push(TEST_VALUE);
    mockStream.push(null);

    s3Mock.on(PutObjectCommand).resolves({
      $metadata: { httpStatusCode: 200 },
    });

    s3Mock.on(GetObjectCommand).resolves({
      $metadata: { httpStatusCode: 200 },
      Body: sdkStreamMixin(mockStream),
      ContentLength: TEST_VALUE.length,
      ContentType: 'text/plain',
    });

    await transactionManager.transaction(async ({ S3Client, RedisClient }) => {
      if (!S3Client || !RedisClient) {
        throw new Error('Clients not initialized');
      }

      // Perform S3 operation
      await S3Client.putObject({
        Bucket: TEST_BUCKET,
        Key: TEST_KEY,
        Body: Buffer.from(TEST_VALUE),
      });

      // Perform Redis operation
      await RedisClient.set(TEST_REDIS_KEY, TEST_REDIS_VALUE);
    });

    // Verify S3 operation was committed
    expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, {
      Bucket: TEST_BUCKET,
      Key: TEST_KEY,
      Body: expect.any(Buffer),
    });

    // Verify Redis operation was committed
    expect(redisConnection.set).toHaveBeenCalledWith(
      TEST_REDIS_KEY,
      TEST_REDIS_VALUE
    );
  });

  it('should rollback both S3 and Redis operations when an error occurs', async () => {
    // Mock S3 responses
    const mockStream = new Readable();
    mockStream.push('initial-value');
    mockStream.push(null);

    s3Mock.on(PutObjectCommand).resolves({
      $metadata: { httpStatusCode: 200 },
    });

    s3Mock.on(GetObjectCommand).resolves({
      $metadata: { httpStatusCode: 200 },
      Body: sdkStreamMixin(mockStream),
      ContentLength: 'initial-value'.length,
      ContentType: 'text/plain',
    });

    // Set up initial state
    redisConnection.get.mockResolvedValueOnce('initial-value');

    // Attempt transaction that will fail
    await expect(
      transactionManager.transaction(async ({ S3Client, RedisClient }) => {
        if (!S3Client || !RedisClient) {
          throw new Error('Clients not initialized');
        }

        // Update S3
        await S3Client.putObject({
          Bucket: TEST_BUCKET,
          Key: TEST_KEY,
          Body: Buffer.from('new-value'),
        });

        // Update Redis
        await RedisClient.set(TEST_REDIS_KEY, 'new-value');

        // Simulate an error
        throw new Error('Transaction failed');
      })
    ).rejects.toThrow('Transaction failed');

    // Verify S3 operations sequence
    expect(s3Mock).toHaveReceivedNthCommandWith(1, HeadObjectCommand, {
      Bucket: TEST_BUCKET,
      Key: TEST_KEY,
    });
    expect(s3Mock).toHaveReceivedNthCommandWith(2, GetObjectCommand, {
      Bucket: TEST_BUCKET,
      Key: TEST_KEY,
    });
    expect(s3Mock).toHaveReceivedNthCommandWith(3, PutObjectCommand, {
      Bucket: TEST_BUCKET,
      Key: TEST_KEY,
      Body: expect.any(Uint8Array),
    });
    expect(s3Mock).toHaveReceivedNthCommandWith(4, PutObjectCommand, {
      Bucket: TEST_BUCKET,
      Key: TEST_KEY,
      Body: expect.any(Uint8Array),
    });

    // Verify Redis operations sequence
    expect(redisConnection.set).toHaveBeenNthCalledWith(
      1,
      TEST_REDIS_KEY,
      'new-value'
    );
    expect(redisConnection.set).toHaveBeenNthCalledWith(
      2,
      TEST_REDIS_KEY,
      'initial-value'
    );
  });

  it('should handle concurrent transactions correctly', async () => {
    // Mock S3 responses
    const mockStream1 = new Readable();
    mockStream1.push('value1');
    mockStream1.push(null);

    const mockStream2 = new Readable();
    mockStream2.push('value2');
    mockStream2.push(null);

    s3Mock.on(PutObjectCommand).resolves({
      $metadata: { httpStatusCode: 200 },
    });

    // Mock GetObjectCommand responses for both transactions
    s3Mock
      .on(GetObjectCommand)
      .resolvesOnce({
        $metadata: { httpStatusCode: 200 },
        Body: sdkStreamMixin(mockStream1),
        ContentLength: 'value1'.length,
        ContentType: 'text/plain',
      })
      .resolvesOnce({
        $metadata: { httpStatusCode: 200 },
        Body: sdkStreamMixin(mockStream2),
        ContentLength: 'value2'.length,
        ContentType: 'text/plain',
      });

    const transaction1 = transactionManager.transaction(
      async ({ S3Client, RedisClient }) => {
        if (!S3Client || !RedisClient) {
          throw new Error('Clients not initialized');
        }

        await S3Client.putObject({
          Bucket: TEST_BUCKET,
          Key: 'key1',
          Body: Buffer.from('value1'),
        });
        await RedisClient.set('key1', 'value1');
      }
    );

    const transaction2 = transactionManager.transaction(
      async ({ S3Client, RedisClient }) => {
        if (!S3Client || !RedisClient) {
          throw new Error('Clients not initialized');
        }

        await S3Client.putObject({
          Bucket: TEST_BUCKET,
          Key: 'key2',
          Body: Buffer.from('value2'),
        });
        await RedisClient.set('key2', 'value2');
      }
    );

    await Promise.all([transaction1, transaction2]);

    // Verify S3 operations
    expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, {
      Bucket: TEST_BUCKET,
      Key: 'key1',
      Body: expect.any(Buffer),
    });
    expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, {
      Bucket: TEST_BUCKET,
      Key: 'key2',
      Body: expect.any(Buffer),
    });

    // Verify Redis operations
    expect(redisConnection.set).toHaveBeenCalledWith('key1', 'value1');
    expect(redisConnection.set).toHaveBeenCalledWith('key2', 'value2');
  });
});
