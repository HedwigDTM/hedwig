import {
  S3Client as AWSClient,
  CopyObjectCommand,
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadBucketCommand,
  HeadObjectCommand,
  ListBucketsCommand,
  ListBucketsCommandInput,
  ListObjectsCommand,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { S3RollbackStrategyType } from '../Types/S3/S3RollBackStrategy';
import { S3BucketParams, S3ObjectParams, S3RollbackClient } from './S3Client';
import 'aws-sdk-client-mock-jest';
import { mockClient } from 'aws-sdk-client-mock';
import { Readable } from 'stream';
import { sdkStreamMixin } from '@smithy/util-stream';

describe('S3Client', () => {
  const s3Mock = mockClient(AWSClient);
  let connection: AWSClient;

  beforeEach(() => {
    s3Mock.reset();
    connection = new AWSClient({});
  });

  describe('General operations', () => {
    it('Checking .headBucket() - should return info on the bucket', async () => {
      s3Mock.on(HeadBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      const result = await mockS3Client.headBucket(params);

      expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, params);
      expect(result.$metadata.httpStatusCode).toBe(200);
    });

    it('Checking .headObject() - should return object metadata', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        ContentLength: 1024,
        ContentType: 'text/plain',
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      const result = await mockS3Client.headObject(params);

      expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
      expect(result.$metadata.httpStatusCode).toBe(200);
      expect(result.ContentLength).toBe(1024);
      expect(result.ContentType).toBe('text/plain');
    });

    it('Checking .listBuckets() - should list all buckets', async () => {
      const mockBuckets = [
        { Name: 'bucket1', CreationDate: new Date() },
        { Name: 'bucket2', CreationDate: new Date() },
      ];

      s3Mock.on(ListBucketsCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Buckets: mockBuckets,
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: ListBucketsCommandInput = {};

      const result = await mockS3Client.listBuckets(params);

      expect(s3Mock).toHaveReceivedCommandWith(ListBucketsCommand, params);
      expect(result.$metadata.httpStatusCode).toBe(200);
      expect(result.Buckets).toEqual(mockBuckets);
    });

    it('Checking .getObject() - should retrieve an object', async () => {
      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);

      s3Mock.on(GetObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Body: sdkStreamMixin(mockStream),
        ContentLength: 11,
        ContentType: 'text/plain',
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      const result = await mockS3Client.getObject(params);

      expect(s3Mock).toHaveReceivedCommandWith(GetObjectCommand, params);
      expect(result.$metadata.httpStatusCode).toBe(200);
      expect(result.ContentLength).toBe(11);
      expect(result.ContentType).toBe('text/plain');
      expect(result.Body).toBeDefined();
    });

    it('Checking .closeTransaction() - should close the transaction', async () => {
      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );

      await mockS3Client.closeTransaction();
      // Verify that the rollback strategy's closeTransaction was called
      // This is an implementation detail that depends on the specific strategy
      // being used, so we just verify the method exists and can be called
    });

    it('Checking .putObject() - should fail instead of assuming the object is missing when head returns a non-404 error', async () => {
      s3Mock.on(HeadObjectCommand).rejects({
        name: 'AccessDenied',
        $metadata: { httpStatusCode: 403 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );

      await expect(
        mockS3Client.putObject({ Bucket: 'bucketName', Key: 'key' })
      ).rejects.toMatchObject({ name: 'AccessDenied' });
      expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
      expect(s3Mock.commandCalls(DeleteObjectCommand)).toHaveLength(0);
    });

    it('Checking .deleteObject() - should not back up or restore a missing object', async () => {
      s3Mock.on(HeadObjectCommand).rejects({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });
      s3Mock.on(DeleteObjectCommand).resolves({
        $metadata: { httpStatusCode: 204 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      await mockS3Client.deleteObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, params);
      expect(s3Mock.commandCalls(CopyObjectCommand)).toHaveLength(0);
    });

    it('Checking .createBucket() - should not delete an existing bucket on rollback when head reports it already exists', async () => {
      s3Mock.on(HeadBucketCommand).rejects({
        name: 'BucketAlreadyOwnedByYou',
        $metadata: { httpStatusCode: 409 },
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      await mockS3Client.createBucket(params);
      await mockS3Client.rollback();

      expect(s3Mock.commandCalls(DeleteBucketCommand)).toHaveLength(0);
    });

    it('Checking .createBucket() - should fail instead of assuming the bucket is missing when head returns a non-404 error', async () => {
      s3Mock.on(HeadBucketCommand).rejects({
        name: 'AccessDenied',
        $metadata: { httpStatusCode: 403 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );

      await expect(
        mockS3Client.createBucket({ Bucket: 'bucketName' })
      ).rejects.toMatchObject({ name: 'AccessDenied' });
      expect(s3Mock.commandCalls(CreateBucketCommand)).toHaveLength(0);
    });
  });

  describe('Duplicate strategy', () => {
    it('Checking .deleteBucket() DUPLICATE - should delete a bucket and restore in upon rollback', async () => {
      // Mock S3 Commands
      s3Mock.on(ListObjectsCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Contents: [
          {
            Key: 'key',
          },
        ],
      });

      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      await mockS3Client.deleteBucket(params);
      await mockS3Client.rollback();

      await expect(s3Mock).toHaveReceivedCommandWith(CreateBucketCommand, {
        Bucket: 'hedwig-backups-bucketName',
      });
      await expect(s3Mock).toHaveReceivedCommandWith(ListObjectsCommand, {
        Bucket: 'bucketName',
      });
      await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'hedwig-backups-bucketName',
        Key: 'key',
        CopySource: 'bucketName/key',
      });
      await expect(s3Mock).toHaveReceivedCommandWith(
        DeleteBucketCommand,
        params
      );
      await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        CopySource: 'hedwig-backups-bucketName/key',
      });
    });

    it('Checking .createBucket - should create a bucket and delete it upon rollback', async () => {
      s3Mock.on(HeadBucketCommand).rejects({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      await mockS3Client.createBucket(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CreateBucketCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(DeleteBucketCommand, params);
    });

    it('Checking .deleteObject() DUPLICATE - should delete the object and restore it upon rollback', async () => {
      s3Mock.on(DeleteObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(HeadBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      await mockS3Client.deleteObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, {
        Bucket: 'hedwig-backups',
      });
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'hedwig-backups',
        Key: 'test/bucketName/key',
        CopySource: 'bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        CopySource: 'hedwig-backups/test/bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'hedwig-backups',
        Key: 'test/bucketName/key',
      });
    });

    it('Checking .putObject() DUPLICATE - Object exists - should set the new file and restore the old value upon rollback', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(PutObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);
      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const params: S3ObjectParams = {
        Bucket: 'bucketName',
        Key: 'key',
        Body: sdkStreamMixin(mockStream) as any,
      };

      await mockS3Client.putObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'hedwig-backups',
        Key: 'test/bucketName/key',
        CopySource: 'bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        CopySource: 'hedwig-backups/test/bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'hedwig-backups',
        Key: 'test/bucketName/key',
      });
    });

    it('Checking .putObject() DUPLICATE - Object doesnt exists - should set the new file and delete it upon rollback', async () => {
      s3Mock.on(HeadObjectCommand).rejects({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });

      s3Mock.on(PutObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);
      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const params: S3ObjectParams = {
        Bucket: 'bucketName',
        Key: 'key',
        Body: sdkStreamMixin(mockStream) as any,
      };

      await mockS3Client.putObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
      });
    });

    it('Checking .putObject() DUPLICATE - concurrent transactions on the same key use isolated backup keys', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(PutObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });

      const clientA = new S3RollbackClient(
        'tx-a',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const clientB = new S3RollbackClient(
        'tx-b',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const params: S3ObjectParams = {
        Bucket: 'bucketName',
        Key: 'key',
        Body: Buffer.from('value'),
      };

      await clientA.putObject(params);
      await clientB.putObject(params);

      const backupKeys = s3Mock
        .commandCalls(CopyObjectCommand)
        .map((call) => call.args[0].input.Key);
      expect(backupKeys).toEqual(
        expect.arrayContaining(['tx-a/bucketName/key', 'tx-b/bucketName/key'])
      );
    });

    it('Checking .putObject() DUPLICATE - same key in different buckets uses bucket-scoped backup keys', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(PutObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );

      await mockS3Client.putObject({ Bucket: 'bucket-a', Key: 'shared' });
      await mockS3Client.putObject({ Bucket: 'bucket-b', Key: 'shared' });

      const backupKeys = s3Mock
        .commandCalls(CopyObjectCommand)
        .map((call) => call.args[0].input.Key);
      expect(backupKeys).toEqual(
        expect.arrayContaining(['test/bucket-a/shared', 'test/bucket-b/shared'])
      );
    });

    it('Checking .closeTransaction() DUPLICATE - should delete only this transaction backup objects', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(HeadBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(PutObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(DeleteObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });

      const clientA = new S3RollbackClient(
        'tx-a',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const clientB = new S3RollbackClient(
        'tx-b',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const params: S3ObjectParams = {
        Bucket: 'bucketName',
        Key: 'key',
        Body: Buffer.from('value'),
      };

      await clientA.putObject(params);
      await clientB.putObject(params);
      await clientA.closeTransaction();

      const deletedKeys = s3Mock
        .commandCalls(DeleteObjectCommand)
        .map((call) => call.args[0].input.Key);
      expect(deletedKeys).toContain('tx-a/bucketName/key');
      expect(deletedKeys).not.toContain('tx-b/bucketName/key');
      expect(s3Mock.commandCalls(DeleteBucketCommand)).toHaveLength(0);

      await clientB.closeTransaction();
      const deletedAfterB = s3Mock
        .commandCalls(DeleteObjectCommand)
        .map((call) => call.args[0].input.Key);
      expect(deletedAfterB).toContain('tx-b/bucketName/key');
    });

    it('Checking .closeTransaction() DUPLICATE - should remove the general backup bucket only when this transaction created it', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(HeadBucketCommand).rejects({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(PutObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(DeleteObjectCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      await mockS3Client.putObject({
        Bucket: 'bucketName',
        Key: 'key',
        Body: Buffer.from('value'),
      });
      await mockS3Client.closeTransaction();

      expect(s3Mock).toHaveReceivedCommandWith(DeleteBucketCommand, {
        Bucket: 'hedwig-backups',
      });
    });
  });

  describe('In memory strategy', () => {
    it('Checking .deleteBucket() MEMORY - should delete a bucket and restore in upon rollback', async () => {
      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);

      s3Mock.on(ListObjectsCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Contents: [
          {
            Key: 'key',
          },
        ],
      });
      s3Mock.on(GetObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Body: sdkStreamMixin(mockStream),
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(PutObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      await mockS3Client.deleteBucket(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(DeleteBucketCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(GetObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CreateBucketCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        Body: expect.any(Uint8Array),
      });
    });

    it('Checking .createBucket Memory - should create a bucket and delete it upon rollback', async () => {
      s3Mock.on(HeadBucketCommand).rejects({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      await mockS3Client.createBucket(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CreateBucketCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(DeleteBucketCommand, params);
    });

    it('Checking .deleteObject() Memory - should delete the object and restore it upon rollback', async () => {
      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);

      s3Mock.on(ListObjectsCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Contents: [
          {
            Key: 'key',
          },
        ],
      });
      s3Mock.on(GetObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Body: sdkStreamMixin(mockStream),
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(PutObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY,
        'hedwig-backups'
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      await mockS3Client.deleteObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(GetObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        Body: expect.any(Uint8Array),
      });
    });

    it('Checking .putObject() Memory - Object exists - should set the new file and restore the old value upon rollback', async () => {
      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);

      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(CopyObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      s3Mock.on(PutObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });
      s3Mock.on(GetObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
        Body: sdkStreamMixin(mockStream),
      });
      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY,
        'hedwig-backups'
      );
      const params: S3ObjectParams = {
        Bucket: 'bucketName',
        Key: 'key',
        Body: sdkStreamMixin(mockStream) as any,
      };

      await mockS3Client.putObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(GetObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        Body: expect.any(Readable),
      });
    });

    it('Checking .putObject() Memory - Object doesnt exists - should set the new file and delete it upon rollback', async () => {
      s3Mock.on(HeadObjectCommand).rejects({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });

      s3Mock.on(PutObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);
      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY,
        'hedwig-backups'
      );
      const params: S3ObjectParams = {
        Bucket: 'bucketName',
        Key: 'key',
        Body: sdkStreamMixin(mockStream) as any,
      };

      await mockS3Client.putObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
      });
    });
  });
});
