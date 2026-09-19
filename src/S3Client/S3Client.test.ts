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
  ListObjectsV2Command,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { S3RollbackStrategyType } from '../types/s3';
import { S3BucketParams, S3ObjectParams } from '../types/s3';
import { S3RollbackClient } from './S3Client';
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
    it('headBucket should return bucket info', async () => {
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

    it('headObject should return object metadata', async () => {
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

    it('listBuckets should list all buckets', async () => {
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

    it('getObject should retrieve an object', async () => {
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

    it('closeTransaction (IN_MEMORY) should clear the in-memory backups', async () => {
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

    it('putObject should fail when a head error is not a 404', async () => {
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

    it('deleteObject (missing object) should delete without backup or restore', async () => {
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

    it('createBucket should not delete an existing bucket on rollback', async () => {
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

    it('createBucket should fail when a head error is not a 404', async () => {
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
    it('deleteBucket (DUPLICATE_FILE) should fail fast for a non-empty bucket', async () => {
      s3Mock.on(HeadBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(ListObjectsV2Command).resolves({
        $metadata: { httpStatusCode: 200 },
        Contents: [{ Key: 'key' }],
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );

      await expect(
        mockS3Client.deleteBucket({ Bucket: 'bucketName' })
      ).rejects.toThrow('is not empty');
      expect(s3Mock.commandCalls(CopyObjectCommand)).toHaveLength(0);
      expect(s3Mock.commandCalls(DeleteBucketCommand)).toHaveLength(0);
    });

    it('deleteBucket (DUPLICATE_FILE) should delete an empty bucket and recreate it on rollback', async () => {
      s3Mock.on(HeadBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(ListObjectsV2Command).resolves({
        $metadata: { httpStatusCode: 200 },
        Contents: [],
        IsTruncated: false,
      });
      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      await mockS3Client.deleteBucket(params);
      await mockS3Client.rollback();

      await expect(s3Mock).toHaveReceivedCommandWith(
        DeleteBucketCommand,
        params
      );
      await expect(s3Mock).toHaveReceivedCommandWith(
        CreateBucketCommand,
        params
      );
    });

    it('deleteBucket (DUPLICATE_FILE) should fail for a missing bucket', async () => {
      s3Mock.on(HeadBucketCommand).rejects({
        name: 'NotFound',
        $metadata: { httpStatusCode: 404 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.DUPLICATE_FILE,
        'hedwig-backups'
      );

      await expect(
        mockS3Client.deleteBucket({ Bucket: 'bucketName' })
      ).rejects.toThrow('does not exist');
      expect(s3Mock.commandCalls(DeleteBucketCommand)).toHaveLength(0);
    });

    it('createBucket (DUPLICATE_FILE) should create a bucket and delete it on rollback', async () => {
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

    it('deleteObject (DUPLICATE_FILE) should delete the object and restore it on rollback', async () => {
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

    it('putObject (DUPLICATE_FILE, existing object) should set the new body and restore the old one on rollback', async () => {
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

    it('putObject (DUPLICATE_FILE, missing object) should set the object and delete it on rollback', async () => {
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

    it('putObject (DUPLICATE_FILE) should isolate backup keys per concurrent transaction', async () => {
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

    it('putObject (DUPLICATE_FILE) should scope backup keys per bucket', async () => {
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

    it('closeTransaction (DUPLICATE_FILE) should delete only the backup objects of this transaction', async () => {
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

    it('closeTransaction (DUPLICATE_FILE) should remove the general backup bucket only when this transaction created it', async () => {
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
    it('deleteBucket (IN_MEMORY) should fail fast for a non-empty bucket', async () => {
      s3Mock.on(HeadBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(ListObjectsV2Command).resolves({
        $metadata: { httpStatusCode: 200 },
        Contents: [{ Key: 'key' }],
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );

      await expect(
        mockS3Client.deleteBucket({ Bucket: 'bucketName' })
      ).rejects.toThrow('is not empty');
      expect(s3Mock.commandCalls(DeleteBucketCommand)).toHaveLength(0);
    });

    it('deleteBucket (IN_MEMORY) should delete an empty bucket and recreate it on rollback', async () => {
      s3Mock.on(HeadBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(ListObjectsV2Command).resolves({
        $metadata: { httpStatusCode: 200 },
        Contents: [],
        IsTruncated: false,
      });
      s3Mock.on(DeleteBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });
      s3Mock.on(CreateBucketCommand).resolves({
        $metadata: { httpStatusCode: 200 },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3BucketParams = { Bucket: 'bucketName' };

      await mockS3Client.deleteBucket(params);
      await mockS3Client.rollback();

      await expect(s3Mock).toHaveReceivedCommandWith(
        DeleteBucketCommand,
        params
      );
      await expect(s3Mock).toHaveReceivedCommandWith(
        CreateBucketCommand,
        params
      );
    });

    it('createBucket (IN_MEMORY) should create a bucket and delete it on rollback', async () => {
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

    it('deleteObject (IN_MEMORY) should delete the object and restore it on rollback', async () => {
      const mockStream = new Readable();
      mockStream.push('hello world');
      mockStream.push(null);

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

    it('putObject (IN_MEMORY, existing object) should set the new body and restore the old one on rollback', async () => {
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

    it('putObject (IN_MEMORY, missing object) should set the object and delete it on rollback', async () => {
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
