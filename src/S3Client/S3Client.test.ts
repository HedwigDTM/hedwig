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
      s3Mock.on(HeadBucketCommand).rejects(new Error('Bucket does not exist'));
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
        Key: 'key-backup',
        CopySource: 'bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        CopySource: 'hedwig-backups/key-backup',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'hedwig-backups',
        Key: 'key-backup',
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
        Key: 'key-backup',
        CopySource: 'bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        CopySource: 'hedwig-backups/key-backup',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'hedwig-backups',
        Key: 'key-backup',
      });
    });

    it('Checking .putObject() DUPLICATE - Object doesnt exists - should set the new file and delete it upon rollback', async () => {
      s3Mock.on(HeadObjectCommand).rejects();

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
      s3Mock.on(HeadBucketCommand).rejects(new Error('Bucket does not exist'));
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
      s3Mock.on(HeadObjectCommand).rejects();

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
