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

  describe('Geneal orations', () => {
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

      await mockS3Client.headBucket(params);

      expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, params);
    });

    it('Checking .headObject()', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      await mockS3Client.headObject(params);

      expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
    });

    it('Checking .listBuckets() - should list the buckets', async () => {
      s3Mock.on(ListBucketsCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: ListBucketsCommandInput = {};

      await mockS3Client.listBuckets(params);

      expect(s3Mock).toHaveReceivedCommandWith(ListBucketsCommand, params);
    });

    it('Checking .getObject() Memory - should get an object', async () => {
      s3Mock.on(HeadObjectCommand).resolves({
        $metadata: {
          httpStatusCode: 200,
        },
      });

      const mockS3Client = new S3RollbackClient(
        'test',
        connection,
        S3RollbackStrategyType.IN_MEMORY
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      await mockS3Client.getObject(params);

      expect(s3Mock).toHaveReceivedCommandWith(GetObjectCommand, params);
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
        Bucket: 'Hedwig-Backups-bucketName',
      });
      await expect(s3Mock).toHaveReceivedCommandWith(ListObjectsCommand, {
        Bucket: 'bucketName',
      });
      await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'Hedwig-Backups-bucketName',
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
        CopySource: 'Hedwig-Backups-bucketName/key',
      });
    });

    it('Checking .createBucket - should create a bucket and delete it upon rollback', async () => {
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
        'Hedwig-Backups'
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      await mockS3Client.deleteObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, {
        Bucket: 'Hedwig-Backups',
      });
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'Hedwig-Backups',
        Key: 'key-backup',
        CopySource: 'bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        CopySource: 'Hedwig-Backups/key-backup',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'Hedwig-Backups',
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
        'Hedwig-Backups'
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
        Bucket: 'Hedwig-Backups',
        Key: 'key-backup',
        CopySource: 'bucketName/key',
      });
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        CopySource: 'Hedwig-Backups/key-backup',
      });
      expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
        Bucket: 'Hedwig-Backups',
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
        'Hedwig-Backups'
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
      // Mock S3 Commands

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
        Body: Buffer.from('hello world'),
      });
    });

    it('Checking .createBucket Memory - should create a bucket and delete it upon rollback', async () => {
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
        'Hedwig-Backups'
      );
      const params: S3ObjectParams = { Bucket: 'bucketName', Key: 'key' };

      await mockS3Client.deleteObject(params);
      await mockS3Client.rollback();

      expect(s3Mock).toHaveReceivedCommandWith(GetObjectCommand, params);
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, {
        Bucket: 'bucketName',
        Key: 'key',
        Body: Buffer.from('hello world'),
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
        'Hedwig-Backups'
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
      expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
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
        'Hedwig-Backups'
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
