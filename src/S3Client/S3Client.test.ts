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
  let s3Mock = mockClient(AWSClient);
  let connection: AWSClient;

  beforeEach(() => {
    s3Mock.reset();
    connection = new AWSClient({});
  });

  it('Checking .headBucket()', async () => {
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

    await expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, params);
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

    await expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
  });

  it('Checking .listBuckets()', async () => {
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

    await expect(s3Mock).toHaveReceivedCommandWith(ListBucketsCommand, params);
  });

  it('Checking .getObject()', async () => {
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

    await expect(s3Mock).toHaveReceivedCommandWith(GetObjectCommand, params);
  });

  it('Checking .deleteBucket() - DUPLICATE ', async () => {
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

    expect(s3Mock).toHaveReceivedCommandWith(CreateBucketCommand, {
      Bucket: 'Hedwig-Backups-bucketName',
    });
    await expect(s3Mock).toHaveReceivedCommandWith(ListObjectsCommand, {
      Bucket: 'bucketName',
    });
    await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
      Bucket: 'Hedwig-Backups-bucketName',
      Key: 'key',
      CopySource: 'bucketName/key',
    })
    await expect(s3Mock).toHaveReceivedCommandWith(DeleteBucketCommand, params);
    await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
      Bucket: 'bucketName',
      Key: 'key',
      CopySource: 'Hedwig-Backups-bucketName/key',
    })
  });

  it('Checking .createBucket', async () => {
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

    await expect(s3Mock).toHaveReceivedCommandWith(CreateBucketCommand, params);
    await expect(s3Mock).toHaveReceivedCommandWith(DeleteBucketCommand, params);
  });

  it('Checking .deleteObject() - DUPLICATE', async () => {
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

    await expect(s3Mock).toHaveReceivedCommandWith(HeadBucketCommand, {
      Bucket: 'Hedwig-Backups',
    });
    await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
      Bucket: 'Hedwig-Backups',
      Key: 'key-backup',
      CopySource: 'bucketName/key',
    });
    await expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, params);
    await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
      Bucket: 'bucketName',
      Key: 'key',
      CopySource: 'Hedwig-Backups/key-backup',
    });
    await expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
      Bucket: 'Hedwig-Backups',
      Key: 'key-backup',
    });
  });

  it('Checking .putObject() - DUPLICATE - Object exists', async () => {
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

    await expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
    await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
      Bucket: 'Hedwig-Backups',
      Key: 'key-backup',
      CopySource: 'bucketName/key',
    });
    await expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
    await expect(s3Mock).toHaveReceivedCommandWith(CopyObjectCommand, {
      Bucket: 'bucketName',
      Key: 'key',
      CopySource: 'Hedwig-Backups/key-backup',
    });
    await expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
      Bucket: 'Hedwig-Backups',
      Key: 'key-backup',
    });
  });

  it('Checking .putObject() - DUPLICATE - Object doesnt exists', async () => {
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

    await expect(s3Mock).toHaveReceivedCommandWith(HeadObjectCommand, params);
    await expect(s3Mock).toHaveReceivedCommandWith(PutObjectCommand, params);
    await expect(s3Mock).toHaveReceivedCommandWith(DeleteObjectCommand, {
      Bucket: 'bucketName',
      Key: 'key',
    });
  });
});
