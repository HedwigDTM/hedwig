import {
  S3Client as AWSClient,
  CopyObjectCommand,
  CreateBucketCommand,
  DeleteBucketCommand,
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
import { PassThrough, Readable } from 'stream';

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

  it('Checking .deleteBucket() - IN MEMORY ', async () => {
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

    const mockStream = new PassThrough();
    mockStream.write('mock data');
    mockStream.end();
    s3Mock.on(GetObjectCommand).resolves({
      $metadata: {
        httpStatusCode: 200,
      },
      Body: mockStream as any,
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

    s3Mock.on(CreateBucketCommand).resolves({
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

    await expect(s3Mock).toHaveReceivedCommandWith(ListObjectsCommand, params);
    await expect(s3Mock).toHaveReceivedCommandTimes(GetObjectCommand, 1);
    await expect(s3Mock).toHaveReceivedCommandWith(DeleteBucketCommand, params);
    await expect(s3Mock).toHaveReceivedCommandTimes(PutObjectCommand, 1);
    await expect(s3Mock).toHaveReceivedCommandTimes(CreateBucketCommand, 1);
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
    await expect(s3Mock).toHaveReceivedCommandTimes(ListObjectsCommand, 2);
    await expect(s3Mock).toHaveReceivedCommandTimes(DeleteBucketCommand, 1);
    await expect(s3Mock).toHaveReceivedCommandTimes(CopyObjectCommand, 2);
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
});
