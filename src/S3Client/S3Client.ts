import RollbackableClient from '../RollbackableClient/RollbackableClient';
import {
  DeleteObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client as AWSClient,
  CreateBucketConfiguration,
  CreateBucketCommand,
  DeleteBucketCommand,
  GetObjectCommand,
  GetObjectCommandOutput,
  ListBucketsCommandOutput,
  ListBucketsCommand,
  ListBucketsCommandInput,
  HeadBucketCommand,
  HeadObjectCommandOutput,
  HeadBucketCommandOutput,
  PutObjectCommandOutput,
  DeleteObjectCommandOutput,
  CreateBucketCommandOutput,
  DeleteBucketCommandOutput,
} from '@aws-sdk/client-s3';
import { S3RollbackFactory } from './S3RollbackFactory';
import { S3RollbackStrategyType } from '../Types/S3/S3RollBackStrategy';
import { S3RollBackStrategy } from './S3RollbackStrategy';

export interface S3ObjectParams {
  Bucket: string;
  Key: string;
  Body?: Buffer;
}

export interface S3BucketParams {
  Bucket: string;
  CreateBucketConfiguration?: CreateBucketConfiguration;
}
/**
 * The `S3RollbackClient` class is responsible for handling interactions with S3 while ensuring rollback capabilities
 * for each action.
 */
export class S3RollbackClient extends RollbackableClient {
  private connection: AWSClient;
  private rollbackStrategy: S3RollBackStrategy;

  /**
   * Constructs a new `S3RollbackClient`.
   *
   * @param {string} transactionID - Unique identifier for the transaction.
   * @param {AWSClient} connection - The AWS S3 client used for communicating with S3.
   * @param {S3RollbackStrategyType} rollbackStrategyType - Type of rollback strategy to be used for S3 actions.
   */
  constructor(
    transactionID: string,
    connection: AWSClient,
    rollbackStrategyType: S3RollbackStrategyType,
    backupBucketName?: string
  ) {
    super(transactionID);
    this.connection = connection;
    this.rollbackStrategy = S3RollbackFactory(
      this.connection,
      rollbackStrategyType,
      backupBucketName
    );
  }

  /**
   * Uploads an object to the specified S3 bucket and stores a rollback action.
   *
   * If the object already exists, it creates a backup using the defined rollback strategy. Otherwise, it stores
   * a rollback action to delete the newly created object in case of failure.
   *
   * @param {S3ObjectParams} params - The parameters for the S3 `putObject` command (Bucket, Key, Body, etc.).
   * @returns {Promise<PutObjectCommandOutput>} A promise that resolves with the result of the `putObject` command.
   */
  public async putObject(
    params: S3ObjectParams
  ): Promise<PutObjectCommandOutput> {
    let objExisted = false;

    try {
      await this.connection.send(new HeadObjectCommand(params));
      objExisted = true;
      await this.rollbackStrategy.backupFile(params);
    } catch (error) {
      // Object doesn't exist, continue with put operation
    }

    const result = await this.connection.send(new PutObjectCommand(params));

    const rollbackAction = async () => {
      if (objExisted) {
        await this.rollbackStrategy.restoreFile(params);
      } else {
        await this.connection.send(new DeleteObjectCommand(params));
      }
    };

    this.rollbackActions.push(rollbackAction);

    return result;
  }

  /**
   * Deletes an object from the specified S3 bucket and stores a rollback action.
   *
   * A backup is created before deleting the object. In case of failure, the rollback action restores the backup.
   *
   * @param {S3ObjectParams} params - The parameters for the S3 `deleteObject` command (Bucket, Key, etc.).
   * @returns {Promise<DeleteObjectCommandOutput>} A promise that resolves with the result of the `deleteObject` command.
   */
  public async deleteObject(
    params: S3ObjectParams
  ): Promise<DeleteObjectCommandOutput> {
    await this.rollbackStrategy.backupFile(params);
    const result = await this.connection.send(new DeleteObjectCommand(params));

    const rollbackAction = async () => {
      await this.rollbackStrategy.restoreFile(params);
    };

    this.rollbackActions.push(rollbackAction);

    return result;
  }

  /**
   * Creates a new S3 bucket and stores a rollback action.
   *
   * @param {S3BucketParams} params - The parameters for the S3 `createBucket` command (Bucket, etc.).
   * @returns {Promise<CreateBucketCommandOutput>} A promise that resolves with the result of the `createBucket` command.
   */
  public async createBucket(
    params: S3BucketParams
  ): Promise<CreateBucketCommandOutput> {
    let bucketExists = false;
    try {
      await this.connection.send(new HeadBucketCommand(params));
      bucketExists = true;
    } catch (error) {
      // Bucket doesn't exist
    }

    const result = await this.connection.send(new CreateBucketCommand(params));

    const rollbackAction = !bucketExists
      ? async () => {
          await this.connection.send(new DeleteBucketCommand(params));
        }
      : async () => {};

    this.rollbackActions.push(rollbackAction);

    return result;
  }

  /**
   * Deletes an S3 bucket and stores a rollback action.
   *
   * @param {S3BucketParams} params - The parameters for the S3 `deleteBucket` command (Bucket, etc.).
   * @returns {Promise<DeleteBucketCommandOutput>} A promise that resolves with the result of the `deleteBucket` command.
   */
  public async deleteBucket(
    params: S3BucketParams
  ): Promise<DeleteBucketCommandOutput> {
    await this.rollbackStrategy.backupBucket(params);
    const result = await this.connection.send(new DeleteBucketCommand(params));

    const rollbackAction = async () => {
      await this.rollbackStrategy.restoreBucket(params);
    };

    this.rollbackActions.push(rollbackAction);

    return result;
  }

  /**
   * Retrieves an object from the specified S3 bucket.
   *
   * @param {S3ObjectParams} params - The parameters for the S3 `getObject` command (Bucket, Key, etc.).
   * @returns {Promise<GetObjectCommandOutput>} A promise that resolves with the object data.
   */
  public async getObject(
    params: S3ObjectParams
  ): Promise<GetObjectCommandOutput> {
    return await this.connection.send(new GetObjectCommand(params));
  }

  /**
   * Lists all S3 buckets with pagination support.
   *
   * @param {ListBucketsCommandInput} params - The parameters for the S3 `listBuckets` command.
   * @param {string} [continuationToken] - Token used for pagination to retrieve the next set of buckets.
   * @returns {Promise<ListBucketsCommandOutput>} A promise that resolves with the list of buckets.
   */
  public async listBuckets(
    params: ListBucketsCommandInput,
    continuationToken?: string
  ): Promise<ListBucketsCommandOutput> {
    const commandParams = continuationToken 
      ? { ...params, ContinuationToken: continuationToken } 
      : params;
    
    return await this.connection.send(new ListBucketsCommand(commandParams));
  }

  /**
   * Retrieves the metadata of an object from the specified S3 bucket.
   *
   * @param {S3ObjectParams} params - The parameters for the S3 `headObject` command (Bucket, Key, etc.).
   * @returns {Promise<HeadObjectCommandOutput>} A promise that resolves with the metadata of the object.
   */
  public async headObject(
    params: S3ObjectParams
  ): Promise<HeadObjectCommandOutput> {
    return await this.connection.send(new HeadObjectCommand(params));
  }

  /**
   * Retrieves the metadata of a bucket.
   *
   * @param {S3BucketParams} params - The parameters for the S3 `headBucket` command (Bucket, etc.).
   * @returns {Promise<HeadBucketCommandOutput>} A promise that resolves with the metadata of the bucket.
   */
  public async headBucket(
    params: S3BucketParams
  ): Promise<HeadBucketCommandOutput> {
    return await this.connection.send(new HeadBucketCommand(params));
  }

  public async closeTransaction(): Promise<void> {
    await this.rollbackStrategy.closeTransaction();
  }
}
