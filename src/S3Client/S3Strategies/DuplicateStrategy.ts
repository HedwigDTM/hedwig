import { S3BucketParams, S3ObjectParams } from '../S3Client';
import { S3BackupError, S3RestoreError } from '../S3RollbackFactory';
import { S3RollBackStrategy } from '../S3RollbackStrategy';
import {
  CopyObjectCommand,
  DeleteObjectCommand,
  S3Client as AWSClient,
  ListObjectsCommand,
  CreateBucketCommand,
  HeadBucketCommand,
  DeleteBucketCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  NotFound,
} from '@aws-sdk/client-s3';

export class DuplicateStrategy extends S3RollBackStrategy {
  private backupsBucketName: string;

  constructor(_connection: AWSClient, backupsBucketName: string) {
    super(_connection);
    this.backupsBucketName = backupsBucketName;
  }

  /**
   * Backs up the current version of an S3 object by duplicating it to a backup bucket.
   * @param {S3Params} params - Parameters for the backup operation.
   * @returns {Promise<void>}
   */
  public async backupFile(params: S3ObjectParams): Promise<void> {
    await this.createBackupBucket();

    const { Bucket, Key } = params;
    try {
      await this.connection.send(
        new CopyObjectCommand({
          Bucket: this.backupsBucketName,
          Key: `${Key}-backup`,
          CopySource: `${Bucket}/${Key}`,
        })
      );
    } catch (error) {
      console.error(error);
      throw error;
    }
  }

  /**
   * Restores the latest version of an S3 object from the backup bucket to the original bucket.
   * @param {S3Params} params - Parameters for the restore operation.
   * @returns {Promise<void>}
   */
  public async restoreFile(params: S3ObjectParams): Promise<void> {
    const { Bucket, Key } = params;

    try {
      await this.connection.send(
        new CopyObjectCommand({
          Bucket: Bucket,
          Key: Key,
          CopySource: `${this.backupsBucketName}/${Key}-backup`,
        })
      );

      await this.connection.send(
        new DeleteObjectCommand({
          Bucket: this.backupsBucketName,
          Key: `${Key}-backup`,
        })
      );
    } catch {
      throw new S3RestoreError();
    }
  }

  /**
   * Backs up the current version of an S3 bucket by duplicating it to a backup bucket.
   * @param {S3Params} params - Parameters for the backup operation.
   * @returns {Promise<string> - The name of the backup bucket.}
   */
  public async backupBucket(params: S3BucketParams): Promise<string> {
    const { Bucket } = params;
    try {
      const listResponse = await this.connection.send(
        new ListObjectsCommand(params)
      );

      const bucketName = `${this.backupsBucketName}-${Bucket}`;
      if (!listResponse.Contents) {
        throw new S3BackupError('No objects found in the bucket');
      }

      await this.connection.send(
        new CreateBucketCommand({
          Bucket: bucketName,
        })
      );

      for (const object of listResponse.Contents) {
        await this.connection.send(
          new CopyObjectCommand({
            Bucket: bucketName,
            Key: object.Key!,
            CopySource: `${Bucket}/${object.Key}`,
          })
        );
      }
      return bucketName;
    } catch (error) {
      console.error(error);
      throw error;
    }
  }

  public async closeTransaction(): Promise<void> {
    await this.purgeBucket(this.backupsBucketName);
  }
  /**
   * Purges the specified bucket by deleting all its objects and then the bucket itself.
   * @param {string} bucketName - The name of the bucket to purge.
   * @returns {Promise<void>} A promise that resolves once the bucket is purged.
   */
  async purgeBucket(bucketName: string): Promise<void> {
    try {
      try {
        await this.connection.send(
          new HeadBucketCommand({ Bucket: bucketName })
        );
      } catch (error: any) {
        if (error?.name === 'NotFound') {
          return;
        }
        throw error;
      }

      let isTruncated = true;
      let continuationToken: string | undefined = undefined;

      while (isTruncated) {
        const listObjectsCommand: ListObjectsV2Command =
          new ListObjectsV2Command({
            Bucket: bucketName,
            ContinuationToken: continuationToken,
          });

        const listResponse = await this.connection.send(listObjectsCommand);

        if (listResponse.Contents && listResponse.Contents.length > 0) {
          const deleteObjectsCommand = new DeleteObjectsCommand({
            Bucket: bucketName,
            Delete: {
              Objects: listResponse.Contents.map((obj) => ({ Key: obj.Key })),
            },
          });

          await this.connection.send(deleteObjectsCommand);
        }

        // Check if there are more objects to delete
        isTruncated = listResponse.IsTruncated || false;
        continuationToken = listResponse.NextContinuationToken;
      }
      await this.connection.send(
        new DeleteBucketCommand({
          Bucket: bucketName,
        })
      );
    } catch (error) {
      console.error('Error deleting files:', error);
      throw error;
    }
  }

  /**
   * Restores the latest version of an S3 bucket from the backup bucket to the original bucket.
   * @param {S3Params} params - Parameters for the restore operation.
   * @returns {Promise<void>}
   */
  public async restoreBucket(params: S3BucketParams): Promise<void> {
    const { Bucket } = params;

    try {
      await this.connection.send(new CreateBucketCommand(params));

      const listResponse = await this.connection.send(
        new ListObjectsCommand({
          Bucket: `${this.backupsBucketName}-${Bucket}`,
        })
      );

      if (!listResponse.Contents) {
        throw new S3RestoreError('No objects found in the backup bucket');
      }

      for (const object of listResponse.Contents) {
        await this.connection.send(
          new CopyObjectCommand({
            Bucket: params.Bucket,
            Key: object.Key!,
            CopySource: `${this.backupsBucketName}-${Bucket}/${object.Key}`,
          })
        );
      }
    } catch {
      throw new S3RestoreError();
    }
  }

  public async createBackupBucket(): Promise<void> {
    try {
      await this.connection.send(
        new HeadBucketCommand({ Bucket: this.backupsBucketName })
      );
    } catch (error: any) {
      if (
        error.name === 'NotFound' ||
        error.$metadata?.httpStatusCode === 404
      ) {
        await this.connection.send(
          new CreateBucketCommand({ Bucket: this.backupsBucketName })
        );
      } else {
        throw error;
      }
    }
  }
}
