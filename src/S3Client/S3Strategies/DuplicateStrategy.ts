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
    const { Bucket, Key } = params;

    try {
      await this.connection.send(
        new CopyObjectCommand({
          Bucket: this.backupsBucketName,
          Key: `${Key}-backup`,
          CopySource: `${Bucket}/${Key}`,
        })
      );
    } catch {
      throw new S3BackupError();
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
   * @returns {Promise<void>}
   */
  public async backupBucket(params: S3BucketParams): Promise<void> {
    const { Bucket } = params;

    try {
      const listResponse = await this.connection.send(
        new ListObjectsCommand(params)
      );

      if (listResponse.Contents) {
        throw new S3BackupError('No objects found in the bucket');
      }

      await this.connection.send(
        new CreateBucketCommand({
          Bucket: `${this.backupsBucketName}-${Bucket}`,
        })
      );

      for (const object of listResponse.Contents!) {
        await this.connection.send(
          new CopyObjectCommand({
            Bucket: `${this.backupsBucketName}-${Bucket}`,
            Key: object.Key!,
            CopySource: `${Bucket}/${object.Key}`,
          })
        );
      }
    } catch {
      throw new S3BackupError();
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

      if (listResponse.Contents) {
        throw new S3RestoreError('No objects found in the backup bucket');
      }

      for (const object of listResponse.Contents!) {
        await this.connection.send(
          new CopyObjectCommand({
            Bucket: params.Bucket,
            Key: object.Key!,
            CopySource: `${this.backupsBucketName}-${Bucket}/${object.Key}`,
          })
        );
      }

      await this.connection.send(
        new DeleteObjectCommand({
          Bucket: `${this.backupsBucketName}-${Bucket}`,
          Key: '',
        })
      );
    } catch {
      throw new S3RestoreError();
    }
  }

  private async createBackupBucket(): Promise<void> {
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
        throw new S3BackupError();
      }
    }
  }
}
