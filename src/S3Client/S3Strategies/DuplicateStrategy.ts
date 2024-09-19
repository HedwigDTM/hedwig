import { S3BucketParams, S3ObjectParams } from "../S3Client";
import { S3BackupError, S3RestoreError } from "../S3RollbackFactory";
import { S3RollBackStrategy } from "../S3RollbackStrategy";
import {
  CopyObjectCommand,
  DeleteObjectCommand,
  S3Client as AWSClient,
  ListObjectsCommand,
  CreateBucketCommand,
  HeadBucketCommand,
} from "@aws-sdk/client-s3";

export class DuplicateStrategy extends S3RollBackStrategy {
  private backupsBucket: string = 'Hedwig-Backups';

  constructor(_connection: AWSClient) {
    super(_connection);
    this.createBackupBucket();
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
          Bucket: this.backupsBucket,
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
          CopySource: `${this.backupsBucket}/${Key}-backup`,
        })
      );

      await this.connection.send(
        new DeleteObjectCommand({
          Bucket: this.backupsBucket,
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
        throw new S3BackupError("No objects found in the bucket");
      }

      await this.connection.send(
        new CreateBucketCommand({ Bucket: `${this.backupsBucket}-${Bucket}` })
      );

      for (const object of listResponse.Contents!) {
        await this.connection.send(
          new CopyObjectCommand({
            Bucket: `${this.backupsBucket}-${Bucket}`,
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
        new ListObjectsCommand({ Bucket: `${this.backupsBucket}-${Bucket}` })
      );

      if (listResponse.Contents) {
        throw new S3RestoreError("No objects found in the backup bucket");
      }

      for (const object of listResponse.Contents!) {
        await this.connection.send(
          new CopyObjectCommand({
            Bucket: params.Bucket,
            Key: object.Key!,
            CopySource: `${this.backupsBucket}-${Bucket}/${object.Key}`,
          })
        );
      }

      await this.connection.send(
        new DeleteObjectCommand({
          Bucket: `${this.backupsBucket}-${Bucket}`,
          Key: "",
        })
      );
    } catch {
      throw new S3RestoreError();
    }
  }

  private async createBackupBucket(): Promise<void> {
    try {
      await this.connection.send(
        new HeadBucketCommand({ Bucket: this.backupsBucket })
      );
    } catch (error: any) {
      if (
        error.name === "NotFound" ||
        error.$metadata?.httpStatusCode === 404
      ) {
        await this.connection.send(
          new CreateBucketCommand({ Bucket: this.backupsBucket })
        );
      } else {
        throw new S3BackupError();
      }
    }
  }
}
