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
  ListObjectsCommandInput,
  _Object
} from '@aws-sdk/client-s3';

export class DuplicateStrategy extends S3RollBackStrategy {
  private backupsBucketName: string;
  private isGeneralBackupBucketCreated: boolean = false;

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
    if (!this.isGeneralBackupBucketCreated) {
      await this.createGeneralBackupBucket();
    }

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
      throw new S3BackupError(`Failed to backup file: ${error}`);
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
    } catch (error) {
      throw new S3RestoreError(`Failed to restore file: ${error}`);
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
      let marker: string | undefined;

      // Create backup bucket first
      await this.connection.send(
        new CreateBucketCommand({
          Bucket: `${this.backupsBucketName}-${Bucket}`,
        })
      );

      do {
        const listResponse = await this.connection.send(
          new ListObjectsCommand({
            Bucket,
            Marker: marker,
          } as ListObjectsCommandInput)
        );

        if (!listResponse.Contents) {
          throw new S3BackupError('No objects found in the bucket');
        }

        // Process current batch of objects in parallel
        await Promise.all(
          listResponse.Contents.map((object) =>
            this.connection.send(
              new CopyObjectCommand({
                Bucket: `${this.backupsBucketName}-${Bucket}`,
                Key: object.Key!,
                CopySource: `${Bucket}/${object.Key}`,
              })
            )
          )
        );

        marker = listResponse.NextMarker;
      } while (marker);
    } catch (error) {
      throw new S3BackupError(`Failed to backup bucket: ${error}`);
    }
  }

  public async closeTransaction(): Promise<void> {
    try {
      let marker: string | undefined;
      const objectsToDelete: _Object[] = [];

      // List all objects in the backup bucket using continuation tokens
      do {
        const listObjectsResponse = await this.connection.send(
          new ListObjectsCommand({
            Bucket: this.backupsBucketName,
            Marker: marker,
          })
        );

        if (listObjectsResponse.Contents) {
          objectsToDelete.push(...listObjectsResponse.Contents);
        }

        marker = listObjectsResponse.NextMarker;
      } while (marker);

      // Delete all objects in the bucket
      if (objectsToDelete.length > 0) {
        await Promise.all(
          objectsToDelete.map((object) =>
            this.connection.send(
              new DeleteObjectCommand({
                Bucket: this.backupsBucketName,
                Key: object.Key,
              })
            )
          )
        );
      }

      // Delete the backup bucket itself
      await this.connection.send(
        new DeleteBucketCommand({
          Bucket: this.backupsBucketName,
        })
      );
    } catch (error: any) {
      // If the bucket doesn't exist, that's fine - we can ignore this error
      if (error.name === 'NoSuchBucket' || error.$metadata?.httpStatusCode === 404) {
        return;
      }
      // For any other error, rethrow it
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
      // Create restored bucket first
      await this.connection.send(new CreateBucketCommand(params));

      let marker: string | undefined;

      do {
        const listResponse = await this.connection.send(
          new ListObjectsCommand({
            Bucket: `${this.backupsBucketName}-${Bucket}`,
            Marker: marker,
          } as ListObjectsCommandInput)
        );

        if (!listResponse.Contents) {
          throw new S3RestoreError('No objects found in the backup bucket');
        }

        // Process current batch of objects in parallel
        await Promise.all(
          listResponse.Contents.map((object) =>
            this.connection.send(
              new CopyObjectCommand({
                Bucket: Bucket,
                Key: object.Key!,
                CopySource: `${this.backupsBucketName}-${Bucket}/${object.Key}`,
              })
            )
          )
        );

        marker = listResponse.NextMarker;
      } while (marker);
    } catch (error) {
      throw new S3RestoreError(`Failed to restore bucket: ${error}`);
    }
  }

  public async createGeneralBackupBucket(): Promise<void> {
    try {
      await this.connection.send(
        new HeadBucketCommand({ Bucket: this.backupsBucketName })
      );
      this.isGeneralBackupBucketCreated = true;
    } catch (error: any) {
      if (
        error.name === 'NotFound' ||
        error.$metadata?.httpStatusCode === 404
      ) {
        await this.connection.send(
          new CreateBucketCommand({ Bucket: this.backupsBucketName })
        );
        this.isGeneralBackupBucketCreated = true;
      } else {
        throw new S3BackupError(`Failed to create backup bucket: ${error}`);
      }
    }
  }
}