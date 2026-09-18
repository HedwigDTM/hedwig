import { S3ObjectParams } from '../../types/s3';
import { S3BackupError, S3RestoreError } from '../../errors';
import { S3RollBackStrategy } from '../S3RollbackStrategy';
import {
  CopyObjectCommand,
  DeleteObjectCommand,
  S3Client as AWSClient,
  CreateBucketCommand,
  HeadBucketCommand,
  DeleteBucketCommand,
} from '@aws-sdk/client-s3';

function isS3ServiceError(
  error: unknown
): error is { name?: string; $metadata?: { httpStatusCode?: number } } {
  return typeof error === 'object' && error !== null;
}
export class DuplicateStrategy extends S3RollBackStrategy {
  private backupsBucketName: string;
  private transactionID: string;
  private isGeneralBackupBucketCreated: boolean = false;
  private createdGeneralBackupBucket = false;
  private backupObjectKeys = new Set<string>();

  constructor(
    _connection: AWSClient,
    transactionID: string,
    backupsBucketName: string
  ) {
    super(_connection);
    this.backupsBucketName = backupsBucketName;
    this.transactionID = transactionID;
  }

  private backupKey(params: S3ObjectParams): string {
    return `${this.transactionID}/${params.Bucket}/${params.Key}`;
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
          Key: this.backupKey(params),
          CopySource: `${Bucket}/${Key}`,
        })
      );
      this.backupObjectKeys.add(this.backupKey(params));
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
          CopySource: `${this.backupsBucketName}/${this.backupKey(params)}`,
        })
      );

      await this.connection.send(
        new DeleteObjectCommand({
          Bucket: this.backupsBucketName,
          Key: this.backupKey(params),
        })
      );
    } catch (error) {
      throw new S3RestoreError(`Failed to restore file: ${error}`);
    }
  }

  public async closeTransaction(): Promise<void> {
    try {
      // Delete the backup objects created by this transaction
      await Promise.all(
        Array.from(this.backupObjectKeys).map((key) =>
          this.connection.send(
            new DeleteObjectCommand({
              Bucket: this.backupsBucketName,
              Key: key,
            })
          )
        )
      );

      // Remove the general backup bucket only if this transaction created it
      if (this.createdGeneralBackupBucket) {
        await this.connection.send(
          new DeleteBucketCommand({ Bucket: this.backupsBucketName })
        );
      }
    } catch (error: unknown) {
      // If the bucket doesn't exist, that's fine - we can ignore this error
      if (
        isS3ServiceError(error) &&
        (error.name === 'NoSuchBucket' ||
          error.$metadata?.httpStatusCode === 404)
      ) {
        return;
      }
      // For any other error, rethrow it
      throw error;
    }
  }

  public async createGeneralBackupBucket(): Promise<void> {
    try {
      await this.connection.send(
        new HeadBucketCommand({ Bucket: this.backupsBucketName })
      );
      this.isGeneralBackupBucketCreated = true;
    } catch (error: unknown) {
      if (
        isS3ServiceError(error) &&
        (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404)
      ) {
        await this.connection.send(
          new CreateBucketCommand({ Bucket: this.backupsBucketName })
        );
        this.isGeneralBackupBucketCreated = true;
        this.createdGeneralBackupBucket = true;
      } else {
        throw new S3BackupError(`Failed to create backup bucket: ${error}`);
      }
    }
  }
}
