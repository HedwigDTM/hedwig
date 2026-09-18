import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client as AWSClient,
} from '@aws-sdk/client-s3';
import { S3ObjectParams } from '../../types/s3';
import { S3BackupError, S3RestoreError } from '../../errors';
import { S3RollBackStrategy } from '../S3RollbackStrategy';

/**
 * Keeps byte-level backups of S3 objects in process memory.
 *
 * Intended for small objects and short transactions: memory use grows with
 * the size of every backed-up object, and object metadata (Content-Type,
 * tags, ...) is not preserved on restore.
 */
export class InMemoryStrategy extends S3RollBackStrategy {
  private backupFiles: Map<string, Map<string, Uint8Array>> = new Map();

  constructor(_connection: AWSClient) {
    super(_connection);
    this.backupFiles = new Map();
  }

  public async backupFile(params: S3ObjectParams): Promise<void> {
    const { Bucket, Key } = params;

    try {
      const data = await this.connection.send(
        new GetObjectCommand({ Bucket, Key })
      );
      if (!data.Body) {
        throw new S3BackupError(`No data found in the S3 object: ${Key}`);
      }

      let files = this.backupFiles.get(Bucket);
      if (!files) {
        files = new Map<string, Uint8Array>();
        this.backupFiles.set(Bucket, files);
      }
      files.set(Key, await data.Body.transformToByteArray());
    } catch (error) {
      if (error instanceof S3BackupError) {
        throw error;
      }
      throw new S3BackupError(`Failed to backup file from S3: ${error}`);
    }
  }

  public async restoreFile(params: S3ObjectParams): Promise<void> {
    const { Bucket, Key } = params;
    const objectBackup = this.backupFiles.get(Bucket)?.get(Key);
    if (!objectBackup) {
      throw new S3RestoreError('No backup data found for the specified file');
    }

    try {
      await this.connection.send(
        new PutObjectCommand({ Bucket, Key, Body: objectBackup })
      );
    } catch (error) {
      throw new S3RestoreError(`Failed to restore file to S3: ${error}`);
    }
  }

  public async closeTransaction(): Promise<void> {
    this.backupFiles.clear();
  }
}
