import { S3Params } from "../S3Client";
import { S3BackupError, S3RestoreError } from "../S3RollbackFactory";
import { S3Startegy } from "../S3RollbackStrategy";
import {
  CopyObjectCommand,
  DeleteObjectCommand,
  S3Client as AWSClient
} from "@aws-sdk/client-s3";

export class DuplicateStrategy extends S3Startegy {
  private backupsBucket: string = "Hedwig-Backups";

  constructor(_connection: AWSClient) {
    super(_connection);
  }

  /**
   * Backs up the current version of an S3 object by duplicating it to a backup bucket.
   * @param {S3Params} params - Parameters for the backup operation.
   * @returns {Promise<void>}
   */
  public async backup(params: S3Params): Promise<void> {
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
  public async restore(params: S3Params): Promise<void> {
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
}
