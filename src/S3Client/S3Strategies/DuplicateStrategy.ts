import { S3Params } from "../S3Client";
import { S3Startegy } from "../S3RollbackStrategy";
import AWS, { CopyObjectCommand } from "@aws-sdk/client-s3";
import { S3BackupError, S3RestoreError } from "../S3RollbackStrategy";

export class DuplicateStrategy extends S3Startegy {
    private backupsBucket: string = 'Hedwig-Backups';

    constructor(_connection: AWS.S3Client) {
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
        await this.connection.send(new CopyObjectCommand({
            Bucket: Bucket,
            Key: Key,
            CopySource: `${this.backupsBucket}/${Key}-backup`,
        }));
    } catch {
      throw new S3RestoreError();
    }
  }
}
