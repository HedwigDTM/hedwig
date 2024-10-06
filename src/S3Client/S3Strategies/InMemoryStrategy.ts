import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client as AWSClient,
  ListObjectsCommand,
  CreateBucketCommand,
} from "@aws-sdk/client-s3";
import { S3BucketParams, S3ObjectParams } from "../S3Client";
import { S3RollBackStrategy } from "../S3RollbackStrategy";
import { Readable } from "stream";
import { S3BackupError, S3RestoreError } from "../S3RollbackFactory";

/**
 * Class to handle in-memory delete, backup and restore of S3 objects.
 * Implements the iS3Backuper interface.
 */
export class InMemoryStrategy extends S3RollBackStrategy {
  private inMemoryStorage?: Buffer | Map<string, Buffer>;

  constructor(_connection: AWSClient) {
    super(_connection);
    this.inMemoryStorage = undefined;
  }

  /**
   * Backs up the current version of an S3 object to in-memory storage.
   * @param {S3Params} params - Parameters for the backup operation.
   * @returns {Promise<void>}
   */
  public async backupFile(params: S3ObjectParams): Promise<void> {
    const { Bucket, Key } = params;

    try {
      const data = await this.connection.send(
        new GetObjectCommand({
          Bucket,
          Key,
        })
      );
      this.inMemoryStorage = await this.streamToBuffer(data.Body as Readable);
    } catch {
      throw new S3BackupError();
    }
  }

  /**
   * Restores the latest version of an S3 object from in-memory storage.
   * @param {S3Params} params - Parameters for the restore operation.
   * @returns {Promise<void>}
   */
  public async restoreFile(params: S3ObjectParams): Promise<void> {
    if (!this.inMemoryStorage) {
      throw new S3RestoreError('No backup found in inMemory storage');
    } else {
      try {
        const { Bucket, Key } = params;
        await this.connection.send(
          new PutObjectCommand({
            Bucket,
            Key,
            Body: this.inMemoryStorage as Buffer,
          })
        );
      } catch {
        throw new S3RestoreError();
      }
    }
  }

  /**
   * Backs up the current version of an S3 bucket to in-memory storage.
   * @param {S3Params} params - Parameters for the backup operation.
   * @returns {Promise<void>}
   */
  public async backupBucket(params: S3BucketParams): Promise<void> {
    try {
      const listResponse = await this.connection.send(
        new ListObjectsCommand(params)
      );

      if (!listResponse.Contents) {
        throw new S3BackupError("No objects found in the bucket");
      }

      this.inMemoryStorage = new Map<string, Buffer>();

      for (const obj of listResponse.Contents) {
        const data = await this.connection.send(
          new GetObjectCommand({
            Bucket: params.Bucket,
            Key: obj.Key,
          })
        );
        const buffer = await this.streamToBuffer(data.Body as Readable);

        (this.inMemoryStorage as Map<string, Buffer>).set(obj.Key!, buffer);
      }
    } catch {
      throw new S3BackupError();
    }
  }

  /**
   * Restores the latest version of an S3 bucket from in-memory storage.
   * @param {S3Params} params - Parameters for the restore operation.
   * @returns {Promise<void>}
   */
  public async restoreBucket(params: S3BucketParams): Promise<void> {
    if (!this.inMemoryStorage) {
      throw new S3RestoreError("No backup found in inMemory storage");
    } else {
      try {
        this.connection.send(new CreateBucketCommand(params));

        for (const [key, value] of this.inMemoryStorage as Map<
          string,
          Buffer
        >) {
          await this.connection.send(
            new PutObjectCommand({
              Bucket: params.Bucket,
              Key: key,
              Body: value,
            })
          );
        }
      } catch {
        throw new S3RestoreError();
      }
    }
  }

  /**
   * Converts a readable stream to a buffer.
   * @param {Readable} stream - The readable stream to convert.
   * @returns {Promise<Buffer>}
   */
  private async streamToBuffer(stream: Readable): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
      const chunks: any[] = [];
      stream.on('data', (chunk) => chunks.push(chunk));
      stream.on('error', (err) => reject(err));
      stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
  }
}
