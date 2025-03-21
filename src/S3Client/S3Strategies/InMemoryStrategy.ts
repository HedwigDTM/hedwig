import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client as AWSClient,
  ListObjectsCommand,
  CreateBucketCommand,
} from '@aws-sdk/client-s3';
import { S3BucketParams, S3ObjectParams } from '../S3Client';
import { S3RollBackStrategy } from '../S3RollbackStrategy';
import { Readable } from 'stream';
import { S3BackupError, S3RestoreError } from '../S3RollbackFactory';

/**
 * Class to handle in-memory delete, backup and restore of S3 objects.
 * Implements the iS3Backuper interface.
 */
export class InMemoryStrategy extends S3RollBackStrategy {
  private inMemoryStorage: Map<string, Map<string, Buffer>> = new Map();

  constructor(_connection: AWSClient) {
    super(_connection);
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
      if (!this.inMemoryStorage.has(Bucket)) {
        this.inMemoryStorage.set(Bucket, new Map());
      }
      const bucketMap = this.inMemoryStorage.get(Bucket) as Map<string, Buffer>;
      bucketMap.set(Key, await this.streamToBuffer(data.Body as Readable));
    } catch (error) {
      throw new S3BackupError(`Failed to backup file from S3: ${error}`);
    }
  }

  /**
   * Clears the in-memory storage once the transaction is closed.
   * @returns {Promise<void>}
   */
  public async closeTransaction(): Promise<void> {
    this.inMemoryStorage.clear();
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
        const backupData = this.inMemoryStorage.get(Bucket)?.get(Key);
        if (!backupData) {
          throw new S3RestoreError('No backup data found for a specified file');
        }
        await this.connection.send(
          new PutObjectCommand({
            Bucket,
            Key,
            Body: backupData,
          })
        );
      } catch (error) {
        throw new S3RestoreError(`Failed to restore file to S3: ${error}`);
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
      let nextMarker: string | undefined;

      do {
        const listResponse = await this.connection.send(
          new ListObjectsCommand({
            ...params,
            Marker: nextMarker,
          })
        );

        if (!listResponse.Contents) {
          if (!this.inMemoryStorage.has(params.Bucket)) {
            this.inMemoryStorage.set(params.Bucket, new Map());
          }
          break;
        }

        await Promise.all(
          listResponse.Contents.map(async (obj) => {
            if (!obj.Key) {
              throw new S3BackupError('Object key is undefined');
            }
            const data = await this.connection.send(
              new GetObjectCommand({
                Bucket: params.Bucket,
                Key: obj.Key,
              })
            );
            const buffer = await this.streamToBuffer(data.Body as Readable);
            if (!this.inMemoryStorage.has(params.Bucket)) {
              this.inMemoryStorage.set(params.Bucket, new Map());
            }
            const bucketMap = this.inMemoryStorage.get(params.Bucket);
            if (!bucketMap) {
              throw new S3BackupError(
                'Failed to initialize bucket map in memory storage'
              );
            }
            bucketMap.set(obj.Key, buffer);
          })
        );

        nextMarker = listResponse.NextMarker;
      } while (nextMarker);
    } catch (error) {
      throw new S3BackupError(`Failed to backup bucket from S3: ${error}`);
    }
  }

  /**
   * Restores the latest version of an S3 bucket from in-memory storage.
   * @param {S3Params} params - Parameters for the restore operation.
   * @returns {Promise<void>}
   */
  public async restoreBucket(params: S3BucketParams): Promise<void> {
    if (!this.inMemoryStorage) {
      throw new S3RestoreError('No backup found in inMemory storage');
    } else {
      try {
        await this.connection.send(new CreateBucketCommand(params));
        const backupMap = this.inMemoryStorage.get(params.Bucket);
        if (!backupMap) {
          throw new S3RestoreError('No backup found for a specified bucket');
        }

        await Promise.all(
          Array.from(backupMap.entries()).map(async ([key, value]) => {
            await this.connection.send(
              new PutObjectCommand({
                Bucket: params.Bucket,
                Key: key,
                Body: value,
              })
            );
          })
        );
      } catch (error) {
        throw new S3RestoreError(`Failed to restore bucket to S3: ${error}`);
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
