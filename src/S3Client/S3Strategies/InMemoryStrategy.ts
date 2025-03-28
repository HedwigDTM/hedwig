import {
  GetObjectCommand,
  PutObjectCommand,
  S3Client as AWSClient,
  ListObjectsCommand,
  CreateBucketCommand,
} from '@aws-sdk/client-s3';
import { S3BucketParams, S3ObjectParams } from '../S3Client';
import { S3RollBackStrategy } from '../S3RollbackStrategy';
import { S3BackupError, S3RestoreError } from '../S3RollbackFactory';

interface S3BucketBackup {
  objects: Map<string, Uint8Array>;
}

interface S3InMemoryStorage {
  buckets: Map<string, S3BucketBackup>;
}

/**
 * Class to handle in-memory delete, backup and restore of S3 objects.
 * Implements the iS3Backuper interface.
 */
export class InMemoryStrategy extends S3RollBackStrategy {
  private storage: S3InMemoryStorage;

  constructor(_connection: AWSClient) {
    super(_connection);
    this.storage = {
      buckets: new Map(),
    };
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

      // Initialize bucket if it doesn't exist
      if (!this.storage.buckets.has(Bucket)) {
        this.storage.buckets.set(Bucket, {
          objects: new Map(),
        });
      }

      const bucketBackup = this.storage.buckets.get(Bucket)!;
      if (!data.Body) {
        throw new S3BackupError(`No data found in the S3 object: ${Key}`);
      }
      bucketBackup.objects.set(Key, await data.Body.transformToByteArray());
    } catch (error) {
      throw new S3BackupError(`Failed to backup file from S3: ${error}`);
    }
  }

  /**
   * Clears the in-memory storage once the transaction is closed.
   * @returns {Promise<void>}
   */
  public async closeTransaction(): Promise<void> {
    this.storage.buckets.clear();
  }

  /**
   * Restores the latest version of an S3 object from in-memory storage.
   * @param {S3Params} params - Parameters for the restore operation.
   * @returns {Promise<void>}
   */
  public async restoreFile(params: S3ObjectParams): Promise<void> {
    const { Bucket, Key } = params;
    const bucketBackup = this.storage.buckets.get(Bucket);
    
    if (!bucketBackup) {
      throw new S3RestoreError('No backup found for the specified bucket');
    }

    const objectBackup = bucketBackup.objects.get(Key);
    if (!objectBackup) {
      throw new S3RestoreError('No backup data found for the specified file');
    }

    try {
      await this.connection.send(
        new PutObjectCommand({
          Bucket,
          Key,
          Body: objectBackup,
        })
      );
    } catch (error) {
      throw new S3RestoreError(`Failed to restore file to S3: ${error}`);
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
      const bucketBackup: S3BucketBackup = {
        objects: new Map(),
      };

      do {
        const listResponse = await this.connection.send(
          new ListObjectsCommand({
            ...params,
            Marker: nextMarker,
          })
        );

        if (!listResponse.Contents) {
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
            if (!data.Body) {
              throw new S3BackupError('No data found in the S3 object');
            }
            bucketBackup.objects.set(obj.Key, await data.Body.transformToByteArray());
          })
        );

        nextMarker = listResponse.NextMarker;
      } while (nextMarker);

      this.storage.buckets.set(params.Bucket, bucketBackup);
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
    const bucketBackup = this.storage.buckets.get(params.Bucket);
    
    if (!bucketBackup) {
      throw new S3RestoreError('No backup found for the specified bucket');
    }

    try {
      await this.connection.send(new CreateBucketCommand(params));

      await Promise.all(
        Array.from(bucketBackup.objects.entries()).map(async ([key, object]) => {
          await this.connection.send(
            new PutObjectCommand({
              Bucket: params.Bucket,
              Key: key,
              Body: object,
            })
          );
        })
      );
    } catch (error) {
      throw new S3RestoreError(`Failed to restore bucket to S3: ${error}`);
    }
  }
}
