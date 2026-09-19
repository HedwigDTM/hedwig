import {
  CopyObjectCommand,
  DeleteObjectCommand,
  ListObjectsV2Command,
} from '@aws-sdk/client-s3';
import { S3Client as AWSClient } from '@aws-sdk/client-s3';
import { RedisConnection } from '../RedisClient/RedisConnection';
import { S3BackupError, S3RestoreError } from '../errors';
import { TransactionStateStore } from '../types/transaction-state';
import { parseBackupRecord } from '../RedisClient/RedisStrategies/DuplicateStrategy';

export const DEFAULT_BACKUP_BUCKET_NAME = 'hedwig-backups';
export const DEFAULT_BACKUP_HASH_NAME = 'Hedwig-Backups';

export interface RecoveryOptions {
  store: TransactionStateStore;
  /** Only recover transactions that started before this ISO timestamp. */
  olderThan?: string;
  /** Provide to enable S3 recovery (requires the DUPLICATE_FILE strategy). */
  s3?: { connection: AWSClient; backupBucketName?: string };
  /** Provide to enable Redis recovery (requires the DUPLICATE_FILE strategy). */
  redis?: { connection: RedisConnection; backupHashName?: string };
}

export interface RecoverySummary {
  recovered: string[];
  failed: { id: string; error: string }[];
}

/**
 * Rolls back transactions that were orphaned by a crash: their durable
 * backups (DUPLICATE_FILE strategy artifacts — S3 backup keys under
 * `<transactionID>/` and Redis backup hashes) are still on the backend, so
 * recovery replays them and marks the transactions rolled back. Committed
 * transactions are never touched.
 */
export async function recoverInFlightTransactions(
  options: RecoveryOptions
): Promise<RecoverySummary> {
  const { store, olderThan, s3, redis } = options;
  const summary: RecoverySummary = { recovered: [], failed: [] };

  const inFlight = await store.listInFlight(olderThan);

  for (const record of inFlight) {
    try {
      if (redis) {
        await recoverRedisTransaction(
          record.id,
          redis.connection,
          redis.backupHashName
        );
      }
      if (s3) {
        await recoverS3Transaction(
          record.id,
          s3.connection,
          s3.backupBucketName ?? DEFAULT_BACKUP_BUCKET_NAME
        );
      }
      await store.markRolledBack(record.id);
      summary.recovered.push(record.id);
    } catch (error) {
      // Left in-flight so a later recovery pass can retry
      summary.failed.push({ id: record.id, error: String(error) });
    }
  }

  return summary;
}

async function recoverRedisTransaction(
  txId: string,
  connection: RedisConnection,
  backupHashName?: string
): Promise<void> {
  const hash = `${backupHashName ?? DEFAULT_BACKUP_HASH_NAME}:${txId}`;
  const backups = await connection.hGetAll(hash);

  for (const [sourceKey, raw] of Object.entries(backups)) {
    const record = parseBackupRecord(raw, sourceKey);
    if (record.existed) {
      await connection.set(sourceKey, record.value);
    } else {
      await connection.del(sourceKey);
    }
  }
  await connection.del(hash);
}

async function recoverS3Transaction(
  txId: string,
  connection: AWSClient,
  backupBucketName: string
): Promise<void> {
  let marker: string | undefined;
  do {
    const response = await connection.send(
      new ListObjectsV2Command({
        Bucket: backupBucketName,
        Prefix: `${txId}/`,
        ContinuationToken: marker,
      })
    );
    const contents = response.Contents ?? [];
    for (const object of contents) {
      const backupKey = object.Key;
      if (!backupKey) {
        continue;
      }
      const withoutPrefix = backupKey.slice(`${txId}/`.length);
      const separator = withoutPrefix.indexOf('/');
      if (separator === -1) {
        throw new S3RestoreError(`Malformed backup key: ${backupKey}`);
      }
      const sourceBucket = withoutPrefix.slice(0, separator);
      const sourceKey = withoutPrefix.slice(separator + 1);

      try {
        await connection.send(
          new CopyObjectCommand({
            Bucket: sourceBucket,
            Key: sourceKey,
            CopySource: `${backupBucketName}/${backupKey}`,
          })
        );
      } catch (error) {
        throw new S3RestoreError(
          `Failed to restore ${sourceBucket}/${sourceKey}: ${String(error)}`
        );
      }
      await connection.send(
        new DeleteObjectCommand({ Bucket: backupBucketName, Key: backupKey })
      );
    }
    marker = response.IsTruncated
      ? contents[contents.length - 1]?.Key
      : undefined;
  } while (marker);
}
