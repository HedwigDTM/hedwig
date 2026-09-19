import { RedisBackupRecord } from './redis';

export type TransactionStatus = 'in-flight' | 'committed' | 'rolled-back';

export type ClientKind = 's3' | 'redis' | 'custom';

export interface TransactionRecord {
  id: string;
  status: TransactionStatus;
  /** ISO timestamp of when the transaction started. */
  startedAt: string;
}

export interface RecordedAction {
  client: ClientKind;
  /** Operation kind, e.g. "putObject", "set", "incr". */
  kind: string;
}

/**
 * Durable bookkeeping for hedwig transactions. With a store configured, a
 * transaction record is persisted before writes happen, every mutating
 * action is recorded, and completion is marked — enabling a recovery pass
 * that rolls back transactions orphaned by a crash (their durable backups
 * live in the DUPLICATE_FILE strategies: S3 backup keys and Redis hashes).
 */
export interface TransactionStateStore {
  start(record: TransactionRecord): Promise<void>;
  recordAction(id: string, action: RecordedAction): Promise<void>;
  markCommitted(id: string): Promise<void>;
  markRolledBack(id: string): Promise<void>;
  /** Transactions still in-flight, optionally older than an ISO timestamp. */
  listInFlight(olderThan?: string): Promise<TransactionRecord[]>;
}

/**
 * One restorable entry of a durable backup, as produced by the duplicate
 * strategies and consumed by recovery.
 */
export type RestorableBackup = {
  sourceKey: string;
  record: RedisBackupRecord;
};
