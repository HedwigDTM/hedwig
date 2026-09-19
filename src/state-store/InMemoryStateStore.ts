import {
  ClientKind,
  RecordedAction,
  TransactionRecord,
  TransactionStateStore,
  TransactionStatus,
} from '../types/transaction-state';

/**
 * Process-local state store for tests and single-instance usage. State dies
 * with the process, so recovery is only meaningful within one lifetime.
 */
export class InMemoryStateStore implements TransactionStateStore {
  private records: Map<string, TransactionRecord> = new Map();
  private actions: Map<string, RecordedAction[]> = new Map();

  public async start(record: TransactionRecord): Promise<void> {
    this.records.set(record.id, { ...record });
    this.actions.set(record.id, []);
  }

  public async recordAction(id: string, action: RecordedAction): Promise<void> {
    const actions = this.actions.get(id);
    if (!actions) {
      throw new Error(`No transaction record for ${id}`);
    }
    actions.push({ ...action });
  }

  public async markCommitted(id: string): Promise<void> {
    this.setStatus(id, 'committed');
  }

  public async markRolledBack(id: string): Promise<void> {
    this.setStatus(id, 'rolled-back');
  }

  public async listInFlight(olderThan?: string): Promise<TransactionRecord[]> {
    return [...this.records.values()].filter(
      (record) =>
        record.status === 'in-flight' &&
        (!olderThan || record.startedAt < olderThan)
    );
  }

  public getActions(id: string, client?: ClientKind): RecordedAction[] {
    return (this.actions.get(id) ?? []).filter(
      (action) => !client || action.client === client
    );
  }

  private setStatus(id: string, status: TransactionStatus): void {
    const record = this.records.get(id);
    if (!record) {
      throw new Error(`No transaction record for ${id}`);
    }
    this.records.set(id, { ...record, status });
  }
}
