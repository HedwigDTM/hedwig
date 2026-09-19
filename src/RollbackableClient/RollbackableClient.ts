import { RollbackError } from '../errors';
export interface RollbackableAction {
  rollbackAction: () => Promise<unknown>;
}

// Todo: add genrics
export default abstract class RollbackableClient {
  protected rollbackActions: (() => Promise<unknown>)[];
  protected transactionID: string;

  constructor(_transactionID: string) {
    this.transactionID = _transactionID;
    this.rollbackActions = [];
  }

  public getTransactionID(): string {
    return this.transactionID;
  }

  /**
   * Rolls back all previously executed actions within the current transaction.
   *
   * @returns {Promise<void>} A promise that resolves once all rollback actions are complete.
   */
  public async rollback(): Promise<void> {
    const actions = this.rollbackActions.slice().reverse();
    this.rollbackActions = [];

    const failures: unknown[] = [];
    for (const rollbackAction of actions) {
      try {
        await rollbackAction();
      } catch (error) {
        failures.push(error);
      }
    }

    if (failures.length > 0) {
      throw new RollbackError(
        failures.length === 1
          ? '1 rollback action failed'
          : `${failures.length} rollback actions failed`,
        failures
      );
    }
  }

  public abstract closeTransaction(): Promise<void>;
}
