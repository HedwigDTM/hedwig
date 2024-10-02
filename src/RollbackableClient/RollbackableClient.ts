export interface RollbackableAction {
  rollbackAction: () => Promise<any>;
}

// Todo: add genrics
export default abstract class RollbackableClient {
  protected rollbackActions: Map<string, () => Promise<any>>;
  protected transactionID: string;

  constructor(_transactionID: string) {
    this.transactionID = _transactionID;
    this.rollbackActions = new Map<string, () => Promise<any>>();
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
    this.rollbackActions.forEach(async (rollbackAction) => {
      await rollbackAction();
    });
  }
}
