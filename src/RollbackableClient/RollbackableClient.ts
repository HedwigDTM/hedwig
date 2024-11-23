export interface RollbackableAction {
  rollbackAction: () => Promise<any>;
}

// Todo: add genrics
export default abstract class RollbackableClient {
  protected rollbackActions: (() => Promise<any>)[];
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
    for (const rollbackAction of this.rollbackActions.reverse()) {
      await rollbackAction();
    }
  }
}
