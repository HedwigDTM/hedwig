import RollbackableClient from '../RollbackableClient/RollbackableClient';
import { CustomActionsApi, ICustomAction } from './ICustomAction';

/**
 * Transaction-scoped registry for custom actions. Registering an action runs
 * its `execute()` immediately and records `rollback()` as a compensating
 * action; rollback runs compensations in reverse registration order, with
 * the same resilience as every other hedwig client.
 */
export class CustomActionRegistry
  extends RollbackableClient
  implements CustomActionsApi
{
  constructor(transactionID: string) {
    super(transactionID);
  }

  public async register(action: ICustomAction): Promise<unknown> {
    const label = action.name ?? 'unnamed custom action';

    const rollbackAction = async () => {
      await action.rollback();
    };

    this.rollbackActions.push(rollbackAction);

    try {
      return await action.execute();
    } catch (error) {
      // The action never executed, so its compensation must not run
      this.rollbackActions.pop();
      throw new Error(
        `Custom action "${label}" failed during execution: ${String(error)}`
      );
    }
  }

  public async closeTransaction(): Promise<void> {
    return Promise.resolve();
  }
}
