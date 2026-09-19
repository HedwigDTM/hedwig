/**
 * A custom action participates in a hedwig transaction with its own
 * compensating operation. `execute` runs inside the transaction; `rollback`
 * is recorded and invoked (in reverse registration order) if the transaction
 * fails.
 *
 * @beta The custom-actions API is new and may still change in minor
 * releases before it stabilizes.
 */
export interface ICustomAction {
  /** Name used in logs and error messages. */
  name?: string;
  /** Runs the action inside the transaction. */
  execute(): Promise<unknown>;
  /** Compensating operation invoked when the transaction rolls back. */
  rollback(): Promise<unknown>;
}

/**
 * What transaction callbacks receive as `customActions`:
 * registering an action executes it immediately and records its rollback.
 *
 * @beta
 */
export type CustomActionsApi = {
  register: (action: ICustomAction) => Promise<unknown>;
};
