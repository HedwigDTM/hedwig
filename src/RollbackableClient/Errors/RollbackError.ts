/**
 * Custom error class for errors during client rollback.
 * Carries the individual failures of the rollback actions that ran.
 */
export default class RollbackError extends Error {
  readonly rollbackFailures: unknown[];

  constructor(message = '', rollbackFailures: unknown[] = []) {
    super(message);
    this.name = 'RollbackError';
    this.rollbackFailures = rollbackFailures;
  }
}
