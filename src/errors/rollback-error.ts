import { HedwigError } from './hedwig-error';

/**
 * Thrown when one or more rollback actions fail. Carries the individual
 * failures so they can be inspected without masking the original
 * transaction error.
 */
export class RollbackError extends HedwigError {
  readonly rollbackFailures: unknown[];

  constructor(message = '', rollbackFailures: unknown[] = []) {
    super(message);
    this.name = 'RollbackError';
    this.rollbackFailures = rollbackFailures;
  }
}
