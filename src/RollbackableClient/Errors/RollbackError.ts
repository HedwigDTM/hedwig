/**
 * Custom error class for errors during client rollback.
 */
export default class RollbackError extends Error {
  constructor(message = '') {
    super(message);
    this.name = 'RestoreError';
  }
}
