import { HedwigError } from './hedwig-error';

/**
 * Thrown when taking a backup of an S3 object or bucket fails.
 */
export class S3BackupError extends HedwigError {
  constructor(message = '') {
    super(message);
    this.name = 'S3BackupError';
  }
}

/**
 * Thrown when restoring an S3 object from its backup fails.
 */
export class S3RestoreError extends HedwigError {
  constructor(message = '') {
    super(message);
    this.name = 'S3RestoreError';
  }
}
