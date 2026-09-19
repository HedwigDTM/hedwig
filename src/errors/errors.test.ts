import {
  HedwigError,
  RollbackError,
  S3BackupError,
  S3RestoreError,
} from './index';

describe('errors', () => {
  it('RollbackError should carry its individual failures', () => {
    const failures = [new Error('a'), new Error('b')];
    const error = new RollbackError('2 rollback actions failed', failures);

    expect(error).toBeInstanceOf(HedwigError);
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('RollbackError');
    expect(error.rollbackFailures).toEqual(failures);
  });

  it('S3BackupError and S3RestoreError should extend HedwigError', () => {
    const backupError = new S3BackupError('backup failed');
    const restoreError = new S3RestoreError('restore failed');

    expect(backupError).toBeInstanceOf(HedwigError);
    expect(backupError.name).toBe('S3BackupError');
    expect(restoreError).toBeInstanceOf(HedwigError);
    expect(restoreError.name).toBe('S3RestoreError');
  });
});
