# Error handling

All hedwig errors extend `HedwigError`, so `instanceof HedwigError` catches
anything the library throws.

## Hierarchy

| Error            | Thrown when                                                                                           |
| ---------------- | ----------------------------------------------------------------------------------------------------- |
| `HedwigError`    | base class of all hedwig errors                                                                       |
| `RollbackError`  | rollback actions failed (carries `rollbackFailures`) or cleanup failed after a successful transaction |
| `S3BackupError`  | taking an S3 backup failed                                                                            |
| `S3RestoreError` | restoring an S3 object from its backup failed                                                         |

```ts
import { RollbackError } from '@hedwig-team/dtm';

try {
  await manager.transaction(async ({ S3Client }) => {
    await S3Client.putObject({ Bucket: 'b', Key: 'k', Body: data });
    throw new Error('business rule violated');
  });
} catch (error) {
  // The ORIGINAL error always wins.
  if ((error as Error).message === 'business rule violated') {
    const withCleanup = error as Error & { cleanupFailures?: unknown[] };
    if (withCleanup.cleanupFailures) {
      // rollback/closeTransaction/disconnect failures live here,
      // the original error is never replaced
    }
  }
}
```

## Failure semantics inside a transaction

1. If the callback throws, all recorded actions roll back in **reverse
   order**. A failing rollback action does **not** stop the remaining ones;
   if any fail, a `RollbackError` is thrown with the individual failures in
   `error.rollbackFailures` — but the **original callback error** is what
   propagates out of `transaction()` (rollback failures ride along as
   `error.cleanupFailures`).
2. If the callback succeeds but cleanup (`closeTransaction`, disconnecting
   owned connections) fails, `transaction()` rejects with a `RollbackError`
   carrying the cleanup failures.
3. Rollback is single-shot: actions are consumed as they roll back, so a
   second `rollback()` call cannot re-run them.

## S3-specific behavior

- `putObject` / `createBucket` treat **only 404-style responses** as
  "does not exist". Any other head error (403 with restricted IAM, 5xx,
  network) fails the operation instead of risking a wrong rollback branch.
- `createBucket` recognizes `BucketAlreadyOwnedByYou` /
  `BucketAlreadyExists` and never deletes a pre-existing bucket on rollback.
