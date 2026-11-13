# Changelog

## 1.1.0 - 2026-11-13

A large reliability and developer-experience release: nine correctness fixes
to rollback behavior, a real public API with typed clients, a proper error
hierarchy, two new **beta** capabilities, and documentation.

### New features

- **Real public API.** Everything is importable from the package root:
  `TransactionManager`, `S3RollbackClient`, `RedisRollbackClient`,
  `RollbackableClient`, both strategy enums, the error classes and every
  public type. No deep imports needed.
- **Typed clients.** `TransactionManager<C>` makes a client non-optional in
  the callback exactly when its config was provided, so `if (S3Client)`
  guards are no longer needed for configured clients.
- **`transaction()` returns the callback's value** (`Promise<T>`).
- **Error hierarchy.** `HedwigError` (base), `RollbackError` (carries
  `rollbackFailures`), `S3BackupError`, `S3RestoreError`; catch by class.
- **Verbose logging.** `verbose: true` logs the transaction lifecycle
  (`[hedwig]` prefix): start, rollback trigger and outcome, commit, cleanup
  failures. Silent by default.
- **Documentation.** New `docs/` folder: getting started, strategies, error
  handling, limitations and a full API reference.

### Experimental (beta) features

These APIs are new and may change in minor releases before they stabilize.

- **Custom actions** (`ICustomAction`, `customActions.register(...)`):
  run any side effect inside a transaction with your own compensating
  `rollback()`. Compensations run in reverse order with the same
  resilience as built-in clients.
  See [docs/custom-actions.md](https://github.com/HedwigDTM/hedwig/blob/main/docs/custom-actions.md).
- **Transaction state and crash recovery** (`TransactionStateStore`,
  `InMemoryStateStore`, `RedisStateStore`, `recoverInFlightTransactions`):
  persist transaction state so writes orphaned by a crash can be rolled back
  from the durable `DUPLICATE_FILE` backups. Committed transactions are
  never re-rolled-back. `IN_MEMORY` strategies are not crash-recoverable.
  See [docs/transaction-state.md](https://github.com/HedwigDTM/hedwig/blob/main/docs/transaction-state.md).

### Bug fixes

- **Backups are namespaced per transaction.** Concurrent transactions (and
  the same key in different buckets) no longer overwrite each other's
  backups and corrupt rollbacks. S3 backup keys are now
  `<transactionID>/<Bucket>/<Key>`; Redis backups use a per-transaction hash.
- **Cleanup no longer destroys shared state.** `closeTransaction` removes
  only the artifacts of its own transaction instead of wiping the shared
  backup bucket; the Redis backup hash is now actually deleted (it was a
  literal `hash*` DEL that never matched) and Redis cleanup really runs;
  backup buckets created for bucket operations no longer leak.
- **Rollback is resilient.** A failing rollback action no longer aborts the
  remaining ones; failures aggregate into `RollbackError`. The original
  error is never masked by rollback or cleanup errors (cleanup failures are
  attached as `error.cleanupFailures`). Rollback is single-shot.
- **No data loss on restricted IAM.** Only a 404 now means "object does not
  exist". A 403 or 5xx on `HeadObject`/`HeadBucket` used to make rollback
  delete a pre-existing object; it now fails the operation instead.
  `createBucket` handles `BucketAlreadyOwnedByYou` and never deletes an
  existing bucket on rollback.
- **Redis backups are awaited.** Fixes unhandled promise rejections and
  broken rollbacks when `del`-ing a missing key; absence is recorded
  explicitly.
- **Empty-string Redis values** can be backed up and restored.
- **`deleteBucket` works.** Empty buckets are deleted and recreated on
  rollback; non-empty buckets fail fast with a clear error. The broken
  bucket backup (which silently truncated at 1,000 objects) is removed.
- **No more Redis connection leak.** Connections created by the manager are
  closed after each transaction; connections you supply are never closed.
- **`incr`/`decr` rollback restores the exact pre-transaction value** and no
  longer undoes concurrent writers or leaves new keys behind at `0`.
- **Backup records are validated** when parsed from Redis (corrupted data
  raises a `RollbackError` instead of being trusted).

### Upgrade notes

- `@aws-sdk/client-s3` and `redis` are now **peer dependencies only**; install
  them in your project (npm 7+ does this automatically).
- `deleteBucket` only deletes **empty** buckets (S3 cannot delete non-empty
  ones); it previously failed for every bucket.
- `S3BackupError`/`S3RestoreError` now have matching `name` values
  (previously `BackupError`/`RestoreError`); catch them by class.
- If the callback succeeds but cleanup fails, `transaction()` rejects with a
  `RollbackError`.
- Backup storage layout changed (see bug fixes); backups written by earlier
  versions are not recognized by recovery.

### Maintenance

- Removed junk and test-only production dependencies (including the `npm` CLI
  itself); the only runtime dependency is now `uuid`, upgraded to 11.1.1
  (GHSA-w5hq-g745-h8pq; hedwig only uses `v4()`, which was not affected).
  Production dependencies have no known vulnerabilities, and overall
  `npm audit` findings dropped from 102 to 37, all in development tooling.
- ESLint 9, coverage thresholds, GitHub Actions CI (Node 20/22), automated
  npm publishing with provenance, real `SECURITY.md`, consistent MIT license.
- Test suite grew from 27 to 81 tests, plus an end-to-end run against real
  Redis and an S3-compatible server, including a real `SIGKILL` crash and
  recovery.
