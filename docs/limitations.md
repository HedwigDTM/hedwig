# Limitations

Hedwig provides compensating transactions with rollback — it is **not** a
two-phase-commit system. Know the edges before relying on it:

## No atomic commit phase

Operations apply immediately as you make them. Other readers can observe
intermediate states while your callback runs. Rollback happens only when
your callback throws (or when you call `rollback()` yourself).

## Time-of-check to time-of-use windows

- `putObject` heads the object, then puts it. Another writer can create or
  delete the key between the two calls; the rollback branch is chosen by the
  head result.
- Redis `set`/`del` snapshot the current value, then write. The snapshot GET
  and the write are two round trips on the same connection, not one atomic
  operation.

## Counter operations are snapshot-based, not atomic

`incr`/`decr` snapshot the value first and restore it exactly on rollback
(deleting keys that did not exist). Concurrent increments by other writers
between the snapshot and the increment are restored away as part of the
snapshot.

## S3 buckets

`deleteBucket` targets **empty buckets only** — S3 refuses to delete
non-empty buckets, so hedwig fails fast with a clear error instead of
running a backup that could never be used. Missing buckets fail too; a
successful delete rolls back by recreating the (empty) bucket.

## Single process

Rollback bookkeeping is process-local. If the process dies mid-transaction,
hedwig cannot resume it; recovery of orphaned writes is tracked in
[#71](https://github.com/HedwigDTM/hedwig/issues/71) (pluggable consistency
backend).

## IAM

With `DUPLICATE_FILE`, the credentials need backup-bucket permissions in
addition to the source ones (`s3:CreateBucket`, `s3:CopyObject`,
`s3:DeleteObject` on the backup bucket). Restricted IAM that hides 404s
behind 403s makes object-existence checks fail the transaction by design —
hedwig refuses to guess and refuses to delete data it cannot verify.
