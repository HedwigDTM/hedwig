import { InMemoryStrategy } from "./S3Strategies/InMemoryStrategy";
import { DuplicateStrategy } from "./S3Strategies/DuplicateStrategy";
import { S3Client as AWSClient } from "@aws-sdk/client-s3";
import { S3RollbackStrategy, S3Strategy } from "./S3RollbackStrategy";

/**
 * Custom error class for backup operations.
 */
export class S3BackupError extends Error {
  constructor(message = "") {
    super(message);
    this.name = "BackupError";
  }
}

/**
 * Custom error class for restore operations.
 */
export class S3RestoreError extends Error {
  constructor(message = "") {
    super(message);
    this.name = "RestoreError";
  }
}

export const S3RollbackFactory = (
  connection: AWSClient,
  strategy: S3RollbackStrategy
): S3Strategy => {
  switch (strategy) {
    case S3RollbackStrategy.IN_MEMORY: {
      return new InMemoryStrategy(connection);
    }
    case S3RollbackStrategy.DUPLICATE_FILE: {
      return new DuplicateStrategy(connection);
    }
    default:
      throw new Error("Rollback strategy type was not found!");
  }
};
