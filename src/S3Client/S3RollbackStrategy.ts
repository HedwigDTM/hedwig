import AWS from "@aws-sdk/client-s3";
import { S3Params } from "./S3Client";
import { InMemoryStrategy } from "./S3Strategies/InMemoryStaregy";

export enum S3RollbackStrategy {
    IN_MEMORY,
    DUPLICATE_FILE,
}

export abstract class S3Startegy {
    protected connection: AWS.S3Client;

    constructor(_connection: AWS.S3Client){
        this.connection = _connection;
    }

    public abstract backup(params: S3Params): void;
    public abstract restore(params: S3Params): void;
    public abstract delete(params: S3Params): void;
}

/**
 * Custom error class for backup operations.
 */
export class S3BackupError extends Error {
    constructor(message = '') {
      super(message);
      this.name = "BackupError";
    }
  }
  
/**
 * Custom error class for restore operations.
 */
export class S3RestoreError extends Error {
    constructor(message = '') {
        super(message);
        this.name = "RestoreError";
    }
}

/**
 * Custom error class for restore operations.
 */
export class S3DeleteError extends Error {
    constructor(message = '') {
        super(message);
        this.name = "DeleteError";
    }
}

export const S3RollbackFactory = (connection: AWS.S3Client, strategy: S3RollbackStrategy): S3Startegy => {
    switch(strategy) {
        case (S3RollbackStrategy.IN_MEMORY): {
            return new InMemoryStrategy(connection);
        }
        default :
            throw new Error("Rollback strategy type was not found!");
    }
}