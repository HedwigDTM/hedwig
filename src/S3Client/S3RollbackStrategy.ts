import { S3Client as AWSClient } from "@aws-sdk/client-s3";
import { S3Params } from "./S3Client";

export enum S3RollbackStrategy {
  IN_MEMORY,
  DUPLICATE_FILE,
}

export abstract class S3Strategy {
  protected connection: AWSClient;

  constructor(_connection: AWSClient) {
    this.connection = _connection;
  }

  public abstract backup(params: S3Params): Promise<void>;
  public abstract restore(params: S3Params): Promise<void>;
}
