import { S3Client as AWSClient } from "@aws-sdk/client-s3";
import { S3Params } from "./S3Client";


export abstract class S3RollBackStrategy {
  protected connection: AWSClient;

  constructor(_connection: AWSClient) {
    this.connection = _connection;
  }

  public abstract backup(params: S3Params): Promise<void>;
  public abstract restore(params: S3Params): Promise<void>;
}
