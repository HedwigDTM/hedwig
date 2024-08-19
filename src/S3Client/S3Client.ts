import RollbackableClient from "../RollbackableClient/RollbackableClient";
import {
  DeleteObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client as AWSClient,
} from "@aws-sdk/client-s3";
import { S3RollbackFactory } from "./S3RollbackFactory";
import { v4 as uuidv4 } from "uuid";
import { S3RollbackStrategyType } from "../Types/S3/S3RollBackStrategy";
import { S3RollBackStrategy } from "./S3RollbackStrategy";

export interface S3Params {
  Bucket: string;
  Key: string;
  Body?: Buffer;
}

export class S3RollbackClient extends RollbackableClient {
  private connection: AWSClient;
  private rollbackStrategy: S3RollBackStrategy;

  constructor(
    transactionID: string,
    connection: AWSClient,
    rollbackStrategyType: S3RollbackStrategyType
  ) {
    super(transactionID);
    this.connection = connection;
    this.rollbackStrategy = S3RollbackFactory(
      this.connection,
      rollbackStrategyType
    );
  }

  public async rollback(): Promise<void> {
    this.rollbackActions.forEach(async (rollbackAction) => {
      await rollbackAction();
    });
  }

  public async putObject(params: S3Params): Promise<void> {
    const actionID = `put-${params.Bucket}-${params.Key}-${uuidv4().substring(
      0,
      4
    )}`;
    let objExists = false;
    this.connection.send(new HeadObjectCommand(params)).then(() => {
      objExists = true;
      this.rollbackStrategy.backup(params);
    });

    await this.connection.send(new PutObjectCommand(params));

    const rollbackAction = async () => {
      objExists
        ? await this.rollbackStrategy.restore(params)
        : await this.connection.send(new DeleteObjectCommand(params));
    };

    this.actions.set(actionID, { rollbackAction });
  }

  public async deleteObject(params: S3Params): Promise<void> {
    const actionID = `delete-${params.Bucket}-${
      params.Key
    }-${uuidv4().substring(0, 4)}`;

    await this.rollbackStrategy.backup(params);
    await this.connection.send(new DeleteObjectCommand(params));
    const rollbackAction = async () => {
      await this.rollbackStrategy.restore(params);
    };

    this.actions.set(actionID, { rollbackAction });
  }
}
