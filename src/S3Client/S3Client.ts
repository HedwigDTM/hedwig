import RollbackableClient from "../RollbackableClient/RollbackableClient";
import {
  DeleteObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  S3Client as AWSClient,
} from "@aws-sdk/client-s3";
import { S3RollbackStrategy } from "./S3RollbackStrategy";
import InvocationError from "../RollbackableClient/Errors/InvokeError";
import { S3RollbackFactory } from "./S3RollbackFactory";
import { v4 as uuidv4 } from "uuid";

export interface S3Params {
  Bucket: string;
  Key: string;
  Body?: Buffer;
}

export class S3Client extends RollbackableClient {
  private connection: AWSClient;
  private rollbackStrategy: S3RollbackStrategy;

  constructor(
    _transactionID: string,
    _connection: AWSClient,
    _rollbackStrategy: S3RollbackStrategy
  ) {
    super(_transactionID);
    this.connection = _connection;
    this.rollbackStrategy = _rollbackStrategy;
  }

  public async invoke(): Promise<boolean> {
    for (const [aid, action] of this.actions) {
      try {
        await action.action();
      } catch {
        return false;
      } finally {
        this.rollbackActions.set(aid, action.rollbackAction);
      }
    }

    return true;
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
    const handler = S3RollbackFactory(this.connection, this.rollbackStrategy);
    let objExists = false;

    const action = async () => {
      this.connection.send(new HeadObjectCommand(params)).then(() => {
        objExists = true;
        handler.backup(params);
      });
      await this.connection.send(new PutObjectCommand(params));
    };
    const rollbackAction = async () => {
      objExists
        ? await handler.restore(params)
        : await this.connection.send(new DeleteObjectCommand(params));
    };

    this.actions.set(actionID, { action, rollbackAction });
  }

  public async deleteObject(params: S3Params): Promise<void> {
    const actionID = `delete-${params.Bucket}-${
      params.Key
    }-${uuidv4().substring(0, 4)}`;
    const handler = S3RollbackFactory(this.connection, this.rollbackStrategy);

    const action = async () => {
      await handler.backup(params);
      await this.connection.send(new DeleteObjectCommand(params));
    };
    const rollbackAction = async () => {
      await handler.restore(params);
    };

    this.actions.set(actionID, { action, rollbackAction });
  }
}
