import RollbackableClient from "../RollbackableClient/RollbackableClient";
import AWS, {
  DeleteObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { S3RollbackStrategy, S3RollbackFactory } from "./S3RollbackStrategy";
import InvocationError from "../RollbackableClient/InvokeError";

export interface S3Params {
  Bucket: string;
  Key: string;
  Body?: Buffer;
  Strategy?: S3RollbackStrategy;
}

export default class S3Client extends RollbackableClient {
  private connection: AWS.S3Client;
  private rollbackStrategy: S3RollbackStrategy;

  constructor(
    _transactionID: string,
    _connection: AWS.S3Client,
    _rollbackStrategy: S3RollbackStrategy
  ) {
    super(_transactionID);
    this.connection = _connection;
    this.rollbackStrategy = _rollbackStrategy;
  }

  public async invoke(): Promise<void> {
    this.actions.forEach(async (rollbackableAction, aid) => {
      try {
        await rollbackableAction.action();
      } catch {
        throw new InvocationError(`Error in ${aid}`);
      } finally {
        this.rollbackActions.set(aid, rollbackableAction.rollbackAction);
      }
    });
  }

  public async rollback(): Promise<void> {
    this.rollbackActions.forEach(async (rollbackAction) => {
      await rollbackAction();
    });
  }

  public async putObject(params: S3Params): Promise<void> {
    const actionID = `put-${params.Bucket}-${params.Key}`;
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
    const actionID = `delete-${params.Bucket}-${params.Key}`;
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
