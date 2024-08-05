import RollbackableClient from "../RollbackableClient/RollbackableClient";
import AWS, { HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { S3RollbackStrategy, S3RollbackFactory } from "./S3RollbackStrategy";

export interface S3Params {
    Bucket: string,
    Key: string,
    Body?: Buffer,
    Strategy?: S3RollbackStrategy
}

export default class S3Client extends RollbackableClient {
    private connection: AWS.S3Client;
    private rollbackStrategy: S3RollbackStrategy;

    constructor(_transactionID: string, _connection: AWS.S3Client, _rollbackStrategy: S3RollbackStrategy){
        super(_transactionID);
        this.connection = _connection;
        this.rollbackStrategy = _rollbackStrategy;
    }

    public async invoke(actionID: string): Promise<void> {
        try {
            await this.actions[actionID]();
            delete this.actions[actionID];
        } catch {
            await this.rollback();
        }
    }

    public async rollback(): Promise<void> {
        Object.keys(this.actions).forEach(async aid => {
            await this.reverseActions[aid]();
        });
    }

    public async putObject(params: S3Params): Promise<void> {
        const actionID = `put-${params.Bucket}-${params.Key}`;
        const handler = S3RollbackFactory(this.connection, this.rollbackStrategy);
        let objExists = false;

        this.actions[actionID] = async () => {
            this.connection.send(new HeadObjectCommand(params)).then(() => {
                objExists = true;
                handler.backup(params);
            });

            await this.connection.send(new PutObjectCommand(params));
        };
        this.reverseActions[actionID] = async () => {
            objExists ?
            handler.restore(params) :
            handler.delete(params);
        };
    }

    public async deleteObject(params: S3Params): Promise<void> {
        const actionID = `delete-${params.Bucket}-${params.Key}`;
        const handler = S3RollbackFactory(this.connection, this.rollbackStrategy);

        this.actions[actionID] = async () => {
            await handler.backup(params);
            await handler.delete(params);
        };
        this.reverseActions[actionID] = async () => {
            await handler.restore(params);
        };
    }
}