import RollbackableClient from "../RollbackableClient/RollbackableClient";
import AWS, { DeleteObjectCommand, HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { S3RollbackStrategy, S3RollbackFactory } from "./S3RollbackStrategy";
import InvocationError from "../RollbackableClient/InvokeError";

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

    public async invoke(): Promise<void> {
        Object.keys(this.actions).forEach(async aid => {
            try {
                await this.actions[aid]();
            } catch {
                throw new InvocationError(`Error in ${aid}`);
            }
        });
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
            await handler.restore(params) :
            await this.connection.send(new DeleteObjectCommand(params));
        };
    }

    public async deleteObject(params: S3Params): Promise<void> {
        const actionID = `delete-${params.Bucket}-${params.Key}`;
        const handler = S3RollbackFactory(this.connection, this.rollbackStrategy);

        this.actions[actionID] = async () => {
            await handler.backup(params);
            await this.connection.send(new DeleteObjectCommand(params));
        };
        this.reverseActions[actionID] = async () => {
            await handler.restore(params);
        };
    }
}