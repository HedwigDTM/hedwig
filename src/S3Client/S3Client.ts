import RollbackableClient from "../RollbackableClient/RollbackableClient";
import AWS from "@aws-sdk/client-s3";

export default class S3Client extends RollbackableClient {
    private connection: AWS.S3Client;

    constructor(_transactionID: string, _connection: AWS.S3Client){
        super(_transactionID);
        this.connection = _connection;
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

    public async putObject(params: { Bucket: string, Key: string, Body: Buffer }): Promise<void> {
        const actionID = `put-${params.Bucket}-${params.Key}`;

        const action = new AWS.PutObjectCommand(params);
        const reverseAction = new AWS.DeleteObjectCommand(params);

        this.actions[actionID] = async () => await this.connection.send(action);
        this.reverseActions[actionID] = async () => await this.connection.send(reverseAction);
    }

    public async deleteObject(params: { Bucket: string, Key: string }): Promise<void> {
        const actionID = `delete-${params.Bucket}-${params.Key}`;

        const obj = (await this.connection.send(new AWS.GetObjectCommand(params))).Body;

        const action = new AWS.DeleteObjectCommand(params);
        const reverseAction = new AWS.PutObjectCommand({...params, ...{Body: obj}})

        this.actions[actionID] = async () => await this.connection.send(action);
        this.reverseActions[actionID] = async () => await this.connection.send(reverseAction);
    }
}