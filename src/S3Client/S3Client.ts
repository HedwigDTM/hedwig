import RollbackableClient from "../RollbackableClient/RollbackableClient";
import AWS from "@aws-sdk/client-s3";

interface S3ConnectionProps {
    accessKeyId: string;
    secretKetId: string;
    region: string;
}

export default class S3Client extends RollbackableClient {
    private connection: AWS.S3Client;

    constructor(props: S3ConnectionProps){
        super();
        this.connection = new AWS.S3Client({
            credentials: {
                accessKeyId: props.accessKeyId,
                secretAccessKey: props.secretKetId,
            },
            region: props.region,
        });
    }

    public async invoke(tid: string): Promise<any> {
        try {
            await this.actions[tid]();
        } catch {
            await this.rollback(tid);
        }
    }

    public async rollback(tid: string): Promise<any> {
        await this.reverseActions[tid]();
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