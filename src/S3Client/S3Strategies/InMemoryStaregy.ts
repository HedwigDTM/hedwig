import AWS, { DeleteObjectCommand, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";
import { S3Params } from "../S3Client";
import { S3Startegy } from "../S3RollbackStrategy";
import { Readable } from "stream";
import { S3BackupError, S3RestoreError, S3DeleteError } from "../S3RollbackStrategy";

/**
 * Class to handle in-memory delete, backup and restore of S3 objects.
 * Implements the iS3Backuper interface.
 */
export class InMemoryStrategy extends S3Startegy {
    private inMemoryStorage?: Buffer;

    constructor(_connection: AWS.S3Client){
        super(_connection);
        this.inMemoryStorage = undefined;
    }

    /**
     * Backs up the current version of an S3 object to in-memory storage.
     * @param {S3Params} params - Parameters for the backup operation.
     * @returns {Promise<void>}
    */
    public async backup(params: S3Params): Promise<void> {
        const { Bucket, Key } = params;

        try {
            const data = await this.connection.send(new GetObjectCommand({
                Bucket,
                Key,
            }));
            this.inMemoryStorage = await this.streamToBuffer(data.Body as Readable);
        } catch {
            throw new S3BackupError();
        }
    }
    public async restore(params: S3Params): Promise<void> {
        if(!this.inMemoryStorage) {
            throw new S3RestoreError('No backup found in inMemory storage');
        } else {
            try {
                const { Bucket, Key } = params;
                await this.connection.send(new PutObjectCommand({
                    Bucket,
                    Key,
                    Body: this.inMemoryStorage
                }));
            } catch {
                throw new S3RestoreError();
            }
        }
    }
    public async delete(params: S3Params): Promise<void> {
        try {
            await this.connection.send(new DeleteObjectCommand(params));
        } catch {
            throw new S3DeleteError()
        }
    }
    
    /**
   * Converts a readable stream to a buffer.
   * @param {Readable} stream - The readable stream to convert.
   * @returns {Promise<Buffer>}
   */
  private async streamToBuffer(stream: Readable): Promise<Buffer> {
    return new Promise<Buffer>((resolve, reject) => {
      const chunks: any[] = [];
      stream.on("data", (chunk) => chunks.push(chunk));
      stream.on("error", (err) => reject(err));
      stream.on("end", () => resolve(Buffer.concat(chunks)));
    });
  }
}