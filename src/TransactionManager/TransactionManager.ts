import AWS, { S3ClientConfig } from "@aws-sdk/client-s3";
import InvocationError from "../RollbackableClient/InvokeError";
import RollbackableClient from "../RollbackableClient/RollbackableClient";
import S3Client from "../S3Client/S3Client";
import { v4 as uuidv4 } from "uuid";
import { S3RollbackStrategy } from "../S3Client/S3RollbackStrategy";

/**
 * TransactionManager is responsible for managing distributed transactions
 * across multiple services or data sources. It allows the user to define
 * a sequence of actions, ensuring that all actions are either fully completed
 * or rolled back in case of any failure.
 */
export default class TransactionManager {
  private s3Config: S3ClientConfig & { rollbackStrategy: S3RollbackStrategy };

  constructor() {
    this.s3Config = { rollbackStrategy: S3RollbackStrategy.IN_MEMORY };
  }

  public setS3Config(
    config: S3ClientConfig & { rollbackStrategy: S3RollbackStrategy }
  ): void {
    this.s3Config = { ...this.s3Config, ...config };
  }

  /**
   * Executes a transaction with the given actions.
   * @param callback - A callback function that receives an object with the clients.
   */
  public async transaction(
    callback: (clients: { S3Client: S3Client }) => void
  ): Promise<void> {
    const transactionID = uuidv4();

    const clients: { S3Client: S3Client } = {
      S3Client: new S3Client(
        transactionID,
        new AWS.S3Client(this.s3Config),
        this.s3Config.rollbackStrategy
      ),
    };

    callback(clients);
    Object.values(clients).forEach((client) => {
      try {
        client.invoke();
      } catch (error: any) {
        client.rollback();
        throw error;
      }
    });
  }
}
