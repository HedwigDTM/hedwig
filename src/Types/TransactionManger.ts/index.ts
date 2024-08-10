import { S3RollbackClient } from "../../S3Client/S3Client";

export type RollbackableClients = {
    S3Client: S3RollbackClient
}
export type TransactionCallbackFunction = (clients: Partial<RollbackableClients>) => Promise<void>;
