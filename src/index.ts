import { S3RollbackStrategy } from "./S3Client/S3RollbackStrategy";
import TransactionManager from "./TransactionManager/TransactionManager";

const manager = new TransactionManager();
manager.setS3Config({
  region: "us-east-1",
  endpoint: "http://localhost:4566",
  rollbackStrategy: S3RollbackStrategy.IN_MEMORY,
});

(async () => {
  await manager.transaction(async ({ S3Client }) => {
    await S3Client.putObject({
      Bucket: "yoa",
      Key: "V",
      Body: Buffer.from("Noder", "utf-8"),
    });
    await S3Client.putObject({
      Bucket: "yoa",
      Key: "V",
      Body: Buffer.from("Neder", "utf-8"),
    });
  });
})();
