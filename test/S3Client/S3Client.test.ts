import {
  S3Client as AWSS3Client,
  PutObjectCommand,
  HeadObjectCommand,
  GetObjectCommand,
  CopyObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { describe, expect, it, vi } from "vitest";
import { S3RollbackClient, S3Params } from "../../src/S3Client/S3Client";
import { Readable } from "stream";
import { S3RollbackStrategyType } from "../../src/Types/S3/S3RollBackStrategy";

// Create mock stream
const mockStream = new Readable();
mockStream.push("hello");
mockStream.push("world");
mockStream.push(null);

// Create mock aws connection
const mockS3 = new AWSS3Client({});
const mockSendFileDoesntExists = vi.fn((command) => {
  if (command instanceof PutObjectCommand) {
    return Promise.resolve();
  } else if (command instanceof HeadObjectCommand) {
    return Promise.reject({});
  } else if (command instanceof GetObjectCommand) {
    return Promise.reject({});
  } else if (command instanceof CopyObjectCommand) {
    return Promise.reject({});
  } else if (command instanceof DeleteObjectCommand) {
    return Promise.reject({});
  } else {
    return Promise.reject(new Error("Unsupported command"));
  }
});
const mockSendFileExists = vi.fn((command) => {
  if (command instanceof PutObjectCommand) {
    return Promise.resolve();
  } else if (command instanceof HeadObjectCommand) {
    return Promise.resolve({ Body: mockStream });
  } else if (command instanceof GetObjectCommand) {
    return Promise.resolve({ Body: mockStream });
  } else if (command instanceof CopyObjectCommand) {
    return Promise.resolve({});
  } else if (command instanceof DeleteObjectCommand) {
    return Promise.resolve({});
  } else {
    return Promise.reject(new Error("Unsupported command"));
  }
});

describe("S3Client - put new object to S3", () => {
  mockS3.send = mockSendFileDoesntExists;
  const params: S3Params = {
    Bucket: "test-bucket",
    Key: "test",
    Body: Buffer.from("test", "utf-8"),
  };

  it("IN_MEMORY strategy", async () => {
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.IN_MEMORY
    );
    expect(async () => await client.putObject(params)).not.toThrow();
  });
  it("DUPLICATE strategy", async () => {
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.DUPLICATE_FILE
    );
    expect(async () => await client.putObject(params)).not.toThrow();
  });
});

describe("S3Client - put existing object to S3", () => {
  mockS3.send = mockSendFileExists;
  const params: S3Params = {
    Bucket: "test-bucket",
    Key: "test",
    Body: Buffer.from("test", "utf-8"),
  };

  it("IN_MEMORY strategy", async () => {
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.IN_MEMORY
    );
    expect(async () => await client.putObject(params)).not.toThrow();
  });
  it("DUPLICATE strategy", async () => {
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.DUPLICATE_FILE
    );
    expect(async () => await client.putObject(params)).not.toThrow();
  });
});

describe("S3Client - delete object from S3", () => {
  const params: S3Params = {
    Bucket: "test-bucket",
    Key: "test",
    Body: Buffer.from("test", "utf-8"),
  };

  it("File exsists, IN_MEMORY strategy", async () => {
    mockS3.send = mockSendFileExists;
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.IN_MEMORY
    );
    expect(async () => await client.deleteObject(params)).not.toThrow();
  });
  it("File exsists, DUPLICATE strategy", async () => {
    mockS3.send = mockSendFileExists;
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.DUPLICATE_FILE
    );
    expect(async () => await client.deleteObject(params)).not.toThrow();
  });
  it("File doesn't exists, IN_MEMORY strategy", async () => {
    mockS3.send = mockSendFileDoesntExists;
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.IN_MEMORY
    );
    expect(async () => await client.deleteObject(params)).rejects.toThrow(
    );
  });
  it("File doesn't exists, DUPLICATE strategy", async () => {
    mockS3.send = mockSendFileDoesntExists;
    const client: S3RollbackClient = new S3RollbackClient(
      "1",
      mockS3,
      S3RollbackStrategyType.DUPLICATE_FILE
    );
    expect(async () => await client.deleteObject(params)).rejects.toThrow();
  });
});
