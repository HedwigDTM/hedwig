import {
  S3Client as AWSS3Client,
  PutObjectCommand,
  HeadObjectCommand,
  GetObjectCommand,
  CopyObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { describe, expect, it, vi } from "vitest";
import { S3Client, S3Params } from "../../src/S3Client/S3Client";
import { S3RollbackStrategy } from "../../src/S3Client/S3RollbackStrategy";
import { Readable } from "stream";
import { S3BackupError } from "../../src/S3Client/S3RollbackFactory";

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
    return Promise.resolve({});
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
    const client: S3Client = new S3Client(
      "1",
      mockS3,
      S3RollbackStrategy.IN_MEMORY
    );
    expect(async () => await client.putObject(params)).not.toThrow();
  });
  it("DUPLICATE strategy", async () => {
    const client: S3Client = new S3Client(
      "1",
      mockS3,
      S3RollbackStrategy.DUPLICATE_FILE
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
    const client: S3Client = new S3Client(
      "1",
      mockS3,
      S3RollbackStrategy.IN_MEMORY
    );
    expect(async () => await client.putObject(params)).not.toThrow();
  });
  it("DUPLICATE strategy", async () => {
    const client: S3Client = new S3Client(
      "1",
      mockS3,
      S3RollbackStrategy.DUPLICATE_FILE
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
    const client: S3Client = new S3Client(
      "1",
      mockS3,
      S3RollbackStrategy.IN_MEMORY
    );
    expect(async () => await client.deleteObject(params)).not.toThrow();
  });
  it("File exsists, DUPLICATE strategy", async () => {
    mockS3.send = mockSendFileExists;
    const client: S3Client = new S3Client(
      "1",
      mockS3,
      S3RollbackStrategy.DUPLICATE_FILE
    );
    expect(async () => await client.deleteObject(params)).not.toThrow();
  });
  it("File doesn't exists, IN_MEMORY strategy", async () => {
    mockS3.send = mockSendFileDoesntExists;
    const client: S3Client = new S3Client(
        "1",
        mockS3,
        S3RollbackStrategy.IN_MEMORY
      );
    expect(async () => await client.deleteObject(params)).rejects.toThrow(S3BackupError);
  });
});
