import {
  S3Client as AWSS3Client,
  PutObjectCommand,
  HeadObjectCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import { describe, expect, it, vi } from "vitest";
import { S3Client, S3Params } from "../../src/S3Client/S3Client";
import { S3RollbackStrategy } from "../../src/S3Client/S3RollbackStrategy";
import { Readable } from "stream";

// Create mock stream
const mockStream = new Readable();
mockStream.push("hello");
mockStream.push("world");
mockStream.push(null);

// Create mock aws connection
const mockS3 = new AWSS3Client({});
const mockSend = vi.fn((command) => {
  if (command instanceof PutObjectCommand) {
    return Promise.resolve();
  } else if (command instanceof HeadObjectCommand) {
    return Promise.resolve({});
  } else if (command instanceof GetObjectCommand) {
    return Promise.resolve({ Body: mockStream });
  } else {
    return Promise.reject(new Error("Unsupported command"));
  }
});
mockS3.send = mockSend;

describe("S3Client - put an object to S3 successfully", () => {
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
});
