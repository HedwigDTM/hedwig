import { expect, test } from 'vitest';
import { S3Client as AWSS3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { mock } from 'vitest-mock-extended';
import { S3Params, S3Client } from '../../src/S3Client/S3Client';
import { S3RollbackStrategy } from '../../src/S3Client/S3RollbackStrategy';

const mockS3 = mock<AWSS3Client>();

// Test case: Successful upload
test('S3Client_PutObject_INMEMORY', async () => {
    const params: S3Params = {
        Bucket: 'test-bucket',
        Key: 'test',
        Body: Buffer.from('test', 'utf-8'),
    };
    
    const client = new S3Client('1', mockS3, S3RollbackStrategy.IN_MEMORY);
    expect(() => client.putObject(params)).not.toThrow();
});