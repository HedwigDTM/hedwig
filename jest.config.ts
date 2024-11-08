// jest.config.ts
export default {
  preset: 'ts-jest', // Use ts-jest preset
  testEnvironment: 'node', // Set the test environment to node
  moduleFileExtensions: ['ts', 'js'], // Specify file extensions Jest should work with
  testMatch: ['**/src/**/*.test.ts'], // Define the pattern for test files
  transform: {
    '^.+\\.ts$': 'ts-jest', // Use ts-jest to transform TypeScript files
  },
};
