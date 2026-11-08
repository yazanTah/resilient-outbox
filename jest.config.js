module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  testMatch: ['**/tests/**/*.test.ts', '**/tests/**/*.test.tsx'],
  moduleNameMapper: {
    '^(\\./.*)\\.js$': '$1',
    '^(\\.\\./.*)\\.js$': '$1'
  }
};
