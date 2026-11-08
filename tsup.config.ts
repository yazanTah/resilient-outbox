import { defineConfig } from 'tsup';

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'react/index': 'src/react/index.ts',
    'adapters/index': 'src/adapters/index.ts'
  },
  format: ['cjs', 'esm'],
  // Hermes (React Native) and all evergreen browsers support ES2020
  target: 'es2020',
  tsconfig: 'tsconfig.build.json',
  dts: true,
  clean: true,
  splitting: false,
  sourcemap: true,
  minify: false,
  external: ['react']
});
