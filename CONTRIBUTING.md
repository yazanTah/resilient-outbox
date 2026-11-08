# Contributing

Thanks for helping. This library sits on the path of users' data, so correctness beats features.

## Setup

```bash
nvm use        # Node 22, see .nvmrc
npm ci
npm run verify # lint, format, typecheck, tests with coverage, build, package checks
npm run test:e2e # cross-tab behaviour in real Chromium tabs (needs `npx playwright install chromium`)
```

## Ground rules

- **Every behaviour change needs a test**, including its failure paths: storage that throws,
  executors that hang, app restarts (a new `OutboxQueue` over the same storage), and flushes
  interrupted by `remove`, `clear` or `destroy`. `tests/helpers` has a `FlakyStorage` for this.
- Changes to coordination need a test with two instances sharing storage and a `MemoryCoordinator`
  (`tests/core/crosstab.test.ts`), and should keep `npm run test:e2e` green.
- Coverage is enforced at 95% for statements, branches, functions and lines.
- Keep runtime dependencies at zero.
- Public API changes need README and `CHANGELOG.md` (**Unreleased**) updates.
- Use [Conventional Commits](https://www.conventionalcommits.org/) (`fix:`, `feat:`, `docs:` …).

## Releasing (maintainers)

1. Move **Unreleased** entries in `CHANGELOG.md` under the new version.
2. `npm version <patch|minor|major>` and push the tag.
3. The `Release` workflow verifies, publishes to npm with provenance, and creates a GitHub release.
   It needs an `NPM_TOKEN` repository secret.
