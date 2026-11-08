# Security Policy

## Supported versions

The latest minor release receives security fixes.

## Reporting a vulnerability

Please **do not** open a public issue. Use GitHub's
[private vulnerability reporting](https://github.com/yazanTah/resilient-outbox/security/advisories/new)
instead. You can expect an acknowledgement within 72 hours.

## Scope notes

- Payloads are persisted in plain text by the storage adapter you choose. Don't enqueue secrets
  (tokens, passwords) in payloads — attach credentials in the executor instead — or use an
  encrypted store such as MMKV with an encryption key.
- Idempotency keys are only as unpredictable as the runtime's random source. See the README's
  Limitations section.
