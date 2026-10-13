# Contributing

Thanks for your interest in muse-bridge-rs. This is a small, deliberately
minimal project — contributions that keep it that way are the most welcome.

## Ground rules

- **YAGNI first.** Don't build what isn't needed yet; prefer the standard
  library and existing dependencies over new ones.
- **Never trade away**: input validation, error handling that prevents data
  loss, security properties (hashed keys, role checks), or accessibility of
  the operator tooling.
- **Evidence over claims.** A change is done when it is verified — run the
  e2e suites and say what you ran.

## Getting set up

```bash
cargo build --release
cargo clippy --all-targets   # must stay warning-free
cargo fmt --check
./scripts/e2e_test.sh
./scripts/e2e_stream.sh
```

The e2e scripts create their own throwaway keys and database; they never
touch a production `bridge.db`.

## Making changes

1. Fork and branch from `main`.
2. Keep commits small and messages descriptive (what + why).
3. If you change behavior, update `README.md`, `docs/architecture.md`, and
   `CHANGELOG.md` in the same PR.
4. Never commit secrets, key files, databases, or logs — the `.gitignore`
   blocks the usual paths, and PRs are checked for them.

## Reporting bugs

Open an issue with: what you ran, what you expected, what happened, and
relevant log lines (**redact keys and chat IDs first**). For security
issues, follow [SECURITY.md](SECURITY.md) instead of opening a public issue.
