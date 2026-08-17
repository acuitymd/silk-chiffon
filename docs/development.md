# Development

The repository uses [`just`](https://github.com/casey/just) as its task runner. Run `just` to list recipes.

## Normal loop

Use a targeted `cargo nextest` command while developing one behavior. Before handing work off, run:

```bash
just verify
```

`just verify` applies Rust and Markdown formatting, regenerates `docs/CLI.md`, type-checks and lints every target with all features, runs the complete Nextest suite, validates local Markdown links, builds warning-denied rustdoc, checks every supported provider and storage feature composition, compiles ignored live-test targets, compiles benchmarks, runs the Python BQS benchmark-contract tests, and validates the root package inventory. Doctests are intentionally outside this gate.

CI runs the nonmutating `just ci` equivalent from a clean checkout, checks the declared Rust 1.95 minimum, and checks external links. The clean-checkout gate extracts `git archive HEAD`, builds all targets with all features from that archive, and checks package inventory there. This catches dependencies on ignored or untracked files.

## Verification layers

- `just test` runs the workspace suite with Cargo Nextest and all features. Ignored credentialed tests are compiled but not executed.
- `just docs-check` checks generated CLI docs, local Markdown targets and anchors, and warning-denied workspace rustdoc.
- `just check-provider-features` checks and runs registration tests for all 16 subsets of local bare paths, GCS, S3, and BigQuery, then checks the default feature set.
- `just check-storage-features` runs the storage tests with no backend, each individual backend, the local bare-path configuration, both cloud backends, and all features.
- `just check-live-targets` compiles provider-minimal GCS, S3, BQS fake-service, and soak targets without contacting a cloud service.
- `just check-benchmarks` compiles Rust benchmarks and runs the BQS benchmark harness's offline Python contract tests.
- `just check-package-inventory` verifies that the root package contains its required user-facing files and excludes build state and nested crate sources.
- `just check-clean-archive` is a clean-tree gate used by CI. It refuses a dirty worktree because uncommitted files cannot be represented by `git archive`.
- `just check-links` uses Lychee for external-link validation and requires the `lychee` executable.

## Credentialed tests

Normal validation is offline and credential-free. The ignored GCS, S3, and BigQuery tests require explicit acknowledgement variables and dedicated test resources. Never point them at backup or production prefixes. See [Cloud testing](cloud-testing.md) for setup, cost limits, ownership, and cleanup.

## Package boundary

`just check-package-inventory` checks what the root package would include; it is not a publication test. Internal path dependencies do not yet carry registry versions, so `cargo package` is expected to reject the workspace. Publication requires a deliberate version and release policy for every internal crate.
