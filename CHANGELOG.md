# Changelog

## [Unreleased]

### Developed in gg tickets

- **Developed in gg tickets.** The repo carries the rljson DNA (`@rljson/dna-rljson`): gg's `/gg-*` skills, the guides and the quick check (`.github/workflows/quick_check.yaml`).
- **Pull requests are checked by the quick check** (`gg one did commit`: every commit went through gg's local lint, tests and coverage). `Build and Test` runs on `main` after the merge.
- **`install-conformance-tests.js`** takes the conformance tests from a linked sibling checkout, which has no `dist/`, by running that checkout's `deploy-conformance-tests.js`.
- **`tsconfig.json` lists `node` in `types`:** TypeScript 6 includes no `@types` by default.
- **vitest also runs `test/**/*.test.ts`,** where gg's release writes the version test.
- **Dependencies raised to their newest versions** (TypeScript 6 among them).

### Releases go through gg

**What changed for contributors.** The CI no longer publishes:
`.github/workflows/publish.yml` is disabled, and a release happens only when
the owner runs `gg one do publish`, which merges first and uploads only what
`main` holds (`--merge-only` merges without a release). CLAUDE.md describes
the whole workflow through gg — `gg one do create ticket`, `gg one do
commit`, `gg one do push`, `gg one do publish`. The scripts gg replaces —
`create-branch.js`, `push-branch.js`, `wait-for-pr.js`, `publish-to-npm.js`,
`add-version-tag.js` — are removed.

## [0.0.5]

### `setBlob` writes a stream through to disk

A stream handed to `setBlob` used to be collected into a Buffer first, because
the store is content-addressed and the path a blob belongs at is derived from its
own hash — so the destination is unknown until the last byte has been read. That
cost the whole file in RAM twice over: once for the Buffer, and again inside
`hshBuffer`, which copies its input into a padded working buffer.

The bytes now land in a temp file while a running SHA-256 consumes them, and the
finished digest decides where that file is moved.

- The incremental digest agrees exactly with `hshBuffer` — SHA-256 in base64url,
  truncated to 22 characters — verified across the block boundaries where a
  padding mistake would show (0, 1, 55, 56, 63, 64, 65 bytes) and across split
  feeds. They have to agree: a blob stored under a different id is one that
  deduplication can never find and that every existing reference misses.
- The staged file is removed on both outcomes. A re-send of an already-stored
  blob takes the dedup path, and that path used to be able to leak a full copy of
  the file.
- **Removed** `toBuffer`'s stream branch, now dead, and narrowed its parameter to
  `Buffer | string` so a third shape has to choose a path rather than getting the
  buffering one by default.

Requires `@rljson/bs` 0.0.27.

## [0.0.3]

- `BsFs.setBlob` now writes the blob and its metadata **atomically** (stage in a
  sibling `.tmp`, then `rename` over the target), so a crash mid-write can never
  leave a half-written, hash-mismatched blob. Concurrent writes of identical
  content (content-addressed → same target) are tolerated: if a peer's rename
  won the race, an existing target counts as success.
- `BsFs.getBlob` range requests now read **only the requested window** off disk
  via a positional `fileHandle.read`, instead of loading the whole (possibly
  multi-GB) blob into RAM just to slice it. Exclusive-end `[start, end)`
  semantics unchanged; a missing `end` reads to the blob end and an `end` past
  the blob size is clamped.

## [0.0.1]

Initial commit.
