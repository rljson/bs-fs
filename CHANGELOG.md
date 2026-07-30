# Changelog

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
