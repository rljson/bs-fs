<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Unlisted blocks are collected, and ids are checked before they become paths

## Motivation

- Since blobs are stored as blocks, `deleteBlob` removes only the blob's
  file: its blocks stay, because other blobs may contain them. Nothing ever
  removed a block that no blob lists any more, so a store that deletes
  blobs grew without end.
- A store with a disk budget has to know how much it holds. `BsFs` could not
  say.
- Every blob and block id became part of a path unchecked. An id such as
  `../../x` could read, change or delete a file outside the store.

## Strategy

- Collect garbage on request, not on its own: the caller decides when a
  sweep runs.
- Err on the side of keeping. A block is removed only when no blob file
  lists it and it was last stored longer ago than a grace period.
- Count the usage from the files on disk, so it is right after a restart.
- Accept only the characters of the ids the store makes itself, and check
  every id before it touches the disk.

## Collecting unlisted blocks

- `collectGarbage({ graceMs })` reads every blob file, collects the blocks
  they list, and removes the other block files once they were last stored
  more than `graceMs` ago — ten minutes by default
  (`BsFs.defaultGraceMs`). Directories it leaves empty go too.
- It resolves to `{ removedBlocks, freedBytes }`.
- Storing a block that is already on disk sets its modification time to
  now (`setBlob`, `setBlock`, and `setBlobBlocks` for the blocks it lists).
  The grace period counts from there, so a block on its way into a new blob
  list is not removed under the writer's feet.
- A blob file that cannot be read makes the sweep reject before anything is
  removed: an unreadable list could hide blocks still in use.
- The blocks of the blobs this instance is writing stay whatever their age.
  Storing a block and removing it never overlap, and one sweep runs at a
  time; a second call waits for the first.

## Reporting the usage

- `usage()` resolves to `{ blobs, blocks, bytes }`: the blob files, the
  block files, and the bytes the block files take on disk.
- Unlisted blocks count until `collectGarbage` removes them; staged files
  (`*.tmp`) do not.

## Checking ids — a security fix

- A blob or block id must be plain: one or more letters, digits, `-` and
  `_`. An id with `/`, `\`, `.` or any other character, or an empty one,
  never becomes a path.
- `blobExists` and `blockExists` answer `false` for such an id without
  touching the disk.
- `getBlob`, `getBlobStream`, `getBlobBlocks`, `getBlobProperties`,
  `getBlock`, `deleteBlob`, `generateSignedUrl` and `setBlobBlocks` reject
  with `Invalid blob id: "<id>"` or `Invalid block id: "<id>"`. So does
  reading a blob whose file lists such a block id.
- `setBlobBlocks` checks all ids before it writes anything.

## New types

- `CollectGarbageOptions`, `CollectGarbageResult` and `BsFsUsage` are
  exported.

## Result

- A store can now keep to a budget: it deletes blobs, collects their
  blocks, and reads its usage back.
- No id given from outside reaches a path outside the store.
- 100 % coverage, including a sweep while `setBlob` is still writing and
  two sweeps asked for at once.

## Open Points

- The sweep reads every blob file. For a store with very many blobs it is a
  long run; there is no index of the blocks in use yet.
- Garbage is collected only when the caller asks.
