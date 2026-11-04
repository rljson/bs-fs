<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Blobs on disk are stored as blocks

## Motivation

- `BsFs` stored every blob as one file, beside a small metadata file. Two
  versions of a large file that differ in one byte took the space of both,
  in full.
- `@rljson/bs` now stores every blob as content-defined blocks and moves
  only blocks between stores. A store on disk has to keep blocks too, or
  every reader would have to fetch a whole file again for a small change.

## Strategy

- Keep each block once, in a file of its own, named by its hash.
- Keep each blob as a small JSON file that lists its blocks.
- Write everything through a temporary file and a rename, as before, so a
  crash never leaves half a block or half a block list.
- Leave the old layout behind: this version does not read it, and offers a
  call that removes it.

## The layout

```text
<base>/
  blocks/ab/cd/ef/gh/<blockId>       the bytes of one block, stored once
  blobs/ab/cd/ef/gh/<blobId>.json    { blobId, size, createdAt, blocks }
```

- The subdirectories are the first eight characters of the id, two each.
- `setBlob` cuts a stream into blocks as it arrives and never holds it
  whole; a block that is already on disk is not written again.
- `getBlob` reads only the blocks a range touches; `getBlobStream` streams
  the blocks in order and checks the whole blob at its end.

## The block calls

- `getBlobBlocks`, `getBlock`, `blockExists`, `setBlock` and
  `setBlobBlocks`, as the `Bs` interface defines them.
- `setBlobBlocks` reads the blocks back and refuses a list that does not
  hash to the blob's id.
- `deleteBlob` deletes the blob's file. Its blocks stay: other blobs may
  contain them.

## Removing the old layout

- `removeOldLayout()` deletes the directories of two characters directly
  under the base directory — where the layout before blocks kept one file
  per blob — and nothing else.
- Files an application keeps beside the store, a database or a cache, stay
  where they are; so do `blocks/` and `blobs/`.

## Dependency updates

- `@rljson/bs` with blocks: the `Bs` interface, `writeBlocks`, `readBlocks`,
  `streamBlocks`, `verifyBlob` and `ContentId` come from there.

## Result

- A second version of a 60 MiB file that differs in one kilobyte in its
  middle adds 1.5 MB on disk, not another 60 MiB.
- 100 tests, 100 % coverage, including the conformance suite of
  `@rljson/bs` with its block tests.

## Open Points

- Nothing removes blocks that no blob lists any more.
- Blobs stored in the old layout cannot be read; `removeOldLayout` is the
  way to free their space.
