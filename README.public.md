<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# @rljson/bs-fs

A blob store on disk for `@rljson/bs`.

```typescript
const bs = new BsFs('./blobs');
const { blobId } = await bs.setBlob(content); // Buffer, string or stream
const { content } = await bs.getBlob(blobId);
```

Every blob is stored as blocks: each block once, in `blocks/`, and each blob
as a small JSON file in `blobs/` that lists its blocks. Two versions of a file
that share most of their bytes share most of their blocks on disk. A stream
is cut into blocks as it arrives and never held whole.

Deleting a blob deletes its JSON file; its blocks stay, since other blobs may
contain them. `collectGarbage()` removes the blocks no blob lists any more.
`removeOldLayout()` deletes what the layout before blocks left in the base
directory (one file per blob, in directories of two characters), which this
version cannot read.

## Collect garbage, measure the usage

```typescript
const { removedBlocks, freedBytes } = await bs.collectGarbage();
const { blobs, blocks, bytes } = await bs.usage();
```

`collectGarbage({ graceMs })` reads every blob file, then removes each block
file that no blob file lists and that was last stored more than `graceMs`
ago (default `BsFs.defaultGraceMs`, ten minutes), and the directories it
leaves empty.

- **The grace period** counts from when a block was last stored. `setBlob`,
  `setBlock` and `setBlobBlocks` set the modification time of every block they
  store or list to now, also of a block stored already. It keeps the blocks of
  a blob stored block by block with `setBlock` until `setBlobBlocks` lists
  them, and the blocks another instance on the same directory writes: make it
  longer than such a blob takes.
- **The blobs this instance writes** (`setBlob`, `setBlobBlocks`) keep their
  blocks whatever the grace period, also against a sweep that read the blob
  files before their file was written.
- **`graceMs: 0`** removes every block no blob lists yet, also the blocks a
  `setBlock` caller or another instance is about to list. A negative or `NaN`
  grace period rejects.
- **A blob file that cannot be read** — no JSON, or no list of blocks — makes
  the call reject before anything is removed. The error names the file:
  repair or delete it.
- **A file removed meanwhile** is skipped. Any other error of the file system
  rejects; what was removed until then stays removed.
- **One sweep at a time:** a call made while one runs starts after it.

## Ids

A blob or block id is plain: one or more letters, digits, `-` and `_`, like
every id the store makes. `blobExists` and `blockExists` answer `false` for
any other id; every other call rejects with `Invalid blob id: "<id>"` or
`Invalid block id: "<id>"`. Such an id never becomes a path.

`usage()` counts the blob files, the block files and the bytes of the block
files. The blocks of a deleted blob count until `collectGarbage()` removes
them; staged files (`*.tmp`) do not count. A file removed meanwhile is
skipped; any other error of the file system rejects.

## Example

[src/example.ts](src/example.ts)
