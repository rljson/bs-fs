<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Architecture

## Layout

```text
<base>/
  blocks/ab/cd/ef/gh/<blockId>       the bytes of one block, stored once
  blobs/ab/cd/ef/gh/<blobId>.json    { blobId, size, createdAt, blocks }
```

The subdirectories are the first eight characters of the id, two each.
An id becomes a path only if it is plain — letters, digits, `-` and `_` —
so no path leaves the store.
Every file is written to a temporary sibling and renamed into place, so a
crash never leaves half a block or half a block list. A block is written
only if it is not there yet; `setBlobBlocks` reads the blocks back and
refuses a list that does not hash to the blob's id.

## Garbage collection

`collectGarbage` marks, then sweeps:

1. **Mark.** Read every `blobs/**/*.json` and collect the block ids they
   list. One file that cannot be read stops the call before anything is
   removed.
2. **Sweep.** Walk `blocks/`. A block file goes only if no blob file listed
   it, no blob this instance is writing lists it, and it was last modified
   before the grace period. Its directories go with it while they are empty.

Three rules keep the blocks a blob needs:

- **Every store sets the modification time.** Storing a block that exists
  already sets it to now (`utimes`); `setBlobBlocks` does the same for the
  blocks it lists. The grace period counts from there.
- **Storing a block and removing it never overlap.** Within one instance,
  each operation on a block waits for the earlier ones on the same block. A
  sweep that read a block's time and decided to remove it finishes first; the
  store that follows writes the block again.
- **A blob being written keeps its blocks.** `setBlob` and `setBlobBlocks`
  register each block before they store or read it, and keep it registered
  until the sweeps that run when the blob's file is written are done: such a
  sweep may have read the blob files before that file existed.

Other instances on the same directory are protected by the grace period
only. A block another instance stores again in the instant between a sweep
reading its time and removing it is lost, so let one instance write a
directory that is swept. Removing an empty directory can make another
instance's write of a block into that directory fail; that write rejects.
