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
Every file is written to a temporary sibling and renamed into place, so a
crash never leaves half a block or half a block list. A block is written
only if it is not there yet; `setBlobBlocks` reads the blocks back and
refuses a list that does not hash to the blob's id.
