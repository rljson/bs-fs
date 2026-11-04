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
contain them. `removeOldLayout()` deletes what the layout before blocks left
in the base directory (one file per blob, in directories of two characters),
which this version cannot read.

## Example

[src/example.ts](src/example.ts)
