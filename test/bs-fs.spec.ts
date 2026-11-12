// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import {
  access,
  mkdir,
  readdir,
  readFile,
  rm,
  stat,
  symlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { basename, dirname, join, relative } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { ContentId } from '@rljson/bs';

import { BsFs } from '../src/bs-fs.ts';

describe('BsFs', () => {
  let bs: BsFs;
  const testDir = './test-bs-fs';

  /** Where a block's or a blob's file lies. */
  const pathOf = (kind: string, id: string, extension: string) =>
    join(
      testDir,
      kind,
      ...[0, 2, 4, 6].map((i) => id.substring(i, i + 2)),
      `${id}${extension}`,
    );

  /** Where a block's file lies. */
  const blockFile = (blockId: string) => pathOf('blocks', blockId, '');

  /** Sets a file's modification time `ms` back, an hour by default. */
  const age = async (path: string, ms = 60 * 60 * 1000) => {
    const time = new Date(Date.now() - ms);
    await utimes(path, time, time);
  };

  /** Every file below `dir` of the store, relative to `dir`, sorted. */
  const filesUnder = async (dir: string) => {
    const root = join(testDir, dir);
    const entries = await readdir(root, {
      recursive: true,
      withFileTypes: true,
    }).catch(() => []);
    return entries
      .filter((entry) => !entry.isDirectory())
      .map((entry) => relative(root, join(entry.parentPath, entry.name)))
      .sort();
  };

  beforeEach(async () => {
    bs = new BsFs(testDir);
    await bs.clear();
  });

  afterEach(async () => {
    await bs.clear();
  });

  describe('setBlob from a stream', () => {
    /** A web stream over `bufs`, the shape `setBlob` takes. */
    const streamOf = (bufs: Buffer[]): ReadableStream =>
      new ReadableStream({
        start(controller) {
          for (const b of bufs) controller.enqueue(new Uint8Array(b));
          controller.close();
        },
      });

    it('gives a stream the SAME blobId as the equivalent buffer', async () => {
      // The one thing that must never drift. The streaming path computes its
      // own digest incrementally instead of calling `hshBuffer` on a whole
      // Buffer, and a blob stored under a different id is a blob that
      // deduplication can never find and that every existing reference misses.
      const content = Buffer.from('the same bytes, either way');
      const viaBuffer = await bs.setBlob(content);
      await bs.clear();
      // Split across chunks on purpose: an incremental digest that mishandled a
      // feed boundary would still agree on a single-chunk stream.
      const viaStream = await bs.setBlob(
        streamOf([content.subarray(0, 7), content.subarray(7)]),
      );

      expect(viaStream.blobId).toBe(viaBuffer.blobId);
      expect(viaStream.size).toBe(content.length);
    });

    it('agrees with the buffer path at every SHA block boundary', async () => {
      // 64 bytes is a SHA-256 block, and 55/56 straddle the point where the
      // length padding needs an extra block. A padding mistake in the
      // incremental digest shows here and nowhere else.
      for (const size of [0, 1, 55, 56, 63, 64, 65, 200]) {
        const content = Buffer.alloc(size);
        for (let i = 0; i < size; i++) content[i] = (i * 7) % 256;
        const viaBuffer = await bs.setBlob(content);
        await bs.clear();
        const viaStream = await bs.setBlob(streamOf([content]));
        expect(viaStream.blobId, `size ${String(size)}`).toBe(viaBuffer.blobId);
        await bs.clear();
      }
    });

    it('reads back exactly what was streamed in', async () => {
      const content = Buffer.alloc(300 * 1024);
      for (let i = 0; i < content.length; i++) content[i] = i % 251;
      const chunks: Buffer[] = [];
      for (let at = 0; at < content.length; at += 64 * 1024) {
        chunks.push(content.subarray(at, at + 64 * 1024));
      }

      const { blobId } = await bs.setBlob(streamOf(chunks));
      const { content: back } = await bs.getBlob(blobId);
      expect(back.equals(content)).toBe(true);
    });

    it('deduplicates a stream against an already stored blob', async () => {
      const content = Buffer.from('stored once');
      const first = await bs.setBlob(content);
      const again = await bs.setBlob(streamOf([content]));

      expect(again.blobId).toBe(first.blobId);
      expect(again.size).toBe(first.size);
      expect(again.createdAt.getTime()).toBe(first.createdAt.getTime());
    });

    it('leaves no staged file behind, stored or deduplicated', async () => {
      // The temp file is written before the id is known, so both outcomes have
      // to clean up after themselves. A re-send of an existing blob taking the
      // dedup path used to be the one that could leak a full copy of the file.
      const content = Buffer.from('twice over');
      await bs.setBlob(streamOf([content]));
      await bs.setBlob(streamOf([content]));

      const { readdir } = await import('node:fs/promises');
      const entries = await readdir(testDir, { recursive: true });
      expect(entries.filter((e) => e.endsWith('.tmp'))).toEqual([]);
    });

    it('stores an empty stream', async () => {
      const props = await bs.setBlob(streamOf([]));
      expect(props.size).toBe(0);
      const { content } = await bs.getBlob(props.blobId);
      expect(content).toHaveLength(0);
    });
  });

  describe('setBlob', () => {
    it('should store a blob from string content', async () => {
      const content = 'Hello, World!';
      const props = await bs.setBlob(content);

      expect(props.blobId).toBeDefined();
      expect(props.size).toBe(Buffer.from(content).length);
      expect(props.createdAt).toBeInstanceOf(Date);
    });

    it('should store a blob from Buffer content', async () => {
      const content = Buffer.from('Binary data');
      const props = await bs.setBlob(content);

      expect(props.blobId).toBeDefined();
      expect(props.size).toBe(content.length);
    });

    it('should store a blob from ReadableStream', async () => {
      const content = 'Stream content';
      const stream = new ReadableStream({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(content));
          controller.close();
        },
      });

      const props = await bs.setBlob(stream);
      expect(props.blobId).toBeDefined();
      expect(props.size).toBe(Buffer.from(content).length);
    });

    it('should deduplicate identical content', async () => {
      const content = 'Duplicate content';
      const props1 = await bs.setBlob(content);
      const props2 = await bs.setBlob(content);

      expect(props1.blobId).toBe(props2.blobId);
      expect(props1.size).toBe(props2.size);

      const size = await bs.size();
      expect(size).toBe(1);
    });

    it('should create proper directory structure', async () => {
      const content = 'Test directory structure';
      const props = await bs.setBlob(content);
      const [{ blockId }] = await bs.getBlobBlocks(props.blobId);

      // A blob's file under blobs/, each block's under blocks/, both in
      // subdirectories from the first 8 chars of the id, 2 at a time
      await expect(
        access(pathOf('blobs', props.blobId, '.json')),
      ).resolves.toBeUndefined();
      await expect(
        access(pathOf('blocks', blockId, '')),
      ).resolves.toBeUndefined();
    });

    it('should handle empty content', async () => {
      const content = '';
      const props = await bs.setBlob(content);

      expect(props.blobId).toBeDefined();
      expect(props.size).toBe(0);
    });

    it('should handle large content', async () => {
      const largeContent = 'x'.repeat(1024 * 1024); // 1MB
      const props = await bs.setBlob(largeContent);

      expect(props.size).toBe(1024 * 1024);
    });
  });

  describe('getBlob', () => {
    it('should retrieve blob content', async () => {
      const content = 'Test content';
      const props = await bs.setBlob(content);

      const result = await bs.getBlob(props.blobId);
      expect(result.content.toString('utf8')).toBe(content);
      expect(result.properties.blobId).toBe(props.blobId);
    });

    it('should handle range requests', async () => {
      const content = '0123456789';
      const props = await bs.setBlob(content);

      const result = await bs.getBlob(props.blobId, {
        range: { start: 2, end: 5 },
      });

      expect(result.content.toString('utf8')).toBe('234');
    });

    it('range with only a start reads to the end of the blob', async () => {
      const props = await bs.setBlob('0123456789');
      const result = await bs.getBlob(props.blobId, { range: { start: 7 } });
      expect(result.content.toString('utf8')).toBe('789');
    });

    it('a zero-length range yields empty content', async () => {
      const props = await bs.setBlob('0123456789');
      const result = await bs.getBlob(props.blobId, {
        range: { start: 4, end: 4 },
      });
      expect(result.content.length).toBe(0);
    });

    it('a range end past the blob size is clamped to the end', async () => {
      const props = await bs.setBlob('0123456789');
      const result = await bs.getBlob(props.blobId, {
        range: { start: 8, end: 99 },
      });
      expect(result.content.toString('utf8')).toBe('89');
    });

    it('should throw error for non-existent blob', async () => {
      await expect(bs.getBlob('non-existent-id')).rejects.toThrow(
        'Blob not found: non-existent-id',
      );
    });

    it('should preserve binary content', async () => {
      const binaryContent = Buffer.from([0, 1, 2, 255, 254, 253]);
      const props = await bs.setBlob(binaryContent);

      const result = await bs.getBlob(props.blobId);
      expect(result.content).toEqual(binaryContent);
    });
  });

  describe('getBlobStream', () => {
    it('should return a readable stream', async () => {
      const content = 'Stream test content';
      const props = await bs.setBlob(content);

      const stream = await bs.getBlobStream(props.blobId);
      expect(stream).toBeDefined();

      // Read the stream
      const reader = stream.getReader();
      const chunks: Uint8Array[] = [];
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
      }

      const result = Buffer.concat(chunks).toString('utf8');
      expect(result).toBe(content);
    });

    it('should throw error for non-existent blob', async () => {
      await expect(bs.getBlobStream('non-existent-id')).rejects.toThrow(
        'Blob not found: non-existent-id',
      );
    });
  });

  describe('deleteBlob', () => {
    it('should delete an existing blob', async () => {
      const content = 'To be deleted';
      const props = await bs.setBlob(content);

      await bs.deleteBlob(props.blobId);

      await expect(bs.getBlob(props.blobId)).rejects.toThrow('Blob not found');
      expect(await bs.blobExists(props.blobId)).toBe(false);
    });

    it('should throw error when deleting non-existent blob', async () => {
      await expect(bs.deleteBlob('non-existent-id')).rejects.toThrow(
        'Blob not found: non-existent-id',
      );
    });

    it('should remove the blob file and keep its blocks', async () => {
      const content = 'Delete test';
      const props = await bs.setBlob(content);
      const [{ blockId }] = await bs.getBlobBlocks(props.blobId);
      const blobPath = pathOf('blobs', props.blobId, '.json');
      const blockPath = pathOf('blocks', blockId, '');

      await expect(access(blobPath)).resolves.toBeUndefined();

      await bs.deleteBlob(props.blobId);

      // The blob is gone; its block stays, other blobs may contain it
      await expect(access(blobPath)).rejects.toThrow();
      await expect(access(blockPath)).resolves.toBeUndefined();
    });
  });

  describe('blobExists', () => {
    it('should return true for existing blob', async () => {
      const content = 'Exists test';
      const props = await bs.setBlob(content);

      expect(await bs.blobExists(props.blobId)).toBe(true);
    });

    it('should return false for non-existent blob', async () => {
      expect(await bs.blobExists('non-existent-id')).toBe(false);
    });

    it('should return false after deletion', async () => {
      const content = 'Deleted test';
      const props = await bs.setBlob(content);

      expect(await bs.blobExists(props.blobId)).toBe(true);
      await bs.deleteBlob(props.blobId);
      expect(await bs.blobExists(props.blobId)).toBe(false);
    });
  });

  describe('getBlobProperties', () => {
    it('should return blob properties', async () => {
      const content = 'Properties test';
      const props = await bs.setBlob(content);

      const retrieved = await bs.getBlobProperties(props.blobId);
      expect(retrieved.blobId).toBe(props.blobId);
      expect(retrieved.size).toBe(props.size);
      expect(retrieved.createdAt).toBeInstanceOf(Date);
    });

    it('should throw error for non-existent blob', async () => {
      await expect(bs.getBlobProperties('non-existent-id')).rejects.toThrow(
        'Blob not found: non-existent-id',
      );
    });
  });

  describe('listBlobs', () => {
    it('should list all blobs', async () => {
      await bs.setBlob('Content 1');
      await bs.setBlob('Content 2');
      await bs.setBlob('Content 3');

      const result = await bs.listBlobs();
      expect(result.blobs).toHaveLength(3);
      expect(result.continuationToken).toBeUndefined();
    });

    it('should return empty list when no blobs exist', async () => {
      const result = await bs.listBlobs();
      expect(result.blobs).toHaveLength(0);
    });

    it('should filter by prefix', async () => {
      const props1 = await bs.setBlob('Content A');
      await bs.setBlob('Content B');
      await bs.setBlob('Content C');

      const prefix = props1.blobId.substring(0, 4);
      const result = await bs.listBlobs({ prefix });

      expect(result.blobs.length).toBeGreaterThanOrEqual(1);
      result.blobs.forEach((blob) => {
        expect(blob.blobId.startsWith(prefix)).toBe(true);
      });
    });

    it('should sort results by blobId', async () => {
      await bs.setBlob('A');
      await bs.setBlob('B');
      await bs.setBlob('C');

      const result = await bs.listBlobs();

      for (let i = 0; i < result.blobs.length - 1; i++) {
        expect(
          result.blobs[i]!.blobId.localeCompare(result.blobs[i + 1]!.blobId),
        ).toBeLessThanOrEqual(0);
      }
    });

    it('should handle pagination with maxResults', async () => {
      await bs.setBlob('Content 1');
      await bs.setBlob('Content 2');
      await bs.setBlob('Content 3');
      await bs.setBlob('Content 4');

      const result = await bs.listBlobs({ maxResults: 2 });

      expect(result.blobs).toHaveLength(2);
      expect(result.continuationToken).toBeDefined();
    });

    it('should paginate through all results', async () => {
      await bs.setBlob('Content 1');
      await bs.setBlob('Content 2');
      await bs.setBlob('Content 3');

      const page1 = await bs.listBlobs({ maxResults: 2 });
      expect(page1.blobs).toHaveLength(2);
      expect(page1.continuationToken).toBeDefined();

      const page2 = await bs.listBlobs({
        maxResults: 2,
        continuationToken: page1.continuationToken,
      });
      expect(page2.blobs).toHaveLength(1);
      expect(page2.continuationToken).toBeUndefined();

      // Ensure no duplicates
      const allBlobIds = [
        ...page1.blobs.map((b) => b.blobId),
        ...page2.blobs.map((b) => b.blobId),
      ];
      const uniqueIds = new Set(allBlobIds);
      expect(uniqueIds.size).toBe(allBlobIds.length);
    });
  });

  describe('generateSignedUrl', () => {
    it('should generate a signed URL for existing blob', async () => {
      const content = 'URL test';
      const props = await bs.setBlob(content);

      const url = await bs.generateSignedUrl(props.blobId, 3600);
      expect(url).toContain('fs://');
      expect(url).toContain(props.blobId);
      expect(url).toContain('expires=');
      expect(url).toContain('permissions=read');
    });

    it('should include custom permissions in URL', async () => {
      const content = 'URL test';
      const props = await bs.setBlob(content);

      const url = await bs.generateSignedUrl(props.blobId, 3600, 'delete');
      expect(url).toContain('permissions=delete');
    });

    it('should throw error for non-existent blob', async () => {
      await expect(
        bs.generateSignedUrl('non-existent-id', 3600),
      ).rejects.toThrow('Blob not found: non-existent-id');
    });
  });

  describe('clear', () => {
    it('should remove all blobs', async () => {
      await bs.setBlob('Content 1');
      await bs.setBlob('Content 2');
      await bs.setBlob('Content 3');

      let size = await bs.size();
      expect(size).toBe(3);

      await bs.clear();

      size = await bs.size();
      expect(size).toBe(0);
    });

    it('should handle clearing empty storage', async () => {
      await expect(bs.clear()).resolves.toBeUndefined();
    });

    it('should completely remove the base directory', async () => {
      await bs.setBlob('Test');
      await bs.clear();

      // Try to access the directory - it should not exist
      await expect(access(testDir)).rejects.toThrow();
    });
  });

  describe('size', () => {
    it('should return 0 for empty storage', async () => {
      expect(await bs.size()).toBe(0);
    });

    it('should return correct count after adding blobs', async () => {
      await bs.setBlob('Content 1');
      expect(await bs.size()).toBe(1);

      await bs.setBlob('Content 2');
      expect(await bs.size()).toBe(2);

      await bs.setBlob('Content 3');
      expect(await bs.size()).toBe(3);
    });

    it('should handle deduplication in count', async () => {
      await bs.setBlob('Same content');
      await bs.setBlob('Same content');
      await bs.setBlob('Same content');

      expect(await bs.size()).toBe(1);
    });

    it('should decrease after deletion', async () => {
      const props1 = await bs.setBlob('Content 1');
      const props2 = await bs.setBlob('Content 2');

      expect(await bs.size()).toBe(2);

      await bs.deleteBlob(props1.blobId);
      expect(await bs.size()).toBe(1);

      await bs.deleteBlob(props2.blobId);
      expect(await bs.size()).toBe(0);
    });
  });

  describe('metadata persistence', () => {
    it('should persist and restore metadata correctly', async () => {
      const content = 'Metadata test';
      const props = await bs.setBlob(content);

      // Read the blob file directly
      const metadata = JSON.parse(
        await readFile(pathOf('blobs', props.blobId, '.json'), 'utf8'),
      );

      expect(metadata.blobId).toBe(props.blobId);
      expect(metadata.size).toBe(props.size);
      expect(metadata.blocks).toEqual(await bs.getBlobBlocks(props.blobId));
      expect(new Date(metadata.createdAt).getTime()).toBe(
        props.createdAt.getTime(),
      );
    });

    it('skips a blob file it cannot read, and staged files', async () => {
      const props = await bs.setBlob('Readable');
      await writeFile(join(testDir, 'blobs', 'broken.json'), '{');
      await writeFile(join(testDir, 'blobs', 'staged.json.x.tmp'), '{}');
      expect((await bs.listBlobs()).blobs.map((b) => b.blobId)).toEqual([
        props.blobId,
      ]);
    });
  });

  describe('removeOldLayout', () => {
    it('deletes the directories of the layout before blocks, and nothing else', async () => {
      const { blobId } = await bs.setBlob('kept');
      // The layout before blocks: ab/cd/ef/gh/<id>.txt and <id>.meta.json
      const oldDir = join(testDir, 'ab', 'cd', 'ef', 'gh');
      await mkdir(oldDir, { recursive: true });
      await writeFile(join(oldDir, 'abcdefgh.txt'), 'old');
      await writeFile(join(oldDir, 'abcdefgh.meta.json'), '{}');
      await mkdir(join(testDir, 'Zz'), { recursive: true });
      // Kept beside the store by its user
      await writeFile(join(testDir, 'db.sqlite'), 'db');
      await writeFile(join(testDir, 'xy'), 'a file, not a directory');

      expect(await bs.removeOldLayout()).toBe(2);

      expect((await readdir(testDir)).sort()).toEqual(
        ['blobs', 'blocks', 'db.sqlite', 'xy'].sort(),
      );
      expect((await bs.getBlob(blobId)).content.toString()).toBe('kept');
      expect(await bs.removeOldLayout()).toBe(0);
    });

    it('deletes nothing when the base directory does not exist', async () => {
      expect(await new BsFs('./test-bs-fs-missing').removeOldLayout()).toBe(0);
    });
  });

  describe('setBlock(content)', () => {
    it('stores a block once when two calls store it at the same time', async () => {
      const content = Buffer.from('twice at once');
      const [first, second] = await Promise.all([
        bs.setBlock(content),
        bs.setBlock(content),
      ]);

      expect(second).toEqual(first);
      expect(await filesUnder('blocks')).toEqual([
        relative(join(testDir, 'blocks'), blockFile(first.blockId)),
      ]);
    });

    it('sets the modification time of a block stored already to now', async () => {
      const content = Buffer.from('stored before');
      const { blockId } = await bs.setBlock(content);
      await age(blockFile(blockId));

      await bs.setBlock(content);

      const { mtimeMs } = await stat(blockFile(blockId));
      expect(mtimeMs).toBeGreaterThan(Date.now() - 60 * 1000);
    });
  });

  describe('setBlobBlocks(blobId, blocks)', () => {
    it('sets the modification time of the blocks it lists to now', async () => {
      const content = Buffer.from('listed');
      const block = await bs.setBlock(content);
      await age(blockFile(block.blockId));

      await bs.setBlobBlocks(ContentId.of(content), [block]);

      const { mtimeMs } = await stat(blockFile(block.blockId));
      expect(mtimeMs).toBeGreaterThan(Date.now() - 60 * 1000);
    });
  });

  describe('usage()', () => {
    it('counts nothing in a store that holds nothing', async () => {
      expect(await bs.usage()).toEqual({ blobs: 0, blocks: 0, bytes: 0 });
    });

    it('counts the blob files, the block files and the bytes of the blocks', async () => {
      await bs.setBlob('hello');
      await bs.setBlob('hello');
      await bs.setBlob('hello, world');
      await bs.setBlock(Buffer.from('loose'));

      expect(await bs.usage()).toEqual({ blobs: 2, blocks: 3, bytes: 22 });
    });

    it('counts the blocks of a deleted blob until they are collected', async () => {
      const { blobId } = await bs.setBlob('deleted');
      const [{ blockId }] = await bs.getBlobBlocks(blobId);
      await bs.deleteBlob(blobId);
      expect(await bs.usage()).toEqual({ blobs: 0, blocks: 1, bytes: 7 });

      await age(blockFile(blockId));
      await bs.collectGarbage();

      expect(await bs.usage()).toEqual({ blobs: 0, blocks: 0, bytes: 0 });
    });

    it('counts no staged file', async () => {
      const { blobId } = await bs.setBlob('staged');
      const [{ blockId }] = await bs.getBlobBlocks(blobId);
      await writeFile(`${blockFile(blockId)}.x.tmp`, 'half a block');
      await writeFile(`${pathOf('blobs', blobId, '.json')}.x.tmp`, '{');

      expect(await bs.usage()).toEqual({ blobs: 1, blocks: 1, bytes: 6 });
    });

    it('skips a block file that is gone by the time it is measured', async () => {
      await mkdir(join(testDir, 'blocks'), { recursive: true });
      await symlink('nowhere', join(testDir, 'blocks', 'Gone'));

      expect(await bs.usage()).toEqual({ blobs: 0, blocks: 0, bytes: 0 });
    });

    it('fails when a directory of the store cannot be read', async () => {
      await mkdir(testDir, { recursive: true });
      await writeFile(join(testDir, 'blocks'), 'a file, not a directory');

      await expect(bs.usage()).rejects.toThrow('ENOTDIR');
    });
  });

  describe('collectGarbage(options)', () => {
    /** Stores a block no blob lists, last stored an hour ago. */
    const orphan = async (text = 'orphan') => {
      const block = await bs.setBlock(Buffer.from(text));
      await age(blockFile(block.blockId));
      return block;
    };

    it('removes the blocks no blob lists once they are older than the grace period', async () => {
      const kept = await bs.setBlob('kept');
      const gone = await bs.setBlob('gone');
      const [keptBlock] = await bs.getBlobBlocks(kept.blobId);
      const [goneBlock] = await bs.getBlobBlocks(gone.blobId);
      await bs.deleteBlob(gone.blobId);
      await age(blockFile(keptBlock.blockId));
      await age(blockFile(goneBlock.blockId));

      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 1,
        freedBytes: 4,
      });

      expect(await bs.blockExists(goneBlock.blockId)).toBe(false);
      expect((await bs.getBlob(kept.blobId)).content.toString()).toBe('kept');
      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 0,
        freedBytes: 0,
      });
    });

    it('keeps a block that another blob still lists', async () => {
      const shared = await bs.setBlock(Buffer.from('shared '));
      const own = await bs.setBlock(Buffer.from('own'));
      const both = await bs.setBlobBlocks(
        ContentId.of(Buffer.from('shared own')),
        [shared, own],
      );
      await bs.setBlobBlocks(ContentId.of(Buffer.from('shared ')), [shared]);
      await bs.deleteBlob(both.blobId);
      await age(blockFile(shared.blockId));
      await age(blockFile(own.blockId));

      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 1,
        freedBytes: 3,
      });
      expect(await bs.blockExists(shared.blockId)).toBe(true);
      expect(await bs.blockExists(own.blockId)).toBe(false);
    });

    it('keeps a block for ten minutes after it was stored, by default', async () => {
      expect(BsFs.defaultGraceMs).toBe(10 * 60 * 1000);
      const young = await bs.setBlock(Buffer.from('young'));
      const old = await bs.setBlock(Buffer.from('old'));
      await age(blockFile(young.blockId), 9 * 60 * 1000);
      await age(blockFile(old.blockId), 11 * 60 * 1000);

      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 1,
        freedBytes: 3,
      });
      expect(await bs.blockExists(young.blockId)).toBe(true);

      expect(await bs.collectGarbage({ graceMs: 8 * 60 * 1000 })).toEqual({
        removedBlocks: 1,
        freedBytes: 5,
      });
    });

    for (const graceMs of [-1, Number.NaN]) {
      it(`refuses a grace period of ${String(graceMs)}`, async () => {
        await expect(bs.collectGarbage({ graceMs })).rejects.toThrow(
          `graceMs must be 0 or more milliseconds, not ${String(graceMs)}`,
        );
      });
    }

    it('keeps an old block that setBlock stores again', async () => {
      // A blob stored block by block lists its blocks only at the end: a
      // block it shares with a deleted blob must not go in the meantime.
      const { blockId } = await orphan('stored again');

      await bs.setBlock(Buffer.from('stored again'));

      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 0,
        freedBytes: 0,
      });
      expect((await bs.getBlock(blockId)).toString()).toBe('stored again');
    });

    /**
     * Starts a `setBlob` of a stream that stays open, and waits until the
     * blob's first block is stored; then ages that block by an hour.
     * @returns The content, what `setBlob` returns, and what ends the stream.
     */
    const startWriting = async () => {
      // More than the largest block: the first block is stored while the
      // stream still waits for its end.
      const content = Buffer.alloc(5 * 1024 * 1024);
      for (let i = 0; i < content.length; i++) {
        content[i] = (i * 7 + (i >> 12)) % 256;
      }
      let close = () => {};
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array(content));
          close = () => controller.close();
        },
      });
      const isBlock = (file: string) => !basename(file).includes('.');

      const writing = bs.setBlob(stream);
      let stored = (await filesUnder('blocks')).filter(isBlock);
      while (stored.length === 0) {
        await new Promise((resolve) => setTimeout(resolve, 5));
        stored = (await filesUnder('blocks')).filter(isBlock);
      }
      for (const file of stored) await age(join(testDir, 'blocks', file));
      return { content, writing, close };
    };

    it('keeps the blocks of a blob setBlob is writing, whatever their age', async () => {
      const { content, writing, close } = await startWriting();

      expect(await bs.collectGarbage({ graceMs: 0 })).toEqual({
        removedBlocks: 0,
        freedBytes: 0,
      });

      close();
      const { blobId } = await writing;
      expect((await bs.getBlob(blobId)).content.equals(content)).toBe(true);
    });

    it('keeps the blocks of a blob whose file is written after it read the blob files', async () => {
      // The sweep is held while it reads a blob file, between reading the
      // blob files and removing blocks. A fifo cannot hold it everywhere:
      // Git's mkfifo on Windows makes a plain file, read at once.
      const slowFile = join(testDir, 'blobs', 'slow.json');
      await mkdir(join(testDir, 'blobs'), { recursive: true });
      await writeFile(slowFile, '{"blocks":[]}');
      let reading = () => {};
      const read = new Promise<void>((resolve) => (reading = resolve));
      let release = () => {};
      const released = new Promise<void>((resolve) => (release = resolve));
      const blobFiles = BsFs as unknown as {
        _blocksListedIn(path: string): Promise<string[]>;
      };
      const readBlobFile = blobFiles._blocksListedIn.bind(BsFs);
      const held = vi
        .spyOn(blobFiles, '_blocksListedIn')
        .mockImplementation(async (path) => {
          if (path === slowFile) {
            reading();
            await released;
          }
          return readBlobFile(path);
        });
      try {
        const { content, writing, close } = await startWriting();

        const sweep = bs.collectGarbage({ graceMs: 0 });
        await read; // The sweep reads the blob files now
        close();
        const { blobId } = await writing;
        release();

        expect(await sweep).toEqual({ removedBlocks: 0, freedBytes: 0 });
        expect((await bs.getBlob(blobId)).content.equals(content)).toBe(true);
      } finally {
        held.mockRestore();
      }
    });

    it('never removes a block once setBlock stored it again', async () => {
      // The sweep reads a block's modification time, then removes it: a
      // block stored again in between must stay.
      const contents = Array.from({ length: 300 }, (_, i) =>
        Buffer.from(`orphan ${String(i)}`),
      );
      for (const content of contents) await orphan(content.toString());

      let sweeping = true;
      const sweep = bs.collectGarbage().finally(() => {
        sweeping = false;
      });
      const lost = new Set<string>();
      while (sweeping) {
        // Old again, so the sweep may remove them; then stored again
        await Promise.all(
          contents.map((c) => age(blockFile(ContentId.of(c))).catch(() => {})),
        );
        const stored = await Promise.all(contents.map((c) => bs.setBlock(c)));
        for (const { blockId } of stored) {
          if (!(await bs.blockExists(blockId))) lost.add(blockId);
        }
      }
      await sweep;

      expect([...lost]).toEqual([]);
    });

    for (const text of [
      '{',
      'null',
      '{}',
      '{"blocks":{}}',
      '{"blocks":[null]}',
      '{"blocks":[{"blockId":7}]}',
    ]) {
      it(`removes nothing while a blob file holds ${text}`, async () => {
        const { blockId } = await orphan();
        const broken = join(testDir, 'blobs', 'broken.json');
        await mkdir(join(testDir, 'blobs'), { recursive: true });
        await writeFile(broken, text);

        await expect(bs.collectGarbage()).rejects.toThrow(
          `Cannot read the blob file ${broken}; no block was removed`,
        );
        expect(await bs.blockExists(blockId)).toBe(true);
      });
    }

    it('removes nothing when the blob files cannot be listed', async () => {
      const { blockId } = await orphan();
      await writeFile(join(testDir, 'blobs'), 'a file, not a directory');

      await expect(bs.collectGarbage()).rejects.toThrow('ENOTDIR');
      expect(await bs.blockExists(blockId)).toBe(true);
    });

    it('fails when the block files cannot be listed', async () => {
      await mkdir(testDir, { recursive: true });
      await writeFile(join(testDir, 'blocks'), 'a file, not a directory');

      await expect(bs.collectGarbage()).rejects.toThrow('ENOTDIR');
    });

    it('skips a blob file that is gone by the time it is read', async () => {
      await orphan();
      await mkdir(join(testDir, 'blobs'), { recursive: true });
      await symlink('nowhere', join(testDir, 'blobs', 'gone.json'));

      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 1,
        freedBytes: 6,
      });
    });

    it('skips a block file that is gone by the time it is removed', async () => {
      await mkdir(join(testDir, 'blocks'), { recursive: true });
      await symlink('nowhere', join(testDir, 'blocks', 'Gone'));

      expect(await bs.collectGarbage({ graceMs: 0 })).toEqual({
        removedBlocks: 0,
        freedBytes: 0,
      });
    });

    it('removes the directories a removed block leaves empty, and no other', async () => {
      await orphan('alone');
      const beside = await orphan('beside a staged file');
      const staged = `${blockFile(beside.blockId)}.x.tmp`;
      await writeFile(staged, 'staged');

      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 2,
        freedBytes: 25,
      });

      expect(await filesUnder('blocks')).toEqual([
        relative(join(testDir, 'blocks'), staged),
      ]);
      expect(await readdir(join(testDir, 'blocks'))).toEqual([
        beside.blockId.substring(0, 2),
      ]);
    });

    it('runs one sweep at a time: a second call runs after the first', async () => {
      await orphan();

      expect(
        await Promise.all([bs.collectGarbage(), bs.collectGarbage()]),
      ).toEqual([
        { removedBlocks: 1, freedBytes: 6 },
        { removedBlocks: 0, freedBytes: 0 },
      ]);
    });

    it('runs after a sweep that failed', async () => {
      await orphan();
      const broken = join(testDir, 'blobs', 'broken.json');
      await mkdir(join(testDir, 'blobs'), { recursive: true });
      await writeFile(broken, '{');
      await expect(bs.collectGarbage()).rejects.toThrow('Cannot read');

      await rm(broken);

      expect(await bs.collectGarbage()).toEqual({
        removedBlocks: 1,
        freedBytes: 6,
      });
    });
  });

  describe('an id that is no plain id', () => {
    const root = './test-bs-fs-ids';
    const store = join(root, 'store');
    // Where the paths of the block and the blob "../../etc/passwd" lead:
    // next to the store
    const outsideBlock = join(root, 'etc', 'passwd');
    const outsideBlob = `${outsideBlock}.json`;
    const outsideFiles = [outsideBlock, outsideBlob];
    const mtimes = new Map<string, number>();
    let ids: BsFs;
    let inside: { blockId: string; size: number };

    beforeEach(async () => {
      await rm(root, { recursive: true, force: true });
      ids = new BsFs(store);
      inside = await ids.setBlock(Buffer.from('inside'));
      await mkdir(dirname(outsideBlock), { recursive: true });
      await writeFile(outsideBlock, 'secret');
      await writeFile(
        outsideBlob,
        JSON.stringify({
          blobId: '../../etc/passwd',
          size: 6,
          createdAt: '2026-01-01T00:00:00.000Z',
          blocks: [{ blockId: '../../etc/passwd', size: 6 }],
        }),
      );
      for (const file of outsideFiles) {
        await age(file);
        mtimes.set(file, (await stat(file)).mtimeMs);
      }
    });

    afterEach(async () => {
      await rm(root, { recursive: true, force: true });
    });

    /** Fails unless the files outside the store are there, unchanged. */
    const expectOutsideUnchanged = async () => {
      for (const file of outsideFiles) {
        expect((await stat(file)).mtimeMs, file).toBe(mtimes.get(file));
      }
    };

    const refusals: [string, (id: string) => Promise<unknown>, string][] = [
      ['getBlob', (id) => ids.getBlob(id), 'blob'],
      ['getBlobStream', (id) => ids.getBlobStream(id), 'blob'],
      ['getBlobBlocks', (id) => ids.getBlobBlocks(id), 'blob'],
      ['getBlobProperties', (id) => ids.getBlobProperties(id), 'blob'],
      ['deleteBlob', (id) => ids.deleteBlob(id), 'blob'],
      ['generateSignedUrl', (id) => ids.generateSignedUrl(id, 60), 'blob'],
      ['setBlobBlocks', (id) => ids.setBlobBlocks(id, [inside]), 'blob'],
      ['getBlock', (id) => ids.getBlock(id), 'block'],
      [
        'setBlobBlocks',
        (id) =>
          ids.setBlobBlocks(ContentId.of(Buffer.from('secret')), [
            { blockId: id, size: 6 },
          ]),
        'block',
      ],
    ];

    const answers: [string, (id: string) => Promise<boolean>][] = [
      ['blobExists', (id) => ids.blobExists(id)],
      ['blockExists', (id) => ids.blockExists(id)],
    ];

    for (const id of ['../../etc/passwd', '..\\..\\x', 'a.b', '']) {
      for (const [method, call, kind] of refusals) {
        it(`${method} refuses ${JSON.stringify(id)} as a ${kind} id`, async () => {
          await expect(call(id)).rejects.toThrow(
            `Invalid ${kind} id: ${JSON.stringify(id)}`,
          );
          await expectOutsideUnchanged();
        });
      }

      for (const [method, call] of answers) {
        it(`${method} answers false for ${JSON.stringify(id)}`, async () => {
          expect(await call(id)).toBe(false);
          await expectOutsideUnchanged();
        });
      }
    }

    it('getBlob refuses a blob file that lists such an id', async () => {
      const listing = join(store, 'blobs', 'li', 'st', 'in', 'gs', 'listings');
      await mkdir(dirname(listing), { recursive: true });
      await writeFile(
        `${listing}.json`,
        JSON.stringify({
          blobId: 'listings',
          size: 6,
          createdAt: '2026-01-01T00:00:00.000Z',
          blocks: [{ blockId: '../../etc/passwd', size: 6 }],
        }),
      );

      await expect(ids.getBlob('listings')).rejects.toThrow(
        'Invalid block id: "../../etc/passwd"',
      );
      await expectOutsideUnchanged();
    });

    it('accepts an id of any length made of letters, digits, - and _', async () => {
      await expect(ids.getBlob('nonexistent_1234-x')).rejects.toThrow(
        'Blob not found: nonexistent_1234-x',
      );
      expect(await ids.blockExists(inside.blockId)).toBe(true);
    });
  });

  describe('concurrent operations', () => {
    it('should handle concurrent writes of same content', async () => {
      const content = 'Concurrent test';

      const results = await Promise.all([
        bs.setBlob(content),
        bs.setBlob(content),
        bs.setBlob(content),
      ]);

      // All should have same blobId
      expect(results[0]!.blobId).toBe(results[1]!.blobId);
      expect(results[1]!.blobId).toBe(results[2]!.blobId);

      // Only one blob should exist
      expect(await bs.size()).toBe(1);
    });

    it('should handle concurrent writes of different content', async () => {
      const results = await Promise.all([
        bs.setBlob('Content A'),
        bs.setBlob('Content B'),
        bs.setBlob('Content C'),
      ]);

      // All should have different blobIds
      const ids = results.map((r) => r.blobId);
      const uniqueIds = new Set(ids);
      expect(uniqueIds.size).toBe(3);

      expect(await bs.size()).toBe(3);
    });
  });

  describe('edge cases', () => {
    it('should handle special characters in content', async () => {
      const content = '!@#$%^&*(){}[]|\\:";\'<>?,./~`\n\r\t';
      const props = await bs.setBlob(content);

      const result = await bs.getBlob(props.blobId);
      expect(result.content.toString('utf8')).toBe(content);
    });

    it('should handle Unicode content', async () => {
      const content = '你好世界 🌍 مرحبا العالم';
      const props = await bs.setBlob(content);

      const result = await bs.getBlob(props.blobId);
      expect(result.content.toString('utf8')).toBe(content);
    });

    it('should handle very short blobIds correctly', async () => {
      // This tests if the directory structure creation handles edge cases
      const content = '';
      const props = await bs.setBlob(content);

      // Should still be able to retrieve
      const result = await bs.getBlob(props.blobId);
      expect(result.content.toString('utf8')).toBe(content);
    });
  });
});
