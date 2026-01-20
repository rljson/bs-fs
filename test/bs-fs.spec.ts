// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { access, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';

import { BsFs } from './bs-fs.js';

describe('BsFs', () => {
  let bs: BsFs;
  const testDir = './test-bs-fs';

  beforeEach(async () => {
    bs = new BsFs(testDir);
    await bs.clear();
  });

  afterEach(async () => {
    await bs.clear();
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

      // Verify that subdirectories are created (first 8 chars, 2 at a time)
      const subDirs = [];
      for (let i = 0; i < Math.min(props.blobId.length, 8); i += 2) {
        if (i + 2 <= props.blobId.length) {
          subDirs.push(props.blobId.substring(i, i + 2));
        }
      }

      const expectedDir = join(testDir, ...subDirs);
      const expectedFile = join(expectedDir, `${props.blobId}.txt`);
      const expectedMeta = join(expectedDir, `${props.blobId}.meta.json`);

      // Check files exist
      await expect(access(expectedFile)).resolves.toBeUndefined();
      await expect(access(expectedMeta)).resolves.toBeUndefined();
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

    it('should remove both content and metadata files', async () => {
      const content = 'Delete test';
      const props = await bs.setBlob(content);

      // Verify files exist before deletion
      const subDirs = [];
      for (let i = 0; i < Math.min(props.blobId.length, 8); i += 2) {
        if (i + 2 <= props.blobId.length) {
          subDirs.push(props.blobId.substring(i, i + 2));
        }
      }
      const dir = join(testDir, ...subDirs);
      const filePath = join(dir, `${props.blobId}.txt`);
      const metaPath = join(dir, `${props.blobId}.meta.json`);

      await expect(access(filePath)).resolves.toBeUndefined();
      await expect(access(metaPath)).resolves.toBeUndefined();

      await bs.deleteBlob(props.blobId);

      // Verify files are deleted
      await expect(access(filePath)).rejects.toThrow();
      await expect(access(metaPath)).rejects.toThrow();
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

      // Read metadata file directly
      const subDirs = [];
      for (let i = 0; i < Math.min(props.blobId.length, 8); i += 2) {
        if (i + 2 <= props.blobId.length) {
          subDirs.push(props.blobId.substring(i, i + 2));
        }
      }
      const metaPath = join(testDir, ...subDirs, `${props.blobId}.meta.json`);

      const metaContent = await readFile(metaPath, 'utf8');
      const metadata = JSON.parse(metaContent);

      expect(metadata.blobId).toBe(props.blobId);
      expect(metadata.size).toBe(props.size);
      expect(metadata.createdAt).toBeDefined();
      expect(new Date(metadata.createdAt)).toBeInstanceOf(Date);
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
