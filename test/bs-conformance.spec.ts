// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

// ⚠️ DO NOT MODIFY THIS FILE DIRECTLY ⚠️
// 
// This file is a copy of @rljson/bs/test/bs-conformance.spec.ts.
//
// To make changes, please execute the following steps:
//   1. Clone <https://github.com/rljson/bs>
//   2. Make changes to the original file in the test folder
//   3. Submit a pull request
//   4. Publish a the new changes to npm


import {
  afterAll,
  afterEach,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
} from 'vitest';

import { BLOCK_MAX_BYTES, Bs, BsTestSetup, ContentId } from '@rljson/bs';

import { testSetup } from './bs-conformance.setup.ts';
import { expectGolden, ExpectGoldenOptions } from './setup/goldens.ts';

const ego: ExpectGoldenOptions = {
  npmUpdateGoldensEnabled: false,
};

/**
 * Conformance tests for Bs implementations.
 * Any implementation of the Bs interface should pass these tests.
 *
 * @param externalTestSetup - Optional test setup for external implementations
 */
export const runBsConformanceTests = (
  externalTestSetup?: () => BsTestSetup,
) => {
  return describe('Bs Conformance', () => {
    let bs: Bs;
    let setup: BsTestSetup;

    beforeAll(async () => {
      setup = externalTestSetup ? externalTestSetup() : testSetup();
      await setup.beforeAll();
    });

    beforeEach(async () => {
      await setup.beforeEach();
      bs = setup.bs;
    });

    afterEach(async () => {
      await setup.afterEach();
    });

    afterAll(async () => {
      await setup.afterAll();
    });

    describe('setBlob', () => {
      it('should store a blob from Buffer and return properties', async () => {
        const content = Buffer.from('Hello, World!');
        const result = await bs.setBlob(content);

        expect(result.blobId).toBeDefined();
        expect(result.blobId).toHaveLength(22);
        expect(result.size).toBe(13);
        expect(result.createdAt).toBeInstanceOf(Date);

        delete (result as any).createdAt;
        await expectGolden('bs-conformance/from-buffer.json', ego).toBe(
          result as any,
        );
      });

      it('should store a blob from string', async () => {
        const content = 'Test string content';
        const result = await bs.setBlob(content);

        expect(result.blobId).toBeDefined();
        expect(result.size).toBe(19);

        delete (result as any).createdAt;
        await expectGolden('bs-conformance/from-string.json', ego).toBe(
          result as any,
        );
      });

      it('should store a blob from ReadableStream', async () => {
        const content = 'Stream content';
        const stream = new ReadableStream({
          start(controller) {
            controller.enqueue(new TextEncoder().encode(content));
            controller.close();
          },
        });

        const result = await bs.setBlob(stream);

        expect(result.blobId).toBeDefined();
        expect(result.size).toBe(14);

        delete (result as any).createdAt;
        await expectGolden('bs-conformance/from-stream.json', ego).toBe(
          result as any,
        );
      });

      it('should deduplicate identical content', async () => {
        const content = Buffer.from('Duplicate content');

        const result1 = await bs.setBlob(content);
        const result2 = await bs.setBlob(content);

        expect(result1.blobId).toBe(result2.blobId);
        expect(result1.createdAt).toEqual(result2.createdAt);

        delete (result1 as any).createdAt;
        await expectGolden('bs-conformance/deduplication.json', ego).toBe(
          result1 as any,
        );
      });

      it('should generate different blob IDs for different content', async () => {
        const content1 = Buffer.from('Content 1');
        const content2 = Buffer.from('Content 2');

        const result1 = await bs.setBlob(content1);
        const result2 = await bs.setBlob(content2);

        expect(result1.blobId).not.toBe(result2.blobId);

        delete (result1 as any).createdAt;
        delete (result2 as any).createdAt;
        await expectGolden('bs-conformance/different-content.json', ego).toBe({
          blob1: result1 as any,
          blob2: result2 as any,
        });
      });

      it('should handle empty content', async () => {
        const empty = Buffer.from('');
        const result = await bs.setBlob(empty);

        expect(result.blobId).toBeDefined();
        expect(result.size).toBe(0);

        delete (result as any).createdAt;
        await expectGolden('bs-conformance/empty-content.json', ego).toBe(
          result as any,
        );
      });

      it('should handle binary content', async () => {
        const binaryData = Buffer.from([0, 1, 2, 3, 255, 254, 253]);
        const result = await bs.setBlob(binaryData);

        expect(result.blobId).toBeDefined();
        expect(result.size).toBe(7);

        delete (result as any).createdAt;
        await expectGolden('bs-conformance/binary-content.json', ego).toBe(
          result as any,
        );
      });
    });

    describe('getBlob', () => {
      it('should retrieve stored blob', async () => {
        const content = Buffer.from('Retrieve me!');
        const { blobId } = await bs.setBlob(content);

        const result = await bs.getBlob(blobId);

        expect(result.content.toString()).toBe('Retrieve me!');
        expect(result.properties.blobId).toBe(blobId);
        expect(result.properties.size).toBe(12);

        expect(result.properties.createdAt).toBeInstanceOf(Date);

        delete (result.properties as any).createdAt;
        await expectGolden('bs-conformance/retrieve-blob.json', ego).toBe({
          properties: result.properties as any,
        });
      });

      it('should throw error for non-existent blob', async () => {
        await expect(bs.getBlob('nonexistent1234567890')).rejects.toThrow(
          'Blob not found',
        );
      });

      it('should support range requests', async () => {
        const content = Buffer.from('0123456789');
        const { blobId } = await bs.setBlob(content);

        const result = await bs.getBlob(blobId, {
          range: { start: 2, end: 5 },
        });

        expect(result.content.toString()).toBe('234');

        expect(result.properties.blobId).toBe(blobId);
        expect(result.properties.size).toBe(10); // size is total size

        expect(result.properties.createdAt).toBeInstanceOf(Date);

        delete (result.properties as any).createdAt;
        await expectGolden('bs-conformance/range-request.json', ego).toBe({
          properties: result.properties as any,
        });
      });

      it('should retrieve binary content correctly', async () => {
        const binaryData = Buffer.from([0, 1, 2, 3, 255, 254, 253]);
        const { blobId } = await bs.setBlob(binaryData);

        const result = await bs.getBlob(blobId);

        expect(result.content).toEqual(binaryData);

        expect(result.properties.blobId).toBe(blobId);
        expect(result.properties.size).toBe(7);

        expect(result.properties.createdAt).toBeInstanceOf(Date);

        delete (result.properties as any).createdAt;
        await expectGolden('bs-conformance/retrieve-binary.json', ego).toBe({
          properties: result.properties as any,
        });
      });

      describe('getBlobStream', () => {
        it('should return a ReadableStream for blob', async () => {
          const content = Buffer.from('Stream this content');
          const { blobId } = await bs.setBlob(content);

          const stream = await bs.getBlobStream(blobId);

          expect(stream).toBeInstanceOf(ReadableStream);

          const reader = stream.getReader();
          const chunks: Uint8Array[] = [];
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
          }

          const result = Buffer.concat(chunks).toString();
          expect(result).toBe('Stream this content');

          delete (result as any).createdAt;
          await expectGolden('bs-conformance/get-blob-stream.json', ego).toBe(
            result as any,
          );
        });

        it('should throw error for non-existent blob', async () => {
          await expect(
            bs.getBlobStream('nonexistent1234567890'),
          ).rejects.toThrow('Blob not found');
        });

        it('delivers a multi-chunk blob whole and in order', async () => {
          // Every implementation hands the stream out in pieces — a disk read
          // in 64 kB chunks, a peer in ranged pulls — and the one bug they can
          // all have is a piece lost, repeated or out of order. A 200 kB blob
          // crosses several boundaries on every backend; a nineteen-byte one
          // crosses none, which is why the test above cannot see this.
          const size = 200 * 1024;
          const content = Buffer.alloc(size);
          // A per-position pattern, so a duplicated or reordered chunk shows as
          // a mismatch rather than as more of the same byte.
          for (let i = 0; i < size; i++) content[i] = i % 251;
          const { blobId } = await bs.setBlob(content);

          const reader = (await bs.getBlobStream(blobId)).getReader();
          const chunks: Uint8Array[] = [];
          for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            chunks.push(value);
          }

          const joined = Buffer.concat(chunks);
          expect(joined).toHaveLength(size);
          expect(joined.equals(content)).toBe(true);
        });
      });

      describe('deleteBlob', () => {
        it('should delete an existing blob', async () => {
          const content = Buffer.from('Delete me');
          const { blobId } = await bs.setBlob(content);

          expect(await bs.blobExists(blobId)).toBe(true);

          await bs.deleteBlob(blobId);

          expect(await bs.blobExists(blobId)).toBe(false);
        });

        it('should throw error when deleting non-existent blob', async () => {
          await expect(bs.deleteBlob('nonexistent1234567890')).rejects.toThrow(
            'Blob not found',
          );
        });
      });

      describe('blobExists', () => {
        it('should return true for existing blob', async () => {
          const content = Buffer.from('I exist');
          const { blobId } = await bs.setBlob(content);

          expect(await bs.blobExists(blobId)).toBe(true);
        });

        it('should return false for non-existent blob', async () => {
          expect(await bs.blobExists('nonexistent1234567890')).toBe(false);
        });
      });

      describe('getBlobProperties', () => {
        it('should return properties for existing blob', async () => {
          const content = Buffer.from('Properties test');
          const { blobId, size, createdAt } = await bs.setBlob(content);

          const properties = await bs.getBlobProperties(blobId);

          expect(properties.blobId).toBe(blobId);
          expect(properties.size).toBe(size);
          expect(properties.createdAt).toEqual(createdAt);

          delete (properties as any).createdAt;
          await expectGolden(
            'bs-conformance/get-blob-properties.json',
            ego,
          ).toBe(properties as any);
        });

        it('should throw error for non-existent blob', async () => {
          await expect(
            bs.getBlobProperties('nonexistent1234567890'),
          ).rejects.toThrow('Blob not found');
        });
      });

      describe('listBlobs', () => {
        it('should return empty list when no blobs', async () => {
          const result = await bs.listBlobs();

          expect(result.blobs).toEqual([]);
          expect(result.continuationToken).toBeUndefined();
        });

        it('should list all blobs', async () => {
          await bs.setBlob('Blob 1');
          await bs.setBlob('Blob 2');
          await bs.setBlob('Blob 3');

          const result = await bs.listBlobs();

          expect(result.blobs).toHaveLength(3);
          expect(result.continuationToken).toBeUndefined();

          await expectGolden('bs-conformance/list-all-blobs.json', ego).toBe(
            result.blobs.map((b) => ({
              blobId: b.blobId,
              size: b.size,
            })) as any,
          );
        });

        it('should filter by prefix', async () => {
          const blob1 = await bs.setBlob('Blob 1');
          await bs.setBlob('Blob 2');
          await bs.setBlob('Blob 3');

          const prefix = blob1.blobId.substring(0, 2);

          const result = await bs.listBlobs({ prefix });

          expect(result.blobs.length).toBeGreaterThanOrEqual(1);
          expect(result.blobs.every((b) => b.blobId.startsWith(prefix))).toBe(
            true,
          );

          await expectGolden('bs-conformance/list-by-prefix.json', ego).toBe(
            result.blobs.map((b) => ({
              blobId: b.blobId,
              size: b.size,
            })) as any,
          );
        });

        it('should support pagination with maxResults', async () => {
          await bs.setBlob('Blob 1');
          await bs.setBlob('Blob 2');
          await bs.setBlob('Blob 3');
          await bs.setBlob('Blob 4');

          const page1 = await bs.listBlobs({ maxResults: 2 });

          expect(page1.blobs).toHaveLength(2);
          expect(page1.continuationToken).toBeDefined();

          const page2 = await bs.listBlobs({
            maxResults: 2,
            continuationToken: page1.continuationToken,
          });

          expect(page2.blobs).toHaveLength(2);
          expect(page2.continuationToken).toBeUndefined();

          const allBlobs = [...page1.blobs, ...page2.blobs];
          expect(allBlobs).toHaveLength(4);

          await expectGolden('bs-conformance/paginated-listing.json', ego).toBe(
            {
              page1: {
                blobs: page1.blobs.map((b) => ({
                  blobId: b.blobId,
                  size: b.size,
                })),
              },
              page2: {
                blobs: page2.blobs.map((b) => ({
                  blobId: b.blobId,
                  size: b.size,
                })),
              },
            } as any,
          );
        });

        it('should paginate through all results', async () => {
          await bs.setBlob('Blob 1');
          await bs.setBlob('Blob 2');
          await bs.setBlob('Blob 3');
          await bs.setBlob('Blob 4');
          await bs.setBlob('Blob 5');

          const allBlobs: string[] = [];
          let continuationToken: string | undefined;

          do {
            const page = await bs.listBlobs({
              maxResults: 2,
              continuationToken,
            });
            allBlobs.push(...page.blobs.map((b) => b.blobId));
            continuationToken = page.continuationToken;
          } while (continuationToken);

          expect(allBlobs).toHaveLength(5);
          expect(new Set(allBlobs).size).toBe(5);

          await expectGolden(
            'bs-conformance/paginate-all-results.json',
            ego,
          ).toBe(allBlobs as any);
        });

        it('should return blobs in consistent order', async () => {
          await bs.setBlob('C');
          await bs.setBlob('A');
          await bs.setBlob('B');

          const result1 = await bs.listBlobs();
          const result2 = await bs.listBlobs();

          expect(result1.blobs.map((b) => b.blobId)).toEqual(
            result2.blobs.map((b) => b.blobId),
          );

          await expectGolden(
            'bs-conformance/consistent-ordering.json',
            ego,
          ).toBe(
            result1.blobs.map((b) => ({
              blobId: b.blobId,
              size: b.size,
            })) as any,
          );
        });
      });

      describe('generateSignedUrl', () => {
        it('should generate a signed URL for existing blob', async () => {
          const content = Buffer.from('Sign me');
          const { blobId } = await bs.setBlob(content);

          const url = await bs.generateSignedUrl(blobId, 3600);

          expect(url).toContain(blobId);
          expect(url).toContain('expires=');
          expect(url).toContain('permissions=read');
        });

        it('should support different permissions', async () => {
          const content = Buffer.from('Delete permission');
          const { blobId } = await bs.setBlob(content);

          const url = await bs.generateSignedUrl(blobId, 3600, 'delete');

          expect(url).toContain('permissions=delete');
        });

        it('should throw error for non-existent blob', async () => {
          await expect(
            bs.generateSignedUrl('nonexistent1234567890', 3600),
          ).rejects.toThrow('Blob not found');
        });
      });

      describe('content-addressable behavior', () => {
        it('should generate same blob ID for same content', async () => {
          const content = 'Identical content';

          const result1 = await bs.setBlob(content);
          const result2 = await bs.setBlob(content);

          expect(result1.blobId).toBe(result2.blobId);
        });

        it('should handle large content', async () => {
          const largeContent = Buffer.alloc(1024 * 1024, 'x'); // 1MB
          const result = await bs.setBlob(largeContent);

          expect(result.size).toBe(1024 * 1024);

          const retrieved = await bs.getBlob(result.blobId);
          expect(retrieved.content.length).toBe(1024 * 1024);

          delete (result as any).createdAt;
          await expectGolden('bs-conformance/large-content.json', ego).toBe(
            result as any,
          );
        });
      });
    });

    describe('blocks', () => {
      /** Bytes that do not repeat, the same for the same seed. */
      const noise = (size: number, seed: number): Buffer => {
        const bytes = Buffer.alloc(size);
        let x = seed;
        for (let i = 0; i < size; i++) {
          x = (Math.imul(x, 1103515245) + 12345) >>> 0;
          bytes[i] = x >>> 24;
        }
        return bytes;
      };

      const readAll = async (stream: ReadableStream<Uint8Array>) => {
        const parts: Uint8Array[] = [];
        const reader = stream.getReader();
        for (;;) {
          const { done, value } = await reader.read();
          if (done) return Buffer.concat(parts);
          parts.push(value);
        }
      };

      const large = noise(6 * 1024 * 1024, 11);

      it('stores a large blob as blocks whose sizes add up to the blob', async () => {
        const { blobId, size } = await bs.setBlob(large);
        expect(size).toBe(large.length);
        const blocks = await bs.getBlobBlocks(blobId);
        expect(blocks.length).toBeGreaterThan(1);
        expect(blocks.reduce((sum, b) => sum + b.size, 0)).toBe(large.length);
        for (const block of blocks) {
          expect(block.size).toBeLessThanOrEqual(BLOCK_MAX_BYTES);
          expect(await bs.blockExists(block.blockId)).toBe(true);
          expect(ContentId.of(await bs.getBlock(block.blockId))).toBe(
            block.blockId,
          );
        }
      });

      it('reads a large blob back whole, in ranges and as a stream', async () => {
        const { blobId } = await bs.setBlob(large);
        expect((await bs.getBlob(blobId)).content.equals(large)).toBe(true);
        const start = 1024 * 1024 - 10;
        const end = 3 * 1024 * 1024 + 10;
        expect(
          (await bs.getBlob(blobId, { range: { start, end } })).content.equals(
            large.subarray(start, end),
          ),
        ).toBe(true);
        expect(
          (await readAll(await bs.getBlobStream(blobId))).equals(large),
        ).toBe(true);
      });

      it('gives a stream the blob id of the same bytes in a buffer', async () => {
        const stream = new ReadableStream<Uint8Array>({
          start(controller) {
            for (let i = 0; i < large.length; i += 100_000) {
              controller.enqueue(
                new Uint8Array(large.subarray(i, i + 100_000)),
              );
            }
            controller.close();
          },
        });
        const fromStream = await bs.setBlob(stream);
        expect(fromStream.blobId).toBe(ContentId.of(large));
        expect(await bs.getBlobBlocks(fromStream.blobId)).toEqual(
          await bs.getBlobBlocks((await bs.setBlob(large)).blobId),
        );
      });

      it('stores the blocks two versions share once', async () => {
        const v1 = await bs.setBlob(large);
        const changed = Buffer.from(large);
        changed[large.length / 2] ^= 0xff;
        const v2 = await bs.setBlob(changed);
        const blocks1 = await bs.getBlobBlocks(v1.blobId);
        const blocks2 = await bs.getBlobBlocks(v2.blobId);
        const ids1 = new Set(blocks1.map((b) => b.blockId));
        const fresh = blocks2.filter((b) => !ids1.has(b.blockId));
        expect(fresh.length).toBeGreaterThan(0);
        expect(fresh.length).toBeLessThanOrEqual(2);
      });

      it('stores an empty blob without blocks', async () => {
        const { blobId, size } = await bs.setBlob(Buffer.alloc(0));
        expect(size).toBe(0);
        expect(await bs.getBlobBlocks(blobId)).toEqual([]);
        expect((await bs.getBlob(blobId)).content.length).toBe(0);
        expect((await readAll(await bs.getBlobStream(blobId))).length).toBe(0);
      });

      it('stores a blob from blocks it holds', async () => {
        const a = await bs.setBlock(Buffer.from('first half, '));
        const b = await bs.setBlock(Buffer.from('second half'));
        expect(a).toEqual({
          blockId: ContentId.of(Buffer.from('first half, ')),
          size: 12,
        });
        const blobId = ContentId.of(Buffer.from('first half, second half'));

        const properties = await bs.setBlobBlocks(blobId, [a, b]);
        expect(properties.blobId).toBe(blobId);
        expect(properties.size).toBe(23);
        expect((await bs.getBlob(blobId)).content.toString()).toBe(
          'first half, second half',
        );
        expect(await bs.getBlobBlocks(blobId)).toEqual([a, b]);

        // Stored already: the stored blob is answered.
        expect((await bs.setBlobBlocks(blobId, [a, b])).blobId).toBe(blobId);
      });

      it('refuses blocks that do not make the blob', async () => {
        const a = await bs.setBlock(Buffer.from('some bytes'));
        await expect(bs.setBlobBlocks('not-their-hash', [a])).rejects.toThrow(
          "Blocks of blob 'not-their-hash' make",
        );
        expect(await bs.blobExists('not-their-hash')).toBe(false);
      });

      it('refuses a blob whose blocks it lacks', async () => {
        await expect(
          bs.setBlobBlocks('some-blob', [
            { blockId: 'missing-block', size: 3 },
          ]),
        ).rejects.toThrow('Block not found: missing-block');
      });

      it('reports a missing block', async () => {
        expect(await bs.blockExists('missing-block')).toBe(false);
        await expect(bs.getBlock('missing-block')).rejects.toThrow(
          'Block not found: missing-block',
        );
        await expect(bs.getBlobBlocks('missing-blob')).rejects.toThrow(
          'Blob not found: missing-blob',
        );
      });

      it('keeps the blocks of a deleted blob', async () => {
        const { blobId } = await bs.setBlob('deleted, blocks kept');
        const [{ blockId }] = await bs.getBlobBlocks(blobId);
        await bs.deleteBlob(blobId);
        expect(await bs.blobExists(blobId)).toBe(false);
        expect(await bs.blockExists(blockId)).toBe(true);
      });
    });
  });
};
// Run conformance tests for BsMem
runBsConformanceTests();
