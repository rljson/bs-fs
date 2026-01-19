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

import { Bs, BsTestSetup } from '@rljson/bs';

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
  });
};
// Run conformance tests for BsMem
runBsConformanceTests();
