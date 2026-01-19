// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hshBuffer } from '@rljson/hash';

import { Readable } from 'node:stream';

import type {
  BlobProperties,
  Bs,
  DownloadBlobOptions,
  ListBlobsOptions,
  ListBlobsResult,
} from './bs.js';
interface StoredBlob {
  content: Buffer;
  properties: BlobProperties;
}

/**
 * In-memory implementation of content-addressable blob storage.
 * All blobs are stored in memory using a Map.
 * Useful for testing and development.
 */
export class BsMem implements Bs {
  private readonly blobs = new Map<string, StoredBlob>();

  /**
   * Convert content to Buffer
   * @param content - Content to convert (Buffer, string, or ReadableStream)
   */
  private async toBuffer(
    content: Buffer | string | ReadableStream,
  ): Promise<Buffer> {
    if (Buffer.isBuffer(content)) {
      return content;
    }

    if (typeof content === 'string') {
      return Buffer.from(content, 'utf8');
    }

    // Handle ReadableStream
    const reader = content.getReader();
    const chunks: Uint8Array[] = [];

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }

    return Buffer.concat(chunks);
  }

  async setBlob(
    content: Buffer | string | ReadableStream,
  ): Promise<BlobProperties> {
    const buffer = await this.toBuffer(content);
    const blobId = hshBuffer(buffer);

    // Check if blob already exists (deduplication)
    const existing = this.blobs.get(blobId);
    if (existing) {
      return existing.properties;
    }

    // Store new blob
    const properties: BlobProperties = {
      blobId,
      size: buffer.length,
      createdAt: new Date(),
    };

    this.blobs.set(blobId, {
      content: buffer,
      properties,
    });

    return properties;
  }

  async getBlob(
    blobId: string,
    options?: DownloadBlobOptions,
  ): Promise<{ content: Buffer; properties: BlobProperties }> {
    const stored = this.blobs.get(blobId);
    if (!stored) {
      throw new Error(`Blob not found: ${blobId}`);
    }

    let content = stored.content;

    // Handle range request
    if (options?.range) {
      const { start, end } = options.range;
      content = stored.content.subarray(start, end);
    }

    return {
      content,
      properties: stored.properties,
    };
  }

  async getBlobStream(blobId: string): Promise<ReadableStream> {
    const stored = this.blobs.get(blobId);
    if (!stored) {
      throw new Error(`Blob not found: ${blobId}`);
    }

    // Convert Buffer to ReadableStream
    const nodeStream = Readable.from(stored.content);
    return Readable.toWeb(nodeStream) as ReadableStream;
  }

  async deleteBlob(blobId: string): Promise<void> {
    const deleted = this.blobs.delete(blobId);
    if (!deleted) {
      throw new Error(`Blob not found: ${blobId}`);
    }
  }

  async blobExists(blobId: string): Promise<boolean> {
    return this.blobs.has(blobId);
  }

  async getBlobProperties(blobId: string): Promise<BlobProperties> {
    const stored = this.blobs.get(blobId);
    if (!stored) {
      throw new Error(`Blob not found: ${blobId}`);
    }
    return stored.properties;
  }

  async listBlobs(options?: ListBlobsOptions): Promise<ListBlobsResult> {
    let blobs = Array.from(this.blobs.values()).map(
      (stored) => stored.properties,
    );

    // Filter by prefix if provided
    if (options?.prefix) {
      blobs = blobs.filter((blob) => blob.blobId.startsWith(options.prefix!));
    }

    // Sort by blobId for consistent ordering
    blobs.sort((a, b) => a.blobId.localeCompare(b.blobId));

    // Handle pagination
    const maxResults = options?.maxResults ?? blobs.length;
    let startIndex = 0;

    if (options?.continuationToken) {
      // Continuation token is the last blobId from previous page
      // Find the next item after the token
      const tokenIndex = blobs.findIndex(
        (blob) => blob.blobId === options.continuationToken,
      );
      /* v8 ignore next -- @preserve */
      startIndex = tokenIndex === -1 ? 0 : tokenIndex + 1;
    }

    const endIndex = Math.min(startIndex + maxResults, blobs.length);
    const pageBlobs = blobs.slice(startIndex, endIndex);

    // Set continuation token if there are more results
    const continuationToken =
      endIndex < blobs.length
        ? pageBlobs[pageBlobs.length - 1]?.blobId
        : undefined;

    return {
      blobs: pageBlobs,
      continuationToken,
    };
  }

  async generateSignedUrl(
    blobId: string,
    expiresIn: number,
    permissions?: 'read' | 'delete',
  ): Promise<string> {
    // Check if blob exists
    if (!this.blobs.has(blobId)) {
      throw new Error(`Blob not found: ${blobId}`);
    }

    // For in-memory implementation, return a mock URL
    // In a real implementation, this would generate a proper signed URL
    const expires = Date.now() + expiresIn * 1000;
    const perm = permissions ?? 'read';
    return `mem://${blobId}?expires=${expires}&permissions=${perm}`;
  }

  /**
   * Clear all blobs from storage (useful for testing)
   */
  clear(): void {
    this.blobs.clear();
  }

  /**
   * Get the number of blobs in storage
   */
  get size(): number {
    return this.blobs.size;
  }
}
