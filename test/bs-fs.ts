// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hshBuffer } from '@rljson/hash';

import { createReadStream } from 'node:fs';
import {
  access,
  mkdir,
  readdir,
  readFile,
  rm,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { join } from 'node:path';
import { Readable } from 'node:stream';

import type {
  BlobProperties,
  Bs,
  DownloadBlobOptions,
  ListBlobsOptions,
  ListBlobsResult,
} from '@rljson/bs';

interface StoredMetadata {
  blobId: string;
  size: number;
  createdAt: string;
}

/**
 * Filesystem-based implementation of content-addressable blob storage.
 * All blobs are stored on the filesystem in a hierarchical directory structure.
 * Useful for persistent storage and production use.
 */
export class BsFs implements Bs {
  private readonly baseDir: string;

  /**
   * Create a new BsFs instance
   * @param baseDir - Base directory for blob storage (defaults to './blobs')
   */
  constructor(baseDir: string = './blobs') {
    this.baseDir = baseDir;
  }

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

  /**
   * Generate file path and directory structure for a blobId
   * Creates subdirectories using every two letters from blobId
   * Example: abc123def456 -> blobs/ab/c1/23/de/abc123def456.txt
   */
  private getBlobPath(blobId: string): {
    filePath: string;
    metaPath: string;
    dir: string;
  } {
    const subDirs: string[] = [];

    // Create subdirectories from every two letters
    for (let i = 0; i < Math.min(blobId.length, 8); i += 2) {
      if (i + 2 <= blobId.length) {
        subDirs.push(blobId.substring(i, i + 2));
      }
    }

    const dir = join(this.baseDir, ...subDirs);
    const filePath = join(dir, `${blobId}.txt`);
    const metaPath = join(dir, `${blobId}.meta.json`);

    return { filePath, metaPath, dir };
  }

  /**
   * Ensure directory exists
   */
  private async ensureDir(dir: string): Promise<void> {
    await mkdir(dir, { recursive: true });
  }

  async setBlob(
    content: Buffer | string | ReadableStream,
  ): Promise<BlobProperties> {
    const buffer = await this.toBuffer(content);
    const blobId = hshBuffer(buffer);
    const { filePath, metaPath, dir } = this.getBlobPath(blobId);

    // Check if blob already exists (deduplication)
    try {
      await access(filePath);
      // Blob exists, read and return existing properties
      const metaContent = await readFile(metaPath, 'utf8');
      const metadata: StoredMetadata = JSON.parse(metaContent);
      return {
        blobId: metadata.blobId,
        size: metadata.size,
        createdAt: new Date(metadata.createdAt),
      };
    } catch {
      // Blob doesn't exist, create it
    }

    // Store new blob
    await this.ensureDir(dir);
    await writeFile(filePath, buffer);

    const properties: BlobProperties = {
      blobId,
      size: buffer.length,
      createdAt: new Date(),
    };

    const metadata: StoredMetadata = {
      blobId: properties.blobId,
      size: properties.size,
      createdAt: properties.createdAt.toISOString(),
    };

    await writeFile(metaPath, JSON.stringify(metadata, null, 2), 'utf8');

    return properties;
  }

  async getBlob(
    blobId: string,
    options?: DownloadBlobOptions,
  ): Promise<{ content: Buffer; properties: BlobProperties }> {
    const { filePath, metaPath } = this.getBlobPath(blobId);

    try {
      await access(filePath);
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }

    let content = await readFile(filePath);

    // Handle range request
    if (options?.range) {
      const { start, end } = options.range;
      content = content.subarray(start, end);
    }

    // Read metadata
    const metaContent = await readFile(metaPath, 'utf8');
    const metadata: StoredMetadata = JSON.parse(metaContent);

    const properties: BlobProperties = {
      blobId: metadata.blobId,
      size: metadata.size,
      createdAt: new Date(metadata.createdAt),
    };

    return {
      content,
      properties,
    };
  }

  async getBlobStream(blobId: string): Promise<ReadableStream> {
    const { filePath } = this.getBlobPath(blobId);

    try {
      await access(filePath);
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }

    // Create read stream from file
    const nodeStream = createReadStream(filePath);
    return Readable.toWeb(nodeStream) as ReadableStream;
  }

  async deleteBlob(blobId: string): Promise<void> {
    const { filePath, metaPath } = this.getBlobPath(blobId);

    try {
      await access(filePath);
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }

    await unlink(filePath);
    await unlink(metaPath);
  }

  async blobExists(blobId: string): Promise<boolean> {
    const { filePath } = this.getBlobPath(blobId);
    try {
      await access(filePath);
      return true;
    } catch {
      return false;
    }
  }

  async getBlobProperties(blobId: string): Promise<BlobProperties> {
    const { filePath, metaPath } = this.getBlobPath(blobId);

    try {
      await access(filePath);
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }

    const metaContent = await readFile(metaPath, 'utf8');
    const metadata: StoredMetadata = JSON.parse(metaContent);

    return {
      blobId: metadata.blobId,
      size: metadata.size,
      createdAt: new Date(metadata.createdAt),
    };
  }

  /**
   * Recursively find all blob metadata files in the storage directory
   */
  private async findAllBlobs(): Promise<BlobProperties[]> {
    const blobs: BlobProperties[] = [];

    const scanDir = async (dir: string): Promise<void> => {
      try {
        const entries = await readdir(dir, { withFileTypes: true });

        for (const entry of entries) {
          const fullPath = join(dir, entry.name);

          if (entry.isDirectory()) {
            await scanDir(fullPath);
          } else if (entry.isFile() && entry.name.endsWith('.meta.json')) {
            try {
              const metaContent = await readFile(fullPath, 'utf8');
              const metadata: StoredMetadata = JSON.parse(metaContent);
              blobs.push({
                blobId: metadata.blobId,
                size: metadata.size,
                createdAt: new Date(metadata.createdAt),
              });
            } catch {
              // Skip invalid metadata files
            }
          }
        }
      } catch {
        // Directory doesn't exist or can't be read
      }
    };

    await scanDir(this.baseDir);
    return blobs;
  }

  async listBlobs(options?: ListBlobsOptions): Promise<ListBlobsResult> {
    let blobs = await this.findAllBlobs();

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
    const { filePath } = this.getBlobPath(blobId);

    // Check if blob exists
    try {
      await access(filePath);
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }

    // For filesystem implementation, return a mock URL
    // In a real implementation, this would generate a proper signed URL
    const expires = Date.now() + expiresIn * 1000;
    const perm = permissions ?? 'read';
    return `fs://${blobId}?expires=${expires}&permissions=${perm}`;
  }

  /**
   * Clear all blobs from storage (useful for testing)
   */
  async clear(): Promise<void> {
    try {
      await rm(this.baseDir, { recursive: true, force: true });
    } catch {
      // Directory doesn't exist or can't be removed
    }
  }

  /**
   * Get the number of blobs in storage
   */
  async size(): Promise<number> {
    const blobs = await this.findAllBlobs();
    return blobs.length;
  }
}
