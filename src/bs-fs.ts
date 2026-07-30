// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hshBuffer } from '@rljson/hash';

import { createReadStream } from 'node:fs';
import {
  access,
  mkdir,
  open,
  readdir,
  readFile,
  rename,
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

  /** Example instance for test purposes. */
  static get example(): BsFs {
    return new BsFs('./.bs-fs-example');
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
   * Example: abc123def456 -\> blobs/ab/c1/23/de/abc123def456.txt
   * @param blobId - Hex blob id
   */
  private getBlobPath(blobId: string): {
    filePath: string;
    metaPath: string;
    dir: string;
  } {
    const subDirs: string[] = [];

    // Create subdirectories from every two letters
    for (let i = 0; i + 2 <= Math.min(blobId.length, 8); i += 2) {
      subDirs.push(blobId.substring(i, i + 2));
    }

    const dir = join(this.baseDir, ...subDirs);
    const filePath = join(dir, `${blobId}.txt`);
    const metaPath = join(dir, `${blobId}.meta.json`);

    return { filePath, metaPath, dir };
  }

  /**
   * Ensure directory exists
   * @param dir - Absolute or relative directory path
   */
  private async ensureDir(dir: string): Promise<void> {
    await mkdir(dir, { recursive: true });
  }

  /**
   * Write `data` to `path` atomically: stage in a sibling temp file, then rename
   * over the target. A crash mid-write can never leave a half-written (and thus
   * hash-mismatched) blob or a truncated metadata file. `.tmp` files are ignored
   * by {@link findAllBlobs} (it only reads `*.meta.json`).
   * @param path - Final destination path.
   * @param data - Bytes/string to write.
   */
  private async atomicWrite(
    path: string,
    data: Buffer | string,
  ): Promise<void> {
    const tmp = `${path}.${Date.now().toString(36)}-${Math.floor(
      Math.random() * 1e9,
    ).toString(36)}.tmp`;
    await writeFile(tmp, data);
    try {
      await rename(tmp, path);
    } catch (err) {
      /* v8 ignore start -- @preserve: content-addressed race/crash recovery. A
         concurrent writer landing identical bytes first makes our rename fail
         (EPERM on Windows); if the target now exists the write already
         succeeded. Not deterministically forceable cross-platform. */
      await rm(tmp, { force: true }).catch(() => {});
      try {
        await access(path);
      } catch {
        throw err;
      }
      /* v8 ignore stop */
    }
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

    // Store new blob atomically (temp + rename) so a crash never leaves a
    // partial blob or metadata file.
    await this.ensureDir(dir);
    await this.atomicWrite(filePath, buffer);

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

    await this.atomicWrite(metaPath, JSON.stringify(metadata, null, 2));

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

    // Range request: read ONLY the requested window off disk instead of
    // loading the whole (possibly multi-GB) blob into RAM just to slice it.
    // Keeps the exclusive-end `[start, end)` semantics of `subarray(start, end)`.
    let content: Buffer;
    if (options?.range) {
      const { start, end } = options.range;
      const fh = await open(filePath, 'r');
      try {
        const size = (await fh.stat()).size;
        const to = Math.min(end ?? size, size);
        const len = Math.max(0, to - start);
        const buf = Buffer.allocUnsafe(len);
        if (len > 0) await fh.read(buf, 0, len, start);
        content = buf;
      } finally {
        await fh.close();
      }
    } else {
      content = await readFile(filePath);
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
