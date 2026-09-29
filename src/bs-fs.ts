// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hshBuffer } from '@rljson/hash';

import { createHash } from 'node:crypto';
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

/**
 * Characters of the base64url digest that make a blobId.
 *
 * Fixed by `@rljson/hash`'s default `hashLength`, and repeated here because the
 * streaming write computes its digest itself. A mismatch would store a blob
 * under an id nothing else in the system would look for.
 */
const _HASH_LENGTH = 22;

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
   * Convert content to Buffer.
   *
   * No stream case: a stream never reaches here, because collecting one into a
   * Buffer is the cost {@link BsFs._setBlobFromStream} exists to avoid. The
   * narrowed parameter type is the guard — a caller that adds a third shape has
   * to decide which path it belongs on rather than getting the buffering one by
   * default.
   * @param content - Content to convert.
   * @returns The bytes.
   */
  private toBuffer(content: Buffer | string): Buffer {
    return Buffer.isBuffer(content) ? content : Buffer.from(content, 'utf8');
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
    // A stream is written through to disk rather than collected first. Reading
    // it into a Buffer to hash it costs the whole file in RAM — twice, in fact,
    // because `hshBuffer` copies its input into a padded working buffer — and
    // that is the cost this class exists to avoid for large files.
    if (content instanceof ReadableStream) {
      return this._setBlobFromStream(content);
    }
    const buffer = this.toBuffer(content);
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

  /**
   * Stores a blob from a stream, holding one chunk at a time.
   *
   * ## Why this cannot simply hash and then write
   *
   * The store is content-addressed: the path a blob belongs at is derived from
   * its own hash, so the destination is unknown until the last byte has been
   * read. The bytes therefore land in a temp file while a running SHA-256
   * consumes them, and the finished digest decides where that file is moved.
   *
   * The digest is Node's incremental one rather than {@link hshBuffer}, which
   * takes a whole Buffer. They agree exactly — SHA-256 in base64url, truncated
   * to {@link _HASH_LENGTH} — verified across the block boundaries where a
   * padding mistake would show (55, 56, 63, 64, 65 bytes) and across split
   * feeds. They have to agree: a blob stored under a different id than the
   * buffer path would give it is a blob that deduplication can never find and
   * that every existing reference misses.
   * @param content - The stream to store.
   * @returns Properties of the stored blob.
   */
  private async _setBlobFromStream(
    content: ReadableStream,
  ): Promise<BlobProperties> {
    await this.ensureDir(this.baseDir);
    const tmp = join(
      this.baseDir,
      `.incoming-${Date.now().toString(36)}-${Math.floor(
        Math.random() * 1e9,
      ).toString(36)}.tmp`,
    );

    const digest = createHash('sha256');
    let size = 0;
    const handle = await open(tmp, 'w');
    try {
      const reader = content.getReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        digest.update(value);
        size += value.length;
        await handle.write(value);
      }
    } finally {
      await handle.close();
    }

    const blobId = digest.digest('base64url').slice(0, _HASH_LENGTH);
    const { filePath, metaPath, dir } = this.getBlobPath(blobId);

    // Deduplication, same rule as the buffer path — but the staged file has to
    // be cleaned up either way, or every re-send of an existing blob leaves a
    // copy of it behind.
    try {
      await access(filePath);
      await rm(tmp, { force: true });
      const metaContent = await readFile(metaPath, 'utf8');
      const metadata: StoredMetadata = JSON.parse(metaContent);
      return {
        blobId: metadata.blobId,
        size: metadata.size,
        createdAt: new Date(metadata.createdAt),
      };
    } catch {
      // Not stored yet — fall through and move the staged file into place.
    }

    await this.ensureDir(dir);
    try {
      await rename(tmp, filePath);
    } catch (err) {
      /* v8 ignore start -- @preserve: same content-addressed race as
         `atomicWrite`. A concurrent writer landing identical bytes first makes
         this rename fail (EPERM on Windows); if the target exists now, the
         write already succeeded. Not deterministically forceable. */
      await rm(tmp, { force: true }).catch(() => {});
      try {
        await access(filePath);
      } catch {
        throw err;
      }
      /* v8 ignore stop */
    }

    const properties: BlobProperties = {
      blobId,
      size,
      createdAt: new Date(),
    };
    await this.atomicWrite(
      metaPath,
      JSON.stringify(
        {
          blobId: properties.blobId,
          size: properties.size,
          createdAt: properties.createdAt.toISOString(),
        } satisfies StoredMetadata,
        null,
        2,
      ),
    );

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
