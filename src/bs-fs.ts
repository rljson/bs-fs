// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import {
  type BlobBlock,
  type BlobProperties,
  type Bs,
  ContentId,
  type DownloadBlobOptions,
  type ListBlobsOptions,
  type ListBlobsResult,
  readBlocks,
  streamBlocks,
  verifyBlob,
  writeBlocks,
} from '@rljson/bs';

import type { Dirent } from 'node:fs';
import {
  access,
  mkdir,
  readdir,
  readFile,
  rename,
  rm,
  unlink,
  writeFile,
} from 'node:fs/promises';
import { dirname, join } from 'node:path';

/** What a blob's file holds: its properties and its blocks. */
interface StoredBlob {
  blobId: string;
  size: number;
  createdAt: string;
  blocks: BlobBlock[];
}

/**
 * Filesystem-based implementation of content-addressable blob storage.
 *
 * Every blob is stored as blocks (see `@rljson/bs`): each block once, in a
 * file of its own under `blocks/`, and each blob as a small JSON file under
 * `blobs/` that lists its blocks. Two versions of a file that share most of
 * their bytes therefore share most of their blocks on disk.
 *
 * Deleting a blob deletes its JSON file only. Its blocks stay: other blobs
 * may contain them.
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
   * The path of a block or a blob file. Subdirectories from the first eight
   * characters of the id, two each: `ab/cd/ef/gh/abcdefgh…`.
   * @param kind - `blocks` or `blobs`.
   * @param id - The block's or the blob's id.
   * @param extension - Appended to the file name.
   * @returns The path.
   */
  private _path(
    kind: 'blocks' | 'blobs',
    id: string,
    extension: string,
  ): string {
    const subDirs: string[] = [];
    for (let i = 0; i + 2 <= Math.min(id.length, 8); i += 2) {
      subDirs.push(id.substring(i, i + 2));
    }
    return join(this.baseDir, kind, ...subDirs, `${id}${extension}`);
  }

  /**
   * The path of a block's file.
   * @param blockId - The block.
   * @returns The path.
   */
  private _blockPath(blockId: string): string {
    return this._path('blocks', blockId, '');
  }

  /**
   * The path of a blob's file.
   * @param blobId - The blob.
   * @returns The path.
   */
  private _blobPath(blobId: string): string {
    return this._path('blobs', blobId, '.json');
  }

  /**
   * Whether a file exists.
   * @param path - The file.
   * @returns True if it exists.
   */
  private async _exists(path: string): Promise<boolean> {
    try {
      await access(path);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Write `data` to `path` atomically: stage in a sibling temp file, then rename
   * over the target. A crash mid-write can never leave a half-written (and thus
   * hash-mismatched) block or a truncated blob file. `.tmp` files are ignored
   * by {@link findAllBlobs} (it only reads `*.json`).
   * @param path - Final destination path.
   * @param data - Bytes/string to write.
   */
  private async atomicWrite(
    path: string,
    data: Buffer | string,
  ): Promise<void> {
    await mkdir(dirname(path), { recursive: true });
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

  /**
   * Stores a block unless it is stored already.
   * @param blockId - The block's id, the hash of `bytes`.
   * @param bytes - Its bytes.
   */
  private async _putBlock(blockId: string, bytes: Buffer): Promise<void> {
    const path = this._blockPath(blockId);
    if (!(await this._exists(path))) await this.atomicWrite(path, bytes);
  }

  /**
   * Reads a block.
   * @param blockId - The block.
   * @returns Its bytes.
   */
  private readonly _readBlock = async (blockId: string): Promise<Buffer> => {
    try {
      return await readFile(this._blockPath(blockId));
    } catch {
      throw new Error(`Block not found: ${blockId}`);
    }
  };

  /**
   * Reads a blob's file.
   * @param blobId - The blob.
   * @returns What the file holds.
   */
  private async _readBlob(blobId: string): Promise<StoredBlob> {
    let text: string;
    try {
      text = await readFile(this._blobPath(blobId), 'utf8');
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }
    return JSON.parse(text) as StoredBlob;
  }

  /**
   * The properties in a blob's file.
   * @param stored - What the file holds.
   * @returns The properties.
   */
  private static _properties(stored: StoredBlob): BlobProperties {
    return {
      blobId: stored.blobId,
      size: stored.size,
      createdAt: new Date(stored.createdAt),
    };
  }

  /**
   * Writes a blob's file unless the blob is stored already.
   * @param blobId - The blob.
   * @param size - Its size.
   * @param blocks - Its blocks, all stored.
   * @returns The blob's properties.
   */
  private async _storeBlob(
    blobId: string,
    size: number,
    blocks: BlobBlock[],
  ): Promise<BlobProperties> {
    try {
      return BsFs._properties(await this._readBlob(blobId));
    } catch {
      // Not stored yet.
    }
    const stored: StoredBlob = {
      blobId,
      size,
      createdAt: new Date().toISOString(),
      blocks,
    };
    await this.atomicWrite(this._blobPath(blobId), JSON.stringify(stored));
    return BsFs._properties(stored);
  }

  /**
   * Stores a blob. A stream is cut into blocks as it arrives and never held
   * whole: at most one block and one incoming piece are in memory.
   * @param content - The content.
   * @returns The blob's properties.
   */
  async setBlob(
    content: Buffer | string | ReadableStream,
  ): Promise<BlobProperties> {
    const { blobId, size, blocks } = await writeBlocks(
      content,
      ({ blockId }, bytes) => this._putBlock(blockId, bytes),
    );
    return this._storeBlob(blobId, size, blocks);
  }

  /**
   * Reads a blob, or a range of it. A range reads only the blocks it
   * touches.
   * @param blobId - The blob.
   * @param options - Download options.
   * @returns Its content and properties.
   */
  async getBlob(
    blobId: string,
    options?: DownloadBlobOptions,
  ): Promise<{ content: Buffer; properties: BlobProperties }> {
    const stored = await this._readBlob(blobId);
    const content = await readBlocks(
      stored.blocks,
      this._readBlock,
      options?.range,
    );
    return { content, properties: BsFs._properties(stored) };
  }

  /**
   * Streams a blob, one block at a time.
   * @param blobId - The blob.
   * @returns The stream.
   */
  async getBlobStream(blobId: string): Promise<ReadableStream> {
    const { blocks } = await this._readBlob(blobId);
    return streamBlocks(blobId, blocks, this._readBlock);
  }

  /**
   * Deletes a blob's file. Its blocks stay: other blobs may contain them.
   * @param blobId - The blob.
   */
  async deleteBlob(blobId: string): Promise<void> {
    try {
      await unlink(this._blobPath(blobId));
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }
  }

  async blobExists(blobId: string): Promise<boolean> {
    return this._exists(this._blobPath(blobId));
  }

  async getBlobProperties(blobId: string): Promise<BlobProperties> {
    return BsFs._properties(await this._readBlob(blobId));
  }

  async getBlobBlocks(blobId: string): Promise<BlobBlock[]> {
    return (await this._readBlob(blobId)).blocks;
  }

  async getBlock(blockId: string): Promise<Buffer> {
    return this._readBlock(blockId);
  }

  async blockExists(blockId: string): Promise<boolean> {
    return this._exists(this._blockPath(blockId));
  }

  async setBlock(content: Buffer): Promise<BlobBlock> {
    const blockId = ContentId.of(content);
    await this._putBlock(blockId, content);
    return { blockId, size: content.length };
  }

  async setBlobBlocks(
    blobId: string,
    blocks: BlobBlock[],
  ): Promise<BlobProperties> {
    if (await this.blobExists(blobId)) {
      return this.getBlobProperties(blobId);
    }
    const size = await verifyBlob(blobId, blocks, this._readBlock);
    return this._storeBlob(blobId, size, blocks);
  }

  /**
   * Deletes what the layout before blocks left in the base directory: one
   * file per blob, in directories of two characters directly under it. The
   * blobs there cannot be read any more. Everything else in the base
   * directory stays: `blocks/`, `blobs/` and anything else kept beside them.
   * @returns How many directories were deleted.
   */
  async removeOldLayout(): Promise<number> {
    let entries: Dirent[];
    try {
      entries = await readdir(this.baseDir, { withFileTypes: true });
    } catch {
      return 0;
    }
    const old = entries.filter(
      (entry) => entry.isDirectory() && entry.name.length === 2,
    );
    for (const entry of old) {
      await rm(join(this.baseDir, entry.name), {
        recursive: true,
        force: true,
      });
    }
    return old.length;
  }

  /**
   * Recursively find all blob files in the storage directory
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
          } else if (entry.isFile() && entry.name.endsWith('.json')) {
            try {
              const stored = JSON.parse(
                await readFile(fullPath, 'utf8'),
              ) as StoredBlob;
              blobs.push(BsFs._properties(stored));
            } catch {
              // Skip invalid blob files
            }
          }
        }
      } catch {
        // Directory doesn't exist or can't be read
      }
    };

    await scanDir(join(this.baseDir, 'blobs'));
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
    // Check if blob exists
    if (!(await this.blobExists(blobId))) {
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
