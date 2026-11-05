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
  rmdir,
  stat,
  unlink,
  utimes,
  writeFile,
} from 'node:fs/promises';
import { basename, dirname, join } from 'node:path';

/** What a blob's file holds: its properties and its blocks. */
interface StoredBlob {
  blobId: string;
  size: number;
  createdAt: string;
  blocks: BlobBlock[];
}

/** Options of {@link BsFs.collectGarbage}. */
export interface CollectGarbageOptions {
  /**
   * How long a block no blob lists is kept after it was last stored, in
   * milliseconds. Defaults to {@link BsFs.defaultGraceMs}.
   */
  readonly graceMs?: number;
}

/** What {@link BsFs.collectGarbage} removed. */
export interface CollectGarbageResult {
  /** The block files removed. */
  readonly removedBlocks: number;
  /** The bytes of the block files removed. */
  readonly freedBytes: number;
}

/** What {@link BsFs.usage} finds on disk. */
export interface BsFsUsage {
  /** The blob files. */
  readonly blobs: number;
  /** The block files. */
  readonly blocks: number;
  /** The bytes of the block files. */
  readonly bytes: number;
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
 * may contain them. {@link BsFs.collectGarbage} removes the blocks no blob
 * lists any more.
 */
export class BsFs implements Bs {
  private readonly baseDir: string;

  /** Each block's latest operation; the next one on the block waits for it. */
  readonly #blockOps = new Map<string, Promise<unknown>>();

  /** The blocks of each blob being written: no sweep removes them. */
  readonly #writing = new Set<Set<string>>();

  /** The latest sweep; the next one waits for it. */
  #collecting: Promise<unknown> = Promise.resolve();

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
   * How long {@link BsFs.collectGarbage} keeps a block no blob lists after
   * it was last stored, unless told otherwise: ten minutes.
   */
  static readonly defaultGraceMs = 10 * 60 * 1000;

  /**
   * The path of a block or a blob file. Subdirectories from the first eight
   * characters of the id, two each: `ab/cd/ef/gh/abcdefgh…`. Fails for an id
   * that is no plain id, before anything touches the disk.
   * @param kind - `block` or `blob`.
   * @param id - The block's or the blob's id.
   * @param extension - Appended to the file name.
   * @returns The path.
   */
  private _path(kind: 'block' | 'blob', id: string, extension: string): string {
    BsFs._checkId(kind, id);
    const subDirs: string[] = [];
    for (let i = 0; i + 2 <= Math.min(id.length, 8); i += 2) {
      subDirs.push(id.substring(i, i + 2));
    }
    return join(this.baseDir, `${kind}s`, ...subDirs, `${id}${extension}`);
  }

  /**
   * Fails for an id that is no plain id.
   * @param kind - `block` or `blob`.
   * @param id - The id.
   */
  private static _checkId(kind: 'block' | 'blob', id: string): void {
    if (!BsFs._isPlainId(id)) {
      throw new Error(`Invalid ${kind} id: ${JSON.stringify(id)}`);
    }
  }

  /**
   * Whether an id is plain: one or more letters, digits, `-` and `_`, the
   * characters of every id the store makes. Its path stays inside the store.
   * @param id - The id.
   * @returns True if it is.
   */
  private static _isPlainId(id: string): boolean {
    return /^[\w-]+$/.test(id);
  }

  /**
   * The path of a block's file.
   * @param blockId - The block.
   * @returns The path.
   */
  private _blockPath(blockId: string): string {
    return this._path('block', blockId, '');
  }

  /**
   * The path of a blob's file.
   * @param blobId - The blob.
   * @returns The path.
   */
  private _blobPath(blobId: string): string {
    return this._path('blob', blobId, '.json');
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
   * Stores a block unless it is stored already. A block stored already gets
   * now as its modification time, from which the grace period of
   * {@link BsFs.collectGarbage} counts.
   * @param blockId - The block's id, the hash of `bytes`.
   * @param bytes - Its bytes.
   */
  private async _putBlock(blockId: string, bytes: Buffer): Promise<void> {
    const path = this._blockPath(blockId);
    await this._onBlock(blockId, async () => {
      if (!(await BsFs._touch(path))) await this.atomicWrite(path, bytes);
    });
  }

  /**
   * Runs an operation on a block once the operations on it that started
   * earlier are done, so that storing a block and removing it never overlap.
   * @param blockId - The block.
   * @param op - The operation.
   * @returns What the operation returns.
   */
  private async _onBlock<T>(blockId: string, op: () => Promise<T>): Promise<T> {
    const run = Promise.allSettled([this.#blockOps.get(blockId)]).then(op);
    this.#blockOps.set(blockId, run);
    try {
      return await run;
    } finally {
      if (this.#blockOps.get(blockId) === run) this.#blockOps.delete(blockId);
    }
  }

  /**
   * Sets a file's modification time to now.
   * @param path - The file.
   * @returns False if there is no such file.
   */
  private static async _touch(path: string): Promise<boolean> {
    const now = new Date();
    try {
      await utimes(path, now, now);
      return true;
    } catch (err) {
      BsFs._unlessMissing(err);
      return false;
    }
  }

  /**
   * Rethrows an error unless it says that a file or a directory is missing:
   * removed meanwhile, or never there.
   * @param err - The error.
   */
  private static _unlessMissing(err: unknown): void {
    if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err;
  }

  /**
   * Writes a blob. Until the blob's file is written, and until every sweep
   * that runs by then is done, no sweep removes the blocks `write` lists:
   * a sweep that read the blob files before this blob's file was written
   * would not know that the blob lists them.
   * @param write - Writes the blob; lists each block before it stores it.
   * @returns What `write` returns.
   */
  private async _whileWriting<T>(
    write: (blockIds: Set<string>) => Promise<T>,
  ): Promise<T> {
    const blockIds = new Set<string>();
    this.#writing.add(blockIds);
    try {
      return await write(blockIds);
    } finally {
      void Promise.allSettled([this.#collecting]).then(() =>
        this.#writing.delete(blockIds),
      );
    }
  }

  /**
   * Reads a block.
   * @param blockId - The block.
   * @returns Its bytes.
   */
  private readonly _readBlock = async (blockId: string): Promise<Buffer> => {
    const path = this._blockPath(blockId);
    try {
      return await readFile(path);
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
    const path = this._blobPath(blobId);
    let text: string;
    try {
      text = await readFile(path, 'utf8');
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
    return this._whileWriting(async (writing) => {
      const { blobId, size, blocks } = await writeBlocks(
        content,
        ({ blockId }, bytes) => {
          writing.add(blockId);
          return this._putBlock(blockId, bytes);
        },
      );
      return this._storeBlob(blobId, size, blocks);
    });
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
   * {@link BsFs.collectGarbage} removes those no blob lists any more.
   * @param blobId - The blob.
   */
  async deleteBlob(blobId: string): Promise<void> {
    const path = this._blobPath(blobId);
    try {
      await unlink(path);
    } catch {
      throw new Error(`Blob not found: ${blobId}`);
    }
  }

  async blobExists(blobId: string): Promise<boolean> {
    if (!BsFs._isPlainId(blobId)) return false;
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
    if (!BsFs._isPlainId(blockId)) return false;
    return this._exists(this._blockPath(blockId));
  }

  /**
   * Stores one block. A block stored already gets now as its modification
   * time: {@link BsFs.collectGarbage} keeps it for another grace period, so
   * that it is still there when the blob that lists it is stored.
   * @param content - The block's bytes.
   * @returns The block.
   */
  async setBlock(content: Buffer): Promise<BlobBlock> {
    const blockId = ContentId.of(content);
    await this._putBlock(blockId, content);
    return { blockId, size: content.length };
  }

  /**
   * Stores a blob made of blocks stored already. The blocks get now as their
   * modification time, and no sweep of this instance removes them while the
   * blob is stored.
   * @param blobId - The blob.
   * @param blocks - Its blocks, in order.
   * @returns The blob's properties.
   */
  async setBlobBlocks(
    blobId: string,
    blocks: BlobBlock[],
  ): Promise<BlobProperties> {
    BsFs._checkId('blob', blobId);
    for (const { blockId } of blocks) BsFs._checkId('block', blockId);
    if (await this.blobExists(blobId)) {
      return this.getBlobProperties(blobId);
    }
    return this._whileWriting(async (writing) => {
      for (const { blockId } of blocks) writing.add(blockId);
      await this._touchBlocks(writing);
      const size = await verifyBlob(blobId, blocks, this._readBlock);
      return this._storeBlob(blobId, size, blocks);
    });
  }

  /**
   * Sets the modification time of blocks to now. A missing block is left to
   * the reading that follows.
   * @param blockIds - The blocks.
   */
  private async _touchBlocks(blockIds: Iterable<string>): Promise<void> {
    for (const blockId of blockIds) {
      await this._onBlock(blockId, () =>
        BsFs._touch(this._blockPath(blockId)),
      );
    }
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
   * Removes the block files no blob file lists, once their grace period has
   * passed, and the directories they leave empty: the space that
   * {@link BsFs.deleteBlob} leaves behind.
   *
   * The grace period counts from when a block was last stored: `setBlob`,
   * `setBlock` and `setBlobBlocks` set the modification time of every block
   * they store or list to now. It keeps the blocks of a blob that is stored
   * block by block and does not list them yet, and the blocks that another
   * instance on the same directory is writing; it must be longer than such
   * a blob takes. The blocks of a blob that this instance is writing are
   * kept whatever their age.
   *
   * Reads every blob file before it removes anything. If one cannot be
   * read, it removes nothing and rejects: a sweep without the blocks that
   * file lists could remove blocks a blob still needs. A file that is gone
   * by the time it is read is skipped. Any other error of the file system
   * stops the sweep and rejects; what was removed until then stays removed.
   *
   * One sweep runs at a time: a call made while one runs starts after it.
   * @param options - The grace period.
   * @returns How many block files were removed, and their bytes.
   */
  async collectGarbage(
    options: CollectGarbageOptions = {},
  ): Promise<CollectGarbageResult> {
    const graceMs = options.graceMs ?? BsFs.defaultGraceMs;
    if (Number.isNaN(graceMs) || graceMs < 0) {
      throw new Error(
        `graceMs must be 0 or more milliseconds, not ${String(graceMs)}`,
      );
    }
    const run = Promise.allSettled([this.#collecting]).then(() =>
      this._collect(graceMs),
    );
    this.#collecting = run;
    return run;
  }

  /**
   * Counts what the store holds on disk: the blob files, the block files and
   * the bytes of the block files. A block that no blob lists any more counts
   * until {@link BsFs.collectGarbage} removes it; staged files (`*.tmp`) do
   * not count. A file that is gone by the time it is measured is skipped; any
   * other error of the file system rejects.
   * @returns The counts.
   */
  async usage(): Promise<BsFsUsage> {
    let blobs = 0;
    await this._forEachBlobFile(() => {
      blobs++;
    });
    let blocks = 0;
    let bytes = 0;
    await this._forEachBlockFile(async (_blockId, path) => {
      const size = await BsFs._sizeOf(path);
      if (size === undefined) return;
      blocks++;
      bytes += size;
    });
    return { blobs, blocks, bytes };
  }

  /**
   * One sweep: reads the blocks every blob file lists, then removes the
   * block files of the others that were last stored before the grace period.
   * @param graceMs - The grace period.
   * @returns How many block files were removed, and their bytes.
   */
  private async _collect(graceMs: number): Promise<CollectGarbageResult> {
    const cutoff = Date.now() - graceMs;
    const listed = await this._listedBlocks();
    let removedBlocks = 0;
    let freedBytes = 0;
    await this._forEachBlockFile(async (blockId, path) => {
      if (listed.has(blockId)) return;
      const freed = await this._onBlock(blockId, () =>
        this._removeBlock(blockId, path, cutoff),
      );
      if (freed === undefined) return;
      removedBlocks++;
      freedBytes += freed;
    });
    return { removedBlocks, freedBytes };
  }

  /**
   * The blocks the blob files list.
   * @returns Their ids.
   */
  private async _listedBlocks(): Promise<Set<string>> {
    const listed = new Set<string>();
    await this._forEachBlobFile(async (path) => {
      for (const blockId of await BsFs._blocksListedIn(path)) {
        listed.add(blockId);
      }
    });
    return listed;
  }

  /**
   * The blocks a blob file lists. Fails if the file is no blob file.
   * @param path - The blob file.
   * @returns Their ids; none if the file is gone.
   */
  private static async _blocksListedIn(path: string): Promise<string[]> {
    let text: string;
    try {
      text = await readFile(path, 'utf8');
    } catch (err) {
      BsFs._unlessMissing(err);
      return [];
    }
    const blockIds = BsFs._parseBlockIds(text);
    if (blockIds === undefined) {
      throw new Error(
        `Cannot read the blob file ${path}; no block was removed`,
      );
    }
    return blockIds;
  }

  /**
   * The ids of the blocks in the text of a blob file.
   * @param text - The text.
   * @returns The ids, or `undefined` if the text is no blob file.
   */
  private static _parseBlockIds(text: string): string[] | undefined {
    let parsed: unknown;
    try {
      parsed = JSON.parse(text);
    } catch {
      return undefined;
    }
    const blocks = (parsed as { blocks?: unknown } | null)?.blocks;
    if (!Array.isArray(blocks)) return undefined;
    const ids: unknown[] = blocks.map(
      (block: { blockId?: unknown } | null) => block?.blockId,
    );
    return ids.every((id): id is string => typeof id === 'string')
      ? ids
      : undefined;
  }

  /**
   * Removes a block's file and the directories it leaves empty, unless a blob
   * being written lists the block or the block was stored after `cutoff`.
   * @param blockId - The block.
   * @param path - Its file.
   * @param cutoff - When the grace period began.
   * @returns The bytes freed; `undefined` if the file stays or is gone.
   */
  private async _removeBlock(
    blockId: string,
    path: string,
    cutoff: number,
  ): Promise<number | undefined> {
    if (this._isBeingWritten(blockId)) return undefined;
    let size: number;
    try {
      const stats = await stat(path);
      if (stats.mtimeMs >= cutoff) return undefined;
      await unlink(path);
      size = stats.size;
    } catch (err) {
      BsFs._unlessMissing(err);
      return undefined;
    }
    await this._removeEmptyDirs(dirname(path));
    return size;
  }

  /**
   * Whether a blob being written lists a block.
   * @param blockId - The block.
   * @returns True if one does.
   */
  private _isBeingWritten(blockId: string): boolean {
    return [...this.#writing].some((blockIds) => blockIds.has(blockId));
  }

  /**
   * Removes a block's directory and its parents below `blocks/` as long as
   * they are empty.
   * @param dir - The block's directory.
   */
  private async _removeEmptyDirs(dir: string): Promise<void> {
    const blocks = join(this.baseDir, 'blocks');
    for (let d = dir; d.length > blocks.length; d = dirname(d)) {
      try {
        await rmdir(d);
      } catch {
        return; // Not empty: it holds another block, or a write under way
      }
    }
  }

  /**
   * The size of a file.
   * @param path - The file.
   * @returns Its size; `undefined` if it is gone.
   */
  private static async _sizeOf(path: string): Promise<number | undefined> {
    try {
      return (await stat(path)).size;
    } catch (err) {
      BsFs._unlessMissing(err);
      return undefined;
    }
  }

  /**
   * Visits every blob file, one at a time. Staged files (`*.tmp`) are none.
   * @param visit - Takes the path of one blob file.
   */
  private async _forEachBlobFile(
    visit: (path: string) => Promise<void> | void,
  ): Promise<void> {
    await BsFs._forEachFile(join(this.baseDir, 'blobs'), async (path) => {
      if (path.endsWith('.json')) await visit(path);
    });
  }

  /**
   * Visits every block file, one at a time. Staged files (`*.tmp`) are none.
   * @param visit - Takes the block and the path of one block file.
   */
  private async _forEachBlockFile(
    visit: (blockId: string, path: string) => Promise<void>,
  ): Promise<void> {
    await BsFs._forEachFile(join(this.baseDir, 'blocks'), async (path) => {
      const blockId = basename(path);
      if (BsFs._isPlainId(blockId)) await visit(blockId, path);
    });
  }


  /**
   * Visits every file below a directory, one at a time. A directory that is
   * missing, removed meanwhile or never there, holds none.
   * @param dir - The directory.
   * @param visit - Takes the path of one file.
   */
  private static async _forEachFile(
    dir: string,
    visit: (path: string) => Promise<void>,
  ): Promise<void> {
    let entries: Dirent[];
    try {
      entries = await readdir(dir, { withFileTypes: true });
    } catch (err) {
      BsFs._unlessMissing(err);
      return;
    }
    for (const entry of entries) {
      const path = join(dir, entry.name);
      if (entry.isDirectory()) await BsFs._forEachFile(path, visit);
      else await visit(path);
    }
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
    BsFs._checkId('blob', blobId);
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
