// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BsTestSetup } from '@rljson/bs';

import { BsFs } from './bs-fs.js';

// .............................................................................
/**
 * Test setup for BsFs conformance tests
 */
class MyBsTestSetup implements BsTestSetup {
  bs: BsFs;

  constructor() {
    this.bs = new BsFs('./test-blobs');
  }

  async beforeAll(): Promise<void> {
    // Setup before all tests
  }

  async beforeEach(): Promise<void> {
    // Clear before each test
    await this.bs.clear();
  }

  async afterEach(): Promise<void> {
    // Cleanup after each test
  }

  async afterAll(): Promise<void> {
    // Cleanup after all tests
    await this.bs.clear();
  }
}

// .............................................................................
export const testSetup = () => new MyBsTestSetup();
