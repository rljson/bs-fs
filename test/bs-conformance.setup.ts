// @license
// Copyright (c) 2026 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BsMem, BsTestSetup } from '@rljson/bs';

// .............................................................................
/**
 * Test setup for BsMem conformance tests
 */
class MyBsTestSetup implements BsTestSetup {
  bs: BsMem;

  constructor() {
    this.bs = new BsMem();
  }

  async beforeAll(): Promise<void> {
    // Setup before all tests
  }

  async beforeEach(): Promise<void> {
    // Clear before each test
    this.bs.clear();
  }

  async afterEach(): Promise<void> {
    // Cleanup after each test
  }

  async afterAll(): Promise<void> {
    // Cleanup after all tests
  }
}

// .............................................................................
export const testSetup = () => new MyBsTestSetup();
