// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { BsFs } from '../src/bs-fs';


describe('BsFs', () => {
  it('should validate a template', () => {
    const bsFs = BsFs.example;
    expect(bsFs).toBeDefined();
  });
});
