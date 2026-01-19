/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

// Create a folder dist/conformance-tests if it does not exist
import { existsSync } from 'fs';
import fs from 'fs/promises';
import path from 'path';
import { blue, gray, green, red, yellow } from './functions/colors.js';
import {
  nodeModulesDir,
  scriptsDir,
  testDir,
} from './functions/directories.js';
import { syncFolders } from './functions/sync-folders.js';

// .............................................................................
async function _copyGoldens(srcDir) {
  const from = path.join(srcDir, 'goldens');
  const to = path.join(testDir, 'goldens', 'bs-conformance');
  if (!existsSync(to)) {
    await fs.mkdir(to, { recursive: true });
  }

  console.log(gray(`cp -r ${from} ${to}`));
  await syncFolders(from, to, { excludeHidden: true });
}

// .............................................................................
async function _copyConformanceTests(srcDir) {
  const from = path.join(srcDir, 'bs-conformance.spec.ts');
  const to = path.join(testDir, 'bs-conformance.spec.ts');
  console.log(gray(`cp ${from} ${to}`));
  await fs.copyFile(from, to);
}

// .............................................................................
async function _srcDir() {
  const targetDir = path.join(
    nodeModulesDir,
    '@rljson',
    'bs',
    'dist',
    'conformance-tests',
  );
  if (!existsSync(targetDir)) {
    await fs.mkdir(targetDir, { recursive: true });
  }
  return targetDir;
}

// .............................................................................
async function _copyInstallScript(srcDir) {
  const from = path.join(srcDir, 'scripts', 'install-conformance-tests.js');
  const to = path.join(scriptsDir, 'install-conformance-tests.js');
  console.log(gray(`cp ${from} ${to}`));
  await fs.copyFile(from, to);
}

// .............................................................................
async function _copySetupScript(srcDir) {
  const from = path.join(srcDir, 'bs-conformance.setup.ts');
  const to = path.join(testDir, 'bs-conformance.setup.ts');

  // Only setup file if not exists
  if (existsSync(to)) {
    return;
  }

  // Update exports of Bs and BsTestSetup
  console.log(gray(`cp ${from} ${to}`));
  await fs.copyFile(from, to);

  console.log(
    [
      yellow('Please open'),
      blue('test/bs-conformance.setup.ts'),
      yellow('and modify'),
      green('MyBsTestSetup'),
      yellow('to instantiate your Bs implementation.'),
    ].join(' '),
  );
}

// .............................................................................
try {
  // Create target directory if it doesn't exist
  console.log(blue('Update conformance tests...'));
  const srcDir = await _srcDir();
  await _copyConformanceTests(srcDir);
  await _copyGoldens(srcDir);
  await _copyInstallScript(srcDir);
  await _copySetupScript(srcDir);
} catch (err) {
  console.error(
    red('❌ Error while deploying conformance tests:', err.message),
  );
  process.exit(1);
}
