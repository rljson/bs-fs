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
import { blue, gray, red } from './functions/colors.js';
import { distDir, scriptsDir, testDir } from './functions/directories.js';
import { syncFolders } from './functions/sync-folders.js';

// .............................................................................
async function _copyGoldens(targetDir) {
  const goldensDir = path.join(testDir, 'goldens', 'bs-conformance');
  const targetGoldensDir = path.join(targetDir, 'goldens');
  if (!existsSync(targetGoldensDir)) {
    await fs.mkdir(targetGoldensDir, { recursive: true });
  }

  console.log(gray(`cp ${goldensDir} ${targetGoldensDir}`));
  await syncFolders(goldensDir, targetGoldensDir, { excludeHidden: true });
}

// .............................................................................
async function _copyConformanceTests(targetDir) {
  // Get path to original conformance test file
  const conformanceTestPath = path.join(testDir, 'bs-conformance.spec.ts');

  // Read the content of the conformance test file
  const conformanceTestFileContent = await fs.readFile(
    conformanceTestPath,
    'utf-8',
  );

  // Find the first line that does not start with a comment
  const lines = conformanceTestFileContent.split('\n');
  const firstNonCommentLineIndex = lines.findIndex((line) => {
    return !line.trim().startsWith('//');
  });

  if (firstNonCommentLineIndex < 0 || firstNonCommentLineIndex > 10) {
    throw new Error(
      'bs-conformance.spec.ts does not start with an valid license header',
    );
  }

  // Separate license header from the rest of the file
  const license = lines.slice(0, firstNonCommentLineIndex);
  const rest = lines.slice(firstNonCommentLineIndex);

  // Add modification hint behind the license header
  const dontModifyHint = [
    '// ⚠️ DO NOT MODIFY THIS FILE DIRECTLY ⚠️',
    '// ',
    '// This file is a copy of @rljson/bs/test/bs-conformance.spec.ts.',
    '//',
    '// To make changes, please execute the following steps:',
    '//   1. Clone <https://github.com/rljson/bs>',
    '//   2. Make changes to the original file in the test folder',
    '//   3. Submit a pull request',
    '//   4. Publish a the new changes to npm',
  ];

  let content = [...license, '', ...dontModifyHint, '', ...rest].join('\n');

  // Don't allow npm updateGoldens
  // Make sure "npmUpdateGoldensEnabled: true" is set in the config
  const npmUpdateGoldensEnabled = content.includes(
    'npmUpdateGoldensEnabled: true',
  );
  if (!npmUpdateGoldensEnabled) {
    throw new Error(
      'bs-conformance.spec.ts does not have npmUpdateGoldensEnabled: true',
    );
  }

  // Replace true with false
  content = content.replaceAll(
    'npmUpdateGoldensEnabled: true',
    'npmUpdateGoldensEnabled: false',
  );

  // Replace all occurrences of '../src/' with '@rljson/bs'
  content = content.replaceAll(/'..\/src\/?'/g, "'@rljson/bs'");

  // Write result to the target file
  console.log(gray(`cp ${conformanceTestPath} ${targetDir}`));
  const targetTestPath = path.join(targetDir, 'bs-conformance.spec.ts');
  await fs.writeFile(targetTestPath, content, 'utf-8');
}

// .............................................................................
async function _copyInstallConformanceTests(targetDir) {
  const targetScriptsDir = path.join(targetDir, 'scripts');
  if (!existsSync(targetScriptsDir)) {
    await fs.mkdir(targetScriptsDir, { recursive: true });
  }
  const from = path.join(scriptsDir, 'install-conformance-tests.js');
  const to = path.join(targetScriptsDir, 'install-conformance-tests.js');
  console.log(gray(`cp ${from} ${to}`));
  await fs.copyFile(from, to);
}

// .............................................................................
async function _copyBsConformanceSetup(targetDir) {
  const from = path.join('test', 'bs-conformance.setup.ts');
  const to = path.join(targetDir, 'bs-conformance.setup.ts');

  // Update exports of Bs and BsTestSetup
  const content = await fs.readFile(from, 'utf-8');
  const replacedContent = content.replaceAll(/'...\/src\/?'/g, "'@rljson/bs'");

  console.log(gray(`cp ${from} ${to}`));
  await fs.writeFile(to, replacedContent, 'utf-8');
}

// .............................................................................
async function _targetDir() {
  const targetDir = path.join(distDir, 'conformance-tests');
  if (!existsSync(targetDir)) {
    await fs.mkdir(targetDir, { recursive: true });
  }
  return targetDir;
}

// .............................................................................
try {
  // Create target directory if it doesn't exist
  console.log(blue('Deploy conformance tests...'));
  const targetDir = await _targetDir();
  await _copyConformanceTests(targetDir);
  await _copyGoldens(targetDir);
  await _copyInstallConformanceTests(targetDir);
  await _copyBsConformanceSetup(targetDir);
} catch (err) {
  console.error(
    red('❌ Error while deploying conformance tests:', err.message),
  );
  process.exit(1);
}
