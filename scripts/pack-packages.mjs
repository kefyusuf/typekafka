import { execFileSync } from 'node:child_process';
import { mkdirSync } from 'node:fs';

const PACKAGES = ['packages/broker', 'packages/domain', 'packages/infra'];

// Spawn npm's CLI entry with the current node binary: works on every platform
// and avoids Windows' npm.cmd, which newer Node versions refuse to spawn
// without a shell (EINVAL).
const npmCli = process.env.npm_execpath;
if (!npmCli) {
  console.error('Run this script via npm: npm run pack:packages');
  process.exit(1);
}

mkdirSync('.pack', { recursive: true });

for (const pkg of PACKAGES) {
  execFileSync(process.execPath, [npmCli, 'pack', '--pack-destination', '../../.pack', '--silent'], {
    cwd: pkg,
    stdio: 'inherit',
  });
}

console.log('Packed tarballs are in .pack/ — install them from examples with npm install.');