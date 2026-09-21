import { execFileSync } from 'node:child_process'
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const packageDirectories = {
  adapter: join(root, 'packages/better-sqlite3'),
  walq: join(root, 'packages/walq'),
}
const manifests = Object.fromEntries(
  Object.entries(packageDirectories).map(([key, directory]) => [
    key,
    JSON.parse(readFileSync(join(directory, 'package.json'), 'utf8')),
  ]),
)
const temporaryDirectory = mkdtempSync(join(tmpdir(), 'walq-pack-'))

function tarballName(manifest) {
  return `${manifest.name.replace('@', '').replace('/', '-')}-${manifest.version}.tgz`
}

function run(command, args, cwd = root) {
  execFileSync(command, args, { cwd, stdio: 'inherit' })
}

function pack(packageDirectory, tarballName) {
  run('pnpm', ['pack', '--pack-destination', temporaryDirectory], packageDirectory)
  const tarball = join(temporaryDirectory, tarballName)
  const contents = execFileSync('tar', ['-tf', tarball], { encoding: 'utf8' })
  if (/package\/src\//.test(contents) || /\.test\.[cm]?[jt]s$/m.test(contents)) {
    throw new Error(`${tarballName} contains source tests or fixtures`)
  }
  return tarball
}

try {
  run('pnpm', ['build'])
  run('pnpm', ['exec', 'publint', 'packages/walq', '--pack=pnpm', '--strict'])
  run('pnpm', ['exec', 'publint', 'packages/better-sqlite3', '--pack=pnpm', '--strict'])
  run('pnpm', ['exec', 'attw', '--pack', 'packages/walq', '--profile', 'esm-only', '--quiet'])
  run('pnpm', [
    'exec',
    'attw',
    '--pack',
    'packages/better-sqlite3',
    '--profile',
    'esm-only',
    '--quiet',
  ])

  const walq = pack(packageDirectories.walq, tarballName(manifests.walq))
  const adapter = pack(packageDirectories.adapter, tarballName(manifests.adapter))

  writeFileSync(
    join(temporaryDirectory, 'package.json'),
    `${JSON.stringify(
      {
        private: true,
        type: 'module',
        dependencies: {
          '@walq/better-sqlite3': `file:${adapter}`,
          'better-sqlite3': '^13.0.3',
          walq: `file:${walq}`,
        },
      },
      null,
      2,
    )}\n`,
  )
  writeFileSync(
    join(temporaryDirectory, 'smoke.mjs'),
    `import Database from 'better-sqlite3'
import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from 'walq'

const db = new Database(':memory:')
const queue = new Queue('smoke', { storage: betterSqlite3(db) })
let resolveHandled
const handled = new Promise((resolve) => { resolveHandled = resolve })
const worker = queue.process((data) => resolveHandled(data.value))
await queue.add({ value: 'ok' })
if (await handled !== 'ok') throw new Error('Unexpected job payload')
await worker.close()
db.close()
`,
  )
  writeFileSync(
    join(temporaryDirectory, 'types.ts'),
    `import { betterSqlite3 } from '@walq/better-sqlite3'
import { Queue } from 'walq'
import type { Storage } from 'walq/storage'

const acceptsStorage = (storage: Storage) => new Queue('typed', { storage })
void acceptsStorage
void betterSqlite3
`,
  )
  writeFileSync(
    join(temporaryDirectory, 'tsconfig.json'),
    `${JSON.stringify(
      {
        compilerOptions: {
          lib: ['ES2022'],
          module: 'NodeNext',
          moduleResolution: 'NodeNext',
          noEmit: true,
          strict: true,
        },
        include: ['types.ts'],
      },
      null,
      2,
    )}\n`,
  )

  run('npm', ['install', '--no-audit', '--no-fund'], temporaryDirectory)
  run(process.execPath, ['smoke.mjs'], temporaryDirectory)
  run(join(root, 'node_modules/.bin/tsc'), ['-p', 'tsconfig.json'], temporaryDirectory)

  const adapterManifest = readFileSync(
    join(temporaryDirectory, 'node_modules/@walq/better-sqlite3/package.json'),
    'utf8',
  )
  if (adapterManifest.includes('workspace:')) {
    throw new Error('Published adapter manifest contains a workspace dependency')
  }
} finally {
  rmSync(temporaryDirectory, { force: true, recursive: true })
}
