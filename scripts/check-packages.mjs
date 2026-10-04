import { execFileSync } from 'node:child_process'
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'

const root = resolve(import.meta.dirname, '..')
const packageDirectories = {
  adapter: join(root, 'packages/better-sqlite3'),
  core: join(root, 'packages/core'),
  sqlite: join(root, 'packages/sqlite'),
  common: join(root, 'packages/sqlite-common'),
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

function writeJson(directory, filename, value) {
  writeFileSync(join(directory, filename), `${JSON.stringify(value, null, 2)}\n`)
}

function checkAdapter(key, tarballs) {
  const directory = join(temporaryDirectory, key)
  mkdirSync(directory)
  const builtin = key === 'sqlite'
  const name = manifests[key].name
  const other = builtin ? '@walq/better-sqlite3' : '@walq/sqlite'
  const imports = builtin
    ? `import { DatabaseSync } from 'node:sqlite'
import { sqlite, createStorage } from '@walq/sqlite'`
    : `import Database from 'better-sqlite3'
import { betterSqlite3, createStorage } from '@walq/better-sqlite3'`
  const connection = builtin ? "new DatabaseSync(':memory:')" : "new Database(':memory:')"
  const factory = builtin ? 'sqlite' : 'betterSqlite3'

  writeJson(directory, 'package.json', {
    private: true,
    type: 'module',
    dependencies: {
      [name]: `file:${tarballs[key]}`,
      '@walq/core': `file:${tarballs.core}`,
      ...(!builtin && { 'better-sqlite3': '^13.0.3' }),
    },
    devDependencies: { '@types/node': manifests[key].devDependencies['@types/node'] },
    // Resolve the unpublished shared dependency from its tarball, not the workspace.
    overrides: { '@walq/sqlite-common': `file:${tarballs.common}` },
  })
  writeFileSync(
    join(directory, 'smoke.mjs'),
    `${imports}
import assert from 'node:assert/strict'
import { Queue } from '@walq/core'

assert.throws(() => import.meta.resolve('${other}'), { code: 'ERR_MODULE_NOT_FOUND' })
${builtin ? "assert.throws(() => import.meta.resolve('better-sqlite3'), { code: 'ERR_MODULE_NOT_FOUND' })" : ''}

const db = ${connection}
const queue = new Queue('smoke', { storage: ${factory}(db) })
let resolveHandled
let timeout
const handled = new Promise((resolve, reject) => {
  resolveHandled = resolve
  timeout = setTimeout(() => reject(new Error('Worker timed out')), 5000)
})
const worker = queue.process((data) => resolveHandled(data.value))

try {
  await queue.add({ value: 'ok' })
  assert.equal(await handled, 'ok')
} finally {
  clearTimeout(timeout)
  await worker.close()
  db.close()
}

for (const worker of [false, true]) {
  const storage = await createStorage({ filename: ':memory:', worker })
  try {
    const job = await storage.enqueue({
      queue: 'managed', name: 'job', data: '{}', now: 10,
      availableAt: 10, priority: 0, attempts: 1,
    })
    assert.equal((await storage.inspect({ queue: 'managed', id: job.id })).id, job.id)
  } finally {
    await storage.close()
  }
}
`,
  )
  writeFileSync(
    join(directory, 'types.ts'),
    `${imports}
import { Queue } from '@walq/core'
import type { Storage } from '@walq/core/storage'

const storage: Storage = ${factory}(${connection})
new Queue('typed', { storage })
const managed = await createStorage({ filename: ':memory:', worker: true })
new Queue('managed', { storage: managed })
await managed.close()
`,
  )
  writeJson(directory, 'tsconfig.json', {
    compilerOptions: {
      lib: ['ES2022'],
      module: 'NodeNext',
      moduleResolution: 'NodeNext',
      types: ['node'],
      noEmit: true,
      strict: true,
    },
    include: ['types.ts'],
  })

  run('npm', ['install', '--no-audit', '--no-fund'], directory)
  run(process.execPath, ['smoke.mjs'], directory)
  run(join(root, 'node_modules/.bin/tsc'), ['-p', 'tsconfig.json'], directory)

  for (const manifest of [manifests[key], manifests.common]) {
    const installed = readFileSync(
      join(directory, 'node_modules', manifest.name, 'package.json'),
      'utf8',
    )
    if (installed.includes('workspace:')) {
      throw new Error(`${manifest.name} published manifest contains a workspace dependency`)
    }
    const dependencies = JSON.parse(installed).dependencies ?? {}
    if (other in dependencies || name in dependencies) {
      throw new Error(`${manifest.name} depends on a SQLite adapter`)
    }
  }
}

try {
  run('pnpm', ['build'])
  for (const directory of Object.values(packageDirectories)) {
    run('pnpm', ['exec', 'publint', directory, '--pack=pnpm', '--strict'])
    run('pnpm', ['exec', 'attw', '--pack', directory, '--profile', 'esm-only', '--quiet'])
  }

  const tarballs = Object.fromEntries(
    Object.entries(packageDirectories).map(([key, directory]) => [
      key,
      pack(directory, tarballName(manifests[key])),
    ]),
  )
  checkAdapter('adapter', tarballs)
  checkAdapter('sqlite', tarballs)
} finally {
  rmSync(temporaryDirectory, { force: true, recursive: true })
}
