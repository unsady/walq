import { workerData } from 'node:worker_threads'

import { serveStorage, type StorageOptions } from '@walq/sqlite-common'

import { createStorage } from './index.js'

serveStorage(await createStorage(workerData as StorageOptions))
