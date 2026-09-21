import { describe, expect, it } from 'vitest'

import {
  completeBatchScenarioName,
  completeBatchScenarios,
  fullCompleteBatchGrid,
  quickCompleteBatchGrid,
} from './complete-batch.js'

describe('complete batch scenarios', () => {
  it('expands the batch tiers of both grids', () => {
    expect(completeBatchScenarios(quickCompleteBatchGrid).map(completeBatchScenarioName)).toEqual([
      'complete batch 1',
      'complete batch 4',
      'complete batch 16',
    ])
    expect(completeBatchScenarios(fullCompleteBatchGrid)).toHaveLength(3)
  })
})
