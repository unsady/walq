import { describe, expect, it } from 'vitest'

import {
  claimGroupingScenarioName,
  claimGroupingScenarios,
  fullClaimGroupingGrid,
  quickClaimGroupingGrid,
} from './claim-grouping.js'

describe('claim grouping scenarios', () => {
  it('builds the quick matrix', () => {
    const scenarios = claimGroupingScenarios(quickClaimGroupingGrid)

    expect(scenarios).toHaveLength(24)
    expect(new Set(scenarios.map(claimGroupingScenarioName)).size).toBe(24)
  })

  it('covers every requested queue count and limit in the full matrix', () => {
    const scenarios = claimGroupingScenarios(fullClaimGroupingGrid)

    expect(scenarios).toHaveLength(48)
    expect(new Set(scenarios.map((scenario) => scenario.queues))).toEqual(new Set([1, 4, 8, 32]))
    expect(new Set(scenarios.map((scenario) => scenario.limit))).toEqual(new Set([1, 4, 16]))
  })
})
