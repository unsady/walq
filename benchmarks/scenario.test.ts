import { describe, expect, it } from 'vitest'

import { defineScenario, scenarioOf } from './scenario.js'

describe('defineScenario', () => {
  it('returns a callable benchmark function that carries its descriptor', () => {
    const definition = defineScenario({
      suite: 'demo',
      scenario: 'demo scenario',
      jobs: 3,
      run: async () => 'outcome',
      summarize: () => ({
        suite: 'demo',
        scenario: 'demo scenario',
        params: {},
        metrics: {},
        samples: [],
        notes: [],
        ok: true,
      }),
      throughput: () => 1,
      latency: () => [1],
    })

    expect(definition.name).toBe('demo scenario')
    expect(scenarioOf(definition.fn)?.suite).toBe('demo')
    expect(scenarioOf(definition.fn)?.jobs).toBe(3)
  })

  it('ignores values that are not scenario functions', () => {
    expect(scenarioOf(undefined)).toBeUndefined()
    expect(scenarioOf('scenario')).toBeUndefined()
    expect(scenarioOf(() => {})).toBeUndefined()
  })
})
