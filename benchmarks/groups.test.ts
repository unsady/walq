import { describe, expect, it } from 'vitest'

import {
  groupScenarios,
  positiveSetting,
  runGroupScenario,
  summarizeGroupRuns,
  type GroupRunOutcome,
  type GroupScenario,
} from './groups.js'
import type { Collected } from './harness.js'

function collected(
  outcomes: GroupRunOutcome[],
  failures: string[] = [],
): Collected<GroupRunOutcome> {
  return { outcomes, failures }
}

describe('group scenarios', () => {
  it('keeps the scheduling and stress workloads distinct', () => {
    expect(groupScenarios(512, 64, 64).map(({ name }) => name)).toEqual([
      'ready',
      'saturated',
      'future-groups',
      'future-ready-group',
      'blocked-future',
      'mixed',
      'heavy-fairness',
      'multiple-saturated-due-groups',
    ])
  })

  it('validates scale overrides including unsafe and fractional values', () => {
    expect(positiveSetting(undefined, 64, 'BENCH_GROUPS')).toBe(64)
    expect(positiveSetting('2', 64, 'BENCH_GROUPS')).toBe(2)
    for (const value of ['0', '1.5', '9007199254740992']) {
      expect(() => positiveSetting(value, 64, 'BENCH_GROUPS')).toThrow('BENCH_GROUPS')
    }
  })

  it('measures awaited claims and preserves fairness and blocked selection', async () => {
    const fair = await runGroupScenario({
      name: 'heavy-fairness',
      jobs: 512,
      groups: 64,
      futureGroups: 8,
    })
    expect(fair.claimed).toBe(2048)
    expect(fair.first).toBeGreaterThanOrEqual(0)
    expect(fair.next).toBeGreaterThanOrEqual(0)

    const blocked = await runGroupScenario({
      name: 'multiple-saturated-due-groups',
      jobs: 512,
      groups: 4,
      futureGroups: 8,
    })
    expect(blocked.claimed).toBe(1)
  })
})

describe('group summaries', () => {
  const scenario: GroupScenario = {
    name: 'ready',
    jobs: 512,
    groups: 64,
    futureGroups: 64,
  }

  it('summarizes measured outcomes and records failures', () => {
    const result = summarizeGroupRuns(
      scenario,
      collected(
        [
          { first: 1, next: 2, duration: 7, claims: 3, claimed: 48, servedGroups: 0 },
          { first: 3, next: 4, duration: 9, claims: 3, claimed: 48, servedGroups: 0 },
        ],
        ['run failed'],
      ),
    )

    expect(result.metrics['first claim (µs)']).toBe(1000)
    expect(result.metrics['claimed jobs']).toBe(48)
    expect(result.metrics['claims/sec']).toBeCloseTo((3 / 9) * 1000)
    expect(result.samples[0]?.['claim duration (ms)']).toBe(7)
    expect(result.ok).toBe(false)
    expect(result.notes).toEqual(['run failed'])
  })
})
