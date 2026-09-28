import type {
  MaterializeSchedulesInput,
  ScheduleRepeat,
  StoredSchedule,
  UpsertScheduleInput,
} from '@walq/core/storage'
import { CronExpressionParser } from 'cron-parser'

import { isRecord } from './is-record.js'
import { integer, text } from './validation.js'

export type ScheduleRow = StoredSchedule

export function validateUpsertSchedule(input: UpsertScheduleInput): UpsertScheduleInput {
  if (!isRecord(input)) throw new TypeError('schedule input must be an object')

  text(input.queue as string, 'queue')
  text(input.id as string, 'id')
  text(input.data as string, 'data')
  JSON.parse(input.data)
  integer(input.now, 'now')

  const hasEvery = input.every !== undefined
  const hasCron = input.cron !== undefined
  if (hasEvery === hasCron) throw new TypeError('exactly one of every or cron must be provided')

  if (hasEvery) {
    integer(input.every!, 'every', 1)
    return {
      queue: input.queue,
      id: input.id,
      data: input.data,
      now: input.now,
      every: input.every!,
    }
  }

  text(input.cron!, 'cron')
  try {
    CronExpressionParser.parse(input.cron!, { currentDate: input.now, tz: 'UTC' })
  } catch (error) {
    throw new TypeError(`Invalid cron expression: ${String(error)}`)
  }

  return { queue: input.queue, id: input.id, data: input.data, now: input.now, cron: input.cron! }
}

export function validateScheduleInput(
  input: unknown,
): asserts input is { queue: string; id: string } {
  if (!isRecord(input)) throw new TypeError('schedule input must be an object')
  text(input.queue as string, 'queue')
  text(input.id as string, 'id')
}

export function validateMaterializeInput(
  input: MaterializeSchedulesInput,
): MaterializeSchedulesInput {
  if (!isRecord(input)) throw new TypeError('schedule materialization input must be an object')
  text(input.queue, 'queue')
  integer(input.now, 'now')
  integer(input.attempts, 'attempts', 1)
  return { queue: input.queue, now: input.now, attempts: input.attempts }
}

export function getNextRunAt(repeat: ScheduleRepeat, now: number): number {
  let nextRunAt: number
  if ('every' in repeat && repeat.every !== undefined) {
    nextRunAt = now + repeat.every
  } else {
    const cron = repeat.cron
    if (cron === undefined) throw new TypeError('exactly one of every or cron must be provided')
    nextRunAt = CronExpressionParser.parse(cron, { currentDate: now, tz: 'UTC' }).next().getTime()
  }

  integer(nextRunAt, 'nextRunAt')
  return nextRunAt
}

export function nextAfterMissedRun(schedule: ScheduleRow, now: number): number {
  if (schedule.every !== undefined) {
    const elapsed = now - schedule.nextRunAt
    const nextRunAt = now + (schedule.every - (elapsed % schedule.every))
    integer(nextRunAt, 'nextRunAt')
    return nextRunAt
  }

  if (schedule.cron === undefined) throw new TypeError('Stored schedule has no repeat rule')
  return getNextRunAt({ cron: schedule.cron }, now)
}

export function scheduleRow(row: unknown): ScheduleRow {
  if (!isRecord(row)) throw new TypeError('Invalid stored schedule')
  const common = {
    queue: row.queue as string,
    id: row.id as string,
    data: row.data as string,
    nextRunAt: row.nextRunAt as number,
  }
  return row.every === null
    ? { ...common, cron: row.cron as string }
    : { ...common, every: row.every as number }
}
