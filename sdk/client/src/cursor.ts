import type { Comparison, Document, Filter, SortField, Value } from './types.js'
import { QueryValidationError } from './types.js'
import { getPath } from './safe-value.js'
import { decodeValue, encodeValue, type WireValue } from './wire.js'

interface CursorPayload { readonly version: 1, readonly sort: readonly SortField[], readonly values: readonly WireValue[] }

export interface PageResult<T extends Document> { readonly documents: readonly T[], readonly nextCursor?: string }

export function pageCursorFor (document: Document, sort: readonly SortField[]): string {
  const normalized = normalizePageSort(sort)
  const values = normalized.map((field) => {
    const found = getPath(document, field.path)
    if (!found.found) throw new QueryValidationError(`Cannot create cursor: document is missing ${field.path}`)
    return encodeValue(found.value as Value)
  })
  return encodeCursor({ version: 1, sort: normalized, values })
}

export function pageFilterAfter (cursor: string, sort: readonly SortField[]): Filter {
  const normalized = normalizePageSort(sort)
  const payload = decodeCursor(cursor)
  if (!sameSort(payload.sort, normalized) || payload.values.length !== normalized.length) throw new QueryValidationError('Page cursor does not match this query sort')
  const values = payload.values.map(decodeValue)
  const branches: Filter[] = []
  for (let index = 0; index < normalized.length; index += 1) {
    const branch: Record<string, Value | Comparison> = {}
    for (let previous = 0; previous < index; previous += 1) branch[normalized[previous]!.path] = values[previous]!
    const field = normalized[index]!
    branch[field.path] = field.direction === 1 ? { $gt: values[index]! } : { $lt: values[index]! }
    branches.push(branch)
  }
  return { $or: branches }
}

export function normalizePageSort (sort: readonly SortField[]): readonly SortField[] {
  if (!Array.isArray(sort) || sort.length === 0) throw new QueryValidationError('Seek pagination requires at least one sort field')
  const normalized = sort.map((field) => ({ ...field }))
  const ids = normalized.filter((field) => field.path === '_id')
  if (ids.length > 1) throw new QueryValidationError('Page sort cannot contain _id more than once')
  if (ids.length === 0) normalized.push({ path: '_id', direction: normalized.at(-1)!.direction })
  return Object.freeze(normalized)
}

function encodeCursor (payload: CursorPayload): string {
  const bytes = new TextEncoder().encode(JSON.stringify(payload))
  let binary = ''
  for (const byte of bytes) binary += String.fromCharCode(byte)
  return btoa(binary).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '')
}

function decodeCursor (cursor: string): CursorPayload {
  if (typeof cursor !== 'string' || cursor.length === 0 || cursor.length > 16_384) throw new QueryValidationError('Malformed page cursor')
  try {
    const normalized = cursor.replaceAll('-', '+').replaceAll('_', '/')
    const binary = atob(normalized + '='.repeat((4 - normalized.length % 4) % 4))
    const payload = JSON.parse(new TextDecoder().decode(Uint8Array.from(binary, (character) => character.charCodeAt(0))))
    if (!payload || payload.version !== 1 || !Array.isArray(payload.sort) || !Array.isArray(payload.values)) throw new Error('invalid')
    const sort = payload.sort.map((field: unknown) => {
      if (!field || typeof field !== 'object') throw new Error('invalid')
      const candidate = field as Record<string, unknown>
      if (typeof candidate.path !== 'string' || (candidate.direction !== 1 && candidate.direction !== -1)) throw new Error('invalid')
      return { path: candidate.path, direction: candidate.direction } satisfies SortField
    })
    return { version: 1, sort, values: payload.values as WireValue[] }
  } catch { throw new QueryValidationError('Malformed page cursor') }
}

function sameSort (left: readonly SortField[], right: readonly SortField[]): boolean {
  return left.length === right.length && left.every((field, index) => field.path === right[index]!.path && field.direction === right[index]!.direction)
}
