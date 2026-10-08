import * as Effect from "effect/Effect"
import type * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { ClientMethod } from "../ClickhouseClient.ts"

/** @internal */
export const strictMode = { keeper_map_strict_mode: 1 } as const

// JSON output writes 64-bit integers as bare numbers by default, which
// JavaScript cannot represent exactly; Snowflake ids need them as strings.
/** @internal */
export const int64AsString = { output_format_json_quote_64bit_integers: 1 } as const

/** @internal */
export const asCommand = <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
  Effect.provideService(effect, ClientMethod, "command")

// Strict KeeperMap writes fail the whole Keeper multi-op when a key already
// exists (insert), or when a row read by an `ALTER TABLE ... UPDATE / DELETE`
// was changed or removed before the write (version check).
const conflictMessage = /Transaction failed \((?:Node exists|Bad version|No node)\)/

/** @internal */
export const isConflict = (error: SqlError): boolean => {
  const cause = error.reason.cause
  return typeof cause === "object" && cause !== null && "message" in cause &&
    conflictMessage.test(String(cause.message))
}

/** @internal */
export const stringLiteral = (value: string): string => `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`

// KeeperMap reads a constant `IN` list on the primary key as point lookups.
/** @internal */
export const stringList = (values: ReadonlyArray<string>): string => `(${values.map(stringLiteral).join(", ")})`

// Snowflake ids exceed the safe integer range, so they travel as decimal
// strings and are inlined only after checking they are integers.
/** @internal */
export const int64List = (values: Iterable<string>): string => {
  const items: Array<string> = []
  for (const value of values) {
    if (!/^-?\d+$/.test(value)) {
      throw new Error(`Expected an integer id, got "${value}"`)
    }
    items.push(value)
  }
  return `(${items.join(", ")})`
}

// Tables in different databases must not share a Keeper path, while the same
// table on every replica of a cluster should.
/** @internal */
export const keeperPath = (
  sql: SqlClient.SqlClient
): Effect.Effect<(table: string) => string, SqlError> =>
  Effect.map(
    sql`SELECT currentDatabase()`.values,
    ([[database]]) => (table: string) => `/effect_cluster/${String(database)}/${table}`
  )
