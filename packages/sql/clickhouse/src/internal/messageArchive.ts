import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import type { SqlError } from "effect/sql/SqlError"
import type { ClickhouseClient } from "../ClickhouseClient.ts"
import * as KeeperMap from "./keeperMap.ts"

/** @internal */
export const tableNames = (prefix: string | undefined) => {
  const table = (name: string) => `${prefix ?? "cluster"}_${name}`
  return {
    messages: table("messages"),
    replies: table("replies"),
    replyAcks: table("reply_acks"),
    messageIds: table("message_ids"),
    pending: table("pending"),
    exits: table("exits"),
    exitsArchive: table("exits_archive"),
    messageIdsArchive: table("message_ids_archive")
  }
}

// Snowflake ids inline as about 20 characters, keys as up to 257, against a
// 256 KiB statement limit.
/** @internal */
export const idChunk = 1000
/** @internal */
export const keyChunk = 200

/** @internal */
export const chunksOf = <A>(items: ReadonlyArray<A>, size: number): Array<ReadonlyArray<A>> => {
  const chunks: Array<ReadonlyArray<A>> = []
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size))
  }
  return chunks
}

/** @internal */
export const forChunks = <A, B, E, R>(
  items: ReadonlyArray<A>,
  size: number,
  f: (chunk: ReadonlyArray<A>) => Effect.Effect<ReadonlyArray<B>, E, R>
): Effect.Effect<Array<B>, E, R> => Effect.map(Effect.forEach(chunksOf(items, size), f), (results) => results.flat())

// An array literal of (key, id) pairs, matched with `has(pairs, (a, b))`,
// which reads the same for one pair or many.
const pairArray = (pairs: ReadonlyArray<readonly [string, string]>, key: (value: string) => string) =>
  `[${pairs.map(([a, b]) => `(${key(a)}, ${int64(b)})`).join(", ")}]`
const int64 = (value: string) => KeeperMap.int64List([value]).slice(1, -1)

/** @internal */
export interface ArchiveHooks {
  /** Runs after the pass has read the exits it is about to archive. */
  readonly afterRead?: (exits: ReadonlyArray<readonly [requestId: string, replyId: string]>) => Effect.Effect<void>
}

/** @internal */
export interface ArchiveResult {
  readonly archived: number
  readonly compensated: number
}

/**
 * Moves the exits (and primary keys) of requests completed longer than
 * `retention` ago from Keeper to the MergeTree archives.
 *
 * Keeper stays authoritative: readers consult the archive only for requests
 * Keeper holds nothing about. The pass writes the archive first, then removes
 * the Keeper entries it read with a conditional delete, and finally checks
 * that Keeper holds nothing about the request any more. If it still does (a
 * resume restored or cleared the exit, or requeued the request in between),
 * the archive row just written is removed again.
 *
 * @internal
 */
export const archivePass = (options: {
  readonly client: ClickhouseClient
  readonly prefix?: string | undefined
  readonly retention: Duration.Input
  readonly batchSize?: number | undefined
  readonly hooks?: ArchiveHooks | undefined
}): Effect.Effect<ArchiveResult, SqlError> =>
  Effect.gen(function*() {
    const { client } = options
    const sql = client.withoutTransforms()
    const tables = tableNames(options.prefix)
    const command = <A>(statement: Effect.Effect<A, SqlError>) => client.asCommand(statement)
    const strict = <A>(statement: Effect.Effect<A, SqlError>) =>
      client.withClickhouseSettings(client.asCommand(statement), KeeperMap.strictMode)
    const succeeded = <A>(statement: Effect.Effect<A, SqlError>) =>
      statement.pipe(Effect.as(true), Effect.catchIf(KeeperMap.isConflict, () => Effect.succeed(false)))
    const ids = (values: Iterable<string>) => sql.literal(KeeperMap.int64List(values))
    const strings = (values: ReadonlyArray<string>) => sql.literal(KeeperMap.stringList(values))
    const cutoff = sql.literal(
      `now64(3) - INTERVAL ${Math.max(0, Duration.toMillis(Duration.fromInputUnsafe(options.retention)))} MILLISECOND`
    )
    const batchSize = Math.min(options.batchSize ?? idChunk, idChunk)

    const exits = (yield* sql`
      SELECT request_id, reply_id FROM ${sql(tables.exits)}
      WHERE completed_at < ${cutoff}
      LIMIT ${sql.literal(String(batchSize))}
    `.values).map(([requestId, replyId]) => [String(requestId), String(replyId)] as const)
    if (exits.length === 0) {
      return { archived: 0, compensated: 0 }
    }
    if (options.hooks?.afterRead) {
      yield* options.hooks.afterRead(exits)
    }
    const exitPairs = sql.literal(pairArray(exits, int64))

    // 1. Archive first, so the exit is always in Keeper, the archive or both.
    yield* command(sql`
      INSERT INTO ${sql(tables.exitsArchive)} (request_id, reply_id)
      SELECT tupleElement(pair, 1), tupleElement(pair, 2)
      FROM (SELECT arrayJoin(${exitPairs}) AS pair)
    `)
    // 2. Remove the Keeper entries that are unchanged and still old. A
    //    conflict means one of them changed; remove the others one by one.
    const removed = yield* succeeded(strict(sql`
      ALTER TABLE ${sql(tables.exits)}
      DELETE WHERE has(${exitPairs}, (request_id, reply_id)) AND completed_at < ${cutoff}
    `))
    if (!removed) {
      yield* Effect.forEach(exits, (pair) =>
        succeeded(strict(sql`
          ALTER TABLE ${sql(tables.exits)}
          DELETE WHERE has(${sql.literal(pairArray([pair], int64))}, (request_id, reply_id))
          AND completed_at < ${cutoff}
        `)), { discard: true })
    }
    // 3. Keep an archive row only where Keeper now holds nothing about the
    //    request: no exit and no pending request row.
    const requestIds = exits.map(([requestId]) => requestId)
    const live = new Set([
      ...(yield* sql`SELECT request_id FROM ${sql(tables.exits)} WHERE request_id IN ${ids(requestIds)}`.values),
      ...(yield* sql`SELECT id FROM ${sql(tables.pending)} WHERE id IN ${ids(requestIds)}`.values)
    ].map((row) => String(row[0])))
    const stale = exits.filter(([requestId]) => live.has(requestId))
    if (stale.length > 0) {
      yield* command(sql`
        DELETE FROM ${sql(tables.exitsArchive)}
        WHERE has(${sql.literal(pairArray(stale, int64))}, (request_id, reply_id))
      `)
    }
    const archived = exits.filter(([requestId]) => !live.has(requestId)).map(([requestId]) => requestId)
    if (archived.length === 0) {
      return { archived: 0, compensated: stale.length }
    }

    // The primary keys of the archived requests, where they still own them.
    const keyRows = yield* sql`
      SELECT message_id, request_id FROM ${sql(tables.messages)}
      WHERE request_id IN ${ids(archived)} AND id = request_id AND message_id IS NOT NULL
      LIMIT 1 BY id
    `.values
    const owned = yield* forChunks(keyRows, keyChunk, (chunk) =>
      Effect.map(
        sql`
          SELECT message_id, id FROM ${sql(tables.messageIds)}
          WHERE message_id IN ${strings(chunk.map((row) => String(row[0])))}
        `.values,
        (rows) => {
          const owners = new Map(rows.map(([messageId, id]) => [String(messageId), String(id)]))
          return chunk.flatMap((row) =>
            owners.get(String(row[0])) === String(row[1]) ? [[String(row[0]), String(row[1])] as const] : []
          )
        }
      ))
    let keysCompensated = 0
    yield* forChunks(owned, keyChunk, (chunk) =>
      Effect.gen(function*() {
        const keyPairs = sql.literal(pairArray(chunk, KeeperMap.stringLiteral))
        yield* command(sql`
          INSERT INTO ${sql(tables.messageIdsArchive)} (message_id, id)
          SELECT tupleElement(pair, 1), tupleElement(pair, 2)
          FROM (SELECT arrayJoin(${keyPairs}) AS pair)
        `)
        yield* succeeded(strict(sql`
          ALTER TABLE ${sql(tables.messageIds)}
          DELETE WHERE has(${keyPairs}, (message_id, id))
        `))
        const still = new Set(
          (yield* sql`
            SELECT message_id FROM ${sql(tables.messageIds)}
            WHERE message_id IN ${strings(chunk.map(([key]) => key))}
          `.values).map((row) => String(row[0]))
        )
        const back = chunk.filter(([key]) => still.has(key))
        if (back.length > 0) {
          keysCompensated += back.length
          yield* command(sql`
            DELETE FROM ${sql(tables.messageIdsArchive)}
            WHERE has(${sql.literal(pairArray(back, KeeperMap.stringLiteral))}, (message_id, id))
          `)
        }
        return []
      }))
    return { archived: archived.length, compensated: stale.length + keysCompensated }
  }).pipe((effect) => options.client.withClickhouseSettings(effect, KeeperMap.int64AsString))
