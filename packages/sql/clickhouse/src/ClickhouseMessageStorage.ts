/**
 * Persists cluster mailbox messages and replies in ClickHouse.
 *
 * The ClickHouse-backed `MessageStorage` splits storage by access pattern.
 * Envelopes and replies are immutable facts, appended to `MergeTree` tables
 * whose `rowid` column (`generateSnowflakeID()`) records insertion order.
 * The state that changes or is contended lives in `KeeperMap` tables, where
 * strict inserts and strict `ALTER TABLE ... UPDATE / DELETE` statements are
 * atomic Keeper operations:
 *
 * - `<prefix>_pending` holds one row per unprocessed message, with its read
 *   claim and latest chunk reply. A message leaves it once processed, so the
 *   mailbox scan reads only messages still in flight.
 * - `<prefix>_message_ids` maps request primary keys to request ids; its
 *   strict insert deduplicates requests.
 * - `<prefix>_exits` records the exit reply of each completed request; its
 *   strict insert keeps a single exit per request, and `clearReplies` uses it
 *   as the compare-and-set anchor for `expectedReplyId`.
 *
 * ClickHouse has no transactions, so every operation writes the facts first
 * and lets the Keeper write decide: an exit reply counts only once `exits`
 * points at it, and a pending request whose primary key was never recorded
 * has its key settled by the reader before it is delivered. A crash between
 * two writes leaves at most an unreferenced fact behind. `withTransaction`
 * does not group writes.
 *
 * The server needs ClickHouse Keeper (embedded or external) and a
 * `keeper_map_path_prefix` in its configuration. `<prefix>_message_ids` and
 * `<prefix>_exits` grow with the number of requests until `clearAddress`
 * removes them.
 *
 * `layer`, `layerWith`, `make`, and `makeEncoded` run the migrations before
 * building the storage. To run migrations with a different connection, use
 * `layerMigrations` there and `layerStorage` in the runtime.
 *
 * @stability unstable
 * @since 4.0.0
 */
import { PersistenceError } from "effect/cluster/ClusterError"
import type * as EntityAddress from "effect/cluster/EntityAddress"
import type * as Envelope from "effect/cluster/Envelope"
import * as MessageStorage from "effect/cluster/MessageStorage"
import { SaveResultEncoded } from "effect/cluster/MessageStorage"
import type * as Reply from "effect/cluster/Reply"
import * as ShardId from "effect/cluster/ShardId"
import type { ShardingConfig } from "effect/cluster/ShardingConfig"
import * as Snowflake from "effect/cluster/Snowflake"
import * as Crypto from "effect/Crypto"
import * as Effect from "effect/Effect"
import * as Hex from "effect/encoding/Hex"
import * as Layer from "effect/Layer"
import * as Option from "effect/Option"
import type * as PlatformError from "effect/PlatformError"
import type * as Migrator from "effect/sql/Migrator"
import * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import type * as Statement from "effect/sql/Statement"
import * as NodeCrypto from "node:crypto"
import * as ClickhouseClient from "./ClickhouseClient.ts"
import * as ClickhouseMigrator from "./ClickhouseMigrator.ts"
import * as KeeperMap from "./internal/keeperMap.ts"

const withTracerDisabled = Effect.withTracerEnabled(false)

/**
 * Creates a ClickHouse-backed encoded message storage driver, running its
 * migrations and using the optional table prefix.
 *
 * @see {@link make} for the decoded `MessageStorage` constructor
 *
 * @stability unstable
 * @category constructors
 * @since 4.0.0
 */
export const makeEncoded: (options?: {
  readonly prefix?: string | undefined
}) => Effect.Effect<
  MessageStorage.Encoded,
  never,
  ClickhouseClient.ClickhouseClient | Crypto.Crypto
> = (options) => Effect.andThen(runMessageMigrations(options), makeEncodedStorage(options))

const makeEncodedStorage = Effect.fnUntraced(function*(
  options: {
    readonly prefix?: string | undefined
  } | undefined
) {
  const client = yield* ClickhouseClient.ClickhouseClient
  const sql = client.withoutTransforms()
  const crypto = yield* Crypto.Crypto
  const tables = tableNames(options?.prefix)
  const messagesTable = sql(tables.messages)
  const repliesTable = sql(tables.replies)
  const replyAcksTable = sql(tables.replyAcks)
  const messageIdsTable = sql(tables.messageIds)
  const pendingTable = sql(tables.pending)
  const exitsTable = sql(tables.exits)

  const command = <A>(statement: Effect.Effect<A, SqlError>) => client.asCommand(statement)
  const strict = <A>(statement: Effect.Effect<A, SqlError>) =>
    client.withClickhouseSettings(client.asCommand(statement), KeeperMap.strictMode)
  // Repeat a whole read-and-write step when a concurrent writer changed a row
  // it read: the predicates are evaluated again on the new versions.
  const retryOnConflict = <A>(effect: Effect.Effect<A, SqlError>) =>
    Effect.retry(effect, { while: KeeperMap.isConflict, times: 10 })
  const quoted = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    client.withClickhouseSettings(effect, KeeperMap.int64AsString)
  const succeeded = <A>(effect: Effect.Effect<A, SqlError>) =>
    effect.pipe(
      Effect.as(true),
      Effect.catchIf(KeeperMap.isConflict, () => Effect.succeed(false))
    )

  const ids = (values: Iterable<string>) => sql.literal(KeeperMap.int64List(values))
  const strings = (values: ReadonlyArray<string>) => sql.literal(KeeperMap.stringList(values))
  const int64 = (value: string) => client.param("Int64", value)
  const nullable = (type: string, value: unknown) => client.param(`Nullable(${type})`, value ?? null)
  const emptyFragment = sql.literal("")
  const unclaimed = sql.literal(`(last_read IS NULL OR last_read < now64(3) - INTERVAL 10 MINUTE)`)

  // The composed primary key can exceed 255 characters. Keys that fit are
  // stored as-is, matching the SQL storage; longer keys are stored as a
  // SHA-256 digest, which never contains "/" while composed keys always do.
  const encoder = new TextEncoder()
  const messageIdForPrimaryKey = (primaryKey: string): Effect.Effect<string, PlatformError.PlatformError> =>
    primaryKey.length <= 255
      ? Effect.succeed(primaryKey)
      : Effect.map(crypto.digest("SHA-256", encoder.encode(primaryKey)), Hex.encode)

  const insertMessage = (row: MessageRow) =>
    command(sql`
      INSERT INTO ${messagesTable} (
        id, message_id, shard_id, entity_type, entity_id, kind, tag, payload, headers,
        trace_id, span_id, sampled, request_id, reply_id, deliver_at
      )
      SELECT
        ${int64(row.id)}, ${nullable("String", row.message_id)}, ${row.shard_id}, ${row.entity_type},
        ${row.entity_id}, ${client.param("UInt8", row.kind)}, ${nullable("String", row.tag)},
        ${nullable("String", row.payload)}, ${nullable("String", row.headers)}, ${nullable("String", row.trace_id)},
        ${nullable("String", row.span_id)}, ${nullable("Bool", row.sampled)}, ${int64(row.request_id)},
        ${nullable("Int64", row.reply_id)}, ${nullable("Int64", row.deliver_at)}
    `)

  const insertReply = (row: ReplyRow) =>
    command(sql`
      INSERT INTO ${repliesTable} (id, kind, request_id, payload, sequence)
      SELECT ${int64(row.id)}, ${nullable("UInt8", row.kind)}, ${int64(row.request_id)}, ${row.payload},
        ${nullable("Int32", row.sequence)}
    `)

  // Pending rows are copied from the stored message, so they carry its rowid.
  const pendingColumns = sql.literal(
    `id, request_id, rowid, kind, message_id, shard_id, entity_type, entity_id, deliver_at, last_reply_id, last_read, claim`
  )
  const pendingFromMessage = sql.literal(
    `id, request_id, rowid, kind, message_id, shard_id, entity_type, entity_id, deliver_at, NULL, NULL, ''`
  )
  const enqueue = (row: MessageRow) =>
    command(sql`
      INSERT INTO ${pendingTable} (${pendingColumns})
      SELECT ${pendingFromMessage} FROM ${messagesTable}
      WHERE request_id = ${int64(row.request_id)} AND id = ${int64(row.id)}
      ORDER BY rowid DESC
      LIMIT 1
    `)
  const requeue = (requestId: string, withInterrupts: boolean) =>
    command(sql`
      INSERT INTO ${pendingTable} (${pendingColumns})
      SELECT ${pendingFromMessage} FROM ${messagesTable}
      WHERE request_id = ${int64(requestId)}
      ${withInterrupts ? emptyFragment : sql`AND kind != ${sql.literal(String(messageKind.Interrupt))}`}
      ORDER BY rowid DESC
      LIMIT 1 BY id
    `)
  const dequeue = (messageIds: ReadonlyArray<string>) =>
    retryOnConflict(strict(sql`ALTER TABLE ${pendingTable} DELETE WHERE id IN ${ids(messageIds)}`))

  const requestIdForMessageId = (messageId: string) =>
    sql`SELECT id FROM ${messageIdsTable} WHERE message_id = ${messageId}`.values.pipe(
      Effect.map((rows) => rows.length > 0 ? String(rows[0][0]) : undefined)
    )
  // Record `requestId` as the owner of a primary key unless another request
  // already owns it, returning the owner.
  const claimMessageId = (messageId: string, requestId: string) =>
    succeeded(strict(sql`
      INSERT INTO ${messageIdsTable} (message_id, id)
      SELECT ${messageId}, ${int64(requestId)}
    `)).pipe(
      Effect.flatMap((claimed) =>
        claimed
          ? Effect.succeed(requestId)
          : Effect.map(requestIdForMessageId(messageId), (owner) => owner ?? requestId)
      )
    )

  const exitFor = (requestId: string) =>
    sql`SELECT reply_id FROM ${exitsTable} WHERE request_id IN ${ids([requestId])}`.values.pipe(
      Effect.map((rows) => rows.length > 0 ? String(rows[0][0]) : undefined)
    )

  // The latest reply of a request: its exit once completed, otherwise the
  // latest chunk recorded on its pending row.
  const lastReplyFor = Effect.fnUntraced(function*(requestId: string) {
    let replyId = yield* exitFor(requestId)
    if (replyId === undefined) {
      const rows = yield* sql`SELECT last_reply_id FROM ${pendingTable} WHERE id IN ${ids([requestId])}`.values
      if (rows.length > 0 && rows[0][0] !== null) {
        replyId = String(rows[0][0])
      }
    }
    if (replyId === undefined) {
      return Option.none<Reply.Encoded>()
    }
    const replies = yield* sql<ReplyRow>`
      SELECT id, kind, request_id, payload, sequence FROM ${repliesTable}
      WHERE request_id = ${int64(requestId)} AND id = ${int64(replyId)}
      LIMIT 1
    `
    return replies.length > 0 ? Option.some(replyFromRow(replies[0])) : Option.none<Reply.Encoded>()
  })

  const duplicate = (originalId: string) =>
    Effect.map(lastReplyFor(originalId), (lastReceivedReply) =>
      SaveResultEncoded.Duplicate({
        originalId: Snowflake.Snowflake(originalId),
        lastReceivedReply
      }))

  const addressCondition = (addresses: ReadonlyArray<EntityAddress.EntityAddress>) =>
    sql.literal(
      groupAddresses(addresses).map((group) =>
        `(shard_id = ${KeeperMap.stringLiteral(group.shardId)} AND entity_type = ${
          KeeperMap.stringLiteral(group.entityType)
        } AND entity_id IN ${KeeperMap.stringList(group.addresses.map((address) => address.entityId))})`
      ).join(" OR ")
    )

  // Drops candidates that must not be delivered yet: requests that completed
  // (their leftover pending rows are removed), requests waiting for a chunk
  // reply to be acknowledged, and requests that lost their primary key to
  // another request with the same key.
  const deliverable = Effect.fnUntraced(function*(candidates: ReadonlyArray<PendingRow>) {
    if (candidates.length === 0) {
      return []
    }
    const requestIds = new Set(candidates.map((candidate) => candidate.request_id))
    const completed = new Set(
      (yield* sql`SELECT request_id FROM ${exitsTable} WHERE request_id IN ${ids(requestIds)}`.values)
        .map((row) => String(row[0]))
    )
    const waiting = new Set(
      (yield* sql`
        SELECT DISTINCT request_id FROM ${repliesTable}
        WHERE request_id IN ${ids(requestIds)}
        AND kind IS NULL
        AND id NOT IN (SELECT reply_id FROM ${replyAcksTable} WHERE request_id IN ${ids(requestIds)})
      `.values).map((row) => String(row[0]))
    )
    const dropped: Array<string> = []
    const ready: Array<PendingRow> = []
    const owners = new Map<string, string>()
    const keyed = candidates.filter((candidate) =>
      candidate.message_id !== null && !completed.has(candidate.request_id)
    )
    if (keyed.length > 0) {
      const rows = yield* sql`
        SELECT message_id, id FROM ${messageIdsTable}
        WHERE message_id IN ${strings(keyed.map((candidate) => candidate.message_id!))}
      `.values
      for (const [messageId, id] of rows) {
        owners.set(String(messageId), String(id))
      }
    }
    for (const candidate of candidates) {
      if (completed.has(candidate.request_id)) {
        dropped.push(candidate.id)
        continue
      }
      if (candidate.message_id !== null) {
        let owner = owners.get(candidate.message_id)
        if (owner === undefined) {
          // The save that enqueued this request has not recorded its key yet,
          // or stopped before it did.
          owner = yield* claimMessageId(candidate.message_id, candidate.id)
          owners.set(candidate.message_id, owner)
        }
        if (owner !== candidate.id) {
          dropped.push(candidate.id)
          continue
        }
      }
      if (!waiting.has(candidate.request_id)) {
        ready.push(candidate)
      }
    }
    if (dropped.length > 0) {
      yield* dequeue(dropped)
    }
    return ready
  })

  const envelopesFor = Effect.fnUntraced(function*(rows: ReadonlyArray<PendingRow>) {
    if (rows.length === 0) {
      return []
    }
    const requestIds = new Set(rows.map((row) => row.request_id))
    const messages = yield* sql<MessageRow>`
      SELECT * FROM ${messagesTable}
      WHERE request_id IN ${ids(requestIds)} AND id IN ${ids(rows.map((row) => row.id))}
      ORDER BY rowid ASC
      LIMIT 1 BY id
    `
    const lastReplyIds = rows.flatMap((row) =>
      row.kind === messageKind.Request && row.last_reply_id !== null ? [row.last_reply_id] : []
    )
    const replies = new Map<string, ReplyRow>()
    if (lastReplyIds.length > 0) {
      const replyRows = yield* sql<ReplyRow>`
        SELECT id, kind, request_id, payload, sequence FROM ${repliesTable}
        WHERE request_id IN ${ids(requestIds)} AND id IN ${ids(lastReplyIds)}
        LIMIT 1 BY id
      `
      for (const reply of replyRows) {
        replies.set(String(reply.id), reply)
      }
    }
    const lastReply = new Map(
      rows.map((row) => [row.id, row.last_reply_id === null ? undefined : replies.get(row.last_reply_id)])
    )
    return messages.map((message) => messageFromRow(message, lastReply.get(String(message.id))))
  })

  const pendingRows = (filter: Statement.Fragment) =>
    sql<PendingRow>`
      SELECT id, request_id, kind, message_id, last_reply_id FROM ${pendingTable}
      WHERE ${filter}
      ORDER BY rowid ASC
    `.pipe(Effect.map((rows) =>
      rows.map((row) => ({
        id: String(row.id),
        request_id: String(row.request_id),
        kind: Number(row.kind),
        message_id: row.message_id,
        last_reply_id: row.last_reply_id === null ? null : String(row.last_reply_id)
      }))
    ))

  const deliverableAt = (now: number) => sql.literal(`(deliver_at IS NULL OR deliver_at <= ${Math.floor(now)})`)

  const claim = Effect.fnUntraced(function*(
    shardIds: ReadonlyArray<string>,
    now: number,
    options?: {
      readonly limit?: number | undefined
      readonly addresses?: ReadonlyArray<EntityAddress.EntityAddress> | undefined
    } | undefined
  ) {
    const addressFilter = options?.addresses !== undefined
      ? sql`AND (${addressCondition(options.addresses)})`
      : emptyFragment
    for (let attempt = 0; attempt < 10; attempt++) {
      const candidates = yield* pendingRows(sql`
        shard_id IN ${strings(shardIds)}
        ${addressFilter}
        AND ${unclaimed}
        AND ${deliverableAt(now)}
      `)
      let ready = yield* deliverable(candidates)
      if (options?.limit !== undefined) {
        ready = ready.slice(0, Math.floor(options.limit))
      }
      if (ready.length === 0) {
        return []
      }
      // One compare-and-set over the candidates still unclaimed; a conflict
      // means another reader got there first, so read again.
      const token = NodeCrypto.randomUUID()
      const readyIds = ready.map((row) => row.id)
      const updated = yield* succeeded(strict(sql`
        ALTER TABLE ${pendingTable}
        UPDATE last_read = now64(3), claim = ${token}
        WHERE id IN ${ids(readyIds)} AND ${unclaimed}
      `))
      if (!updated) {
        continue
      }
      const mine = new Set(
        (yield* sql`SELECT id FROM ${pendingTable} WHERE id IN ${ids(readyIds)} AND claim = ${token}`.values)
          .map((row) => String(row[0]))
      )
      return yield* envelopesFor(ready.filter((row) => mine.has(row.id)))
    }
    return []
  })

  const resetClaims = (filter: Statement.Fragment) =>
    retryOnConflict(strict(sql`
      ALTER TABLE ${pendingTable}
      UPDATE last_read = NULL, claim = ''
      WHERE ${filter}
    `))

  const encoded: MessageStorage.Encoded = {
    saveEnvelope: ({ deliverAt, envelope, primaryKey }) =>
      Effect.gen(function*() {
        if (primaryKey === null) {
          const row = envelopeToRow(envelope, null, deliverAt)
          if (envelope._tag === "AckChunk") {
            yield* command(sql`
              INSERT INTO ${replyAcksTable} (reply_id, request_id)
              SELECT ${int64(envelope.replyId)}, ${int64(envelope.requestId)}
            `)
            // Only the latest acknowledgement of a request stays unprocessed.
            yield* retryOnConflict(strict(sql`
              ALTER TABLE ${pendingTable}
              DELETE WHERE request_id = ${int64(envelope.requestId)}
              AND kind = ${sql.literal(String(messageKind.AckChunk))}
            `))
          }
          yield* insertMessage(row)
          yield* enqueue(row)
          return SaveResultEncoded.Success()
        }

        const messageId = yield* messageIdForPrimaryKey(primaryKey)
        const existing = yield* requestIdForMessageId(messageId)
        if (existing !== undefined) {
          return yield* duplicate(existing)
        }
        const row = envelopeToRow(envelope, messageId, deliverAt)
        yield* insertMessage(row)
        yield* enqueue(row)
        const owner = yield* claimMessageId(messageId, row.id)
        if (owner === row.id) {
          return SaveResultEncoded.Success()
        }
        yield* dequeue([row.id])
        return yield* duplicate(owner)
      }).pipe(
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    saveReply: (reply) =>
      Effect.gen(function*() {
        const row = replyToRow(reply)
        const requestId = String(reply.requestId)
        if (reply._tag === "Chunk") {
          yield* insertReply(row)
          yield* retryOnConflict(strict(sql`
            ALTER TABLE ${pendingTable}
            UPDATE last_reply_id = ${int64(row.id)}
            WHERE id IN ${ids([requestId])}
          `))
          return
        }
        const existing = yield* exitFor(requestId)
        if (existing === row.id) {
          return
        }
        if (existing !== undefined) {
          return yield* Effect.fail(new ExitAlreadySaved(requestId))
        }
        // The reply row counts only once `exits` points at it.
        yield* insertReply(row)
        const saved = yield* succeeded(strict(sql`
          INSERT INTO ${exitsTable} (request_id, reply_id)
          SELECT ${int64(requestId)}, ${int64(row.id)}
        `))
        if (!saved && (yield* exitFor(requestId)) !== row.id) {
          return yield* Effect.fail(new ExitAlreadySaved(requestId))
        }
        yield* retryOnConflict(strict(sql`ALTER TABLE ${pendingTable} DELETE WHERE request_id = ${int64(requestId)}`))
      }).pipe(
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    clearReplies: Effect.fnUntraced(
      function*(requestId, options) {
        const id = String(requestId)
        const expected = options?.expectedReplyId === undefined ? undefined : String(options.expectedReplyId)
        if (expected === undefined) {
          yield* retryOnConflict(strict(sql`ALTER TABLE ${exitsTable} DELETE WHERE request_id IN ${ids([id])}`))
          yield* command(sql`
            DELETE FROM ${repliesTable}
            WHERE request_id = ${int64(id)} AND kind = ${sql.literal(String(replyKind.WithExit))}
          `)
          yield* retryOnConflict(strict(sql`
            ALTER TABLE ${pendingTable}
            DELETE WHERE request_id = ${int64(id)} AND kind = ${sql.literal(String(messageKind.Interrupt))}
          `))
          yield* command(sql`
            DELETE FROM ${messagesTable}
            WHERE request_id = ${int64(id)} AND kind = ${sql.literal(String(messageKind.Interrupt))}
          `)
          yield* requeue(id, false)
          return
        }

        // Compare with the latest reply at the storage boundary.
        const exit = yield* exitFor(id)
        if (exit !== undefined) {
          if (exit !== expected) {
            return
          }
          yield* succeeded(strict(sql`
            ALTER TABLE ${exitsTable}
            DELETE WHERE request_id IN ${ids([id])} AND reply_id = ${int64(expected)}
          `))
          if ((yield* exitFor(id)) !== undefined) {
            return
          }
          yield* command(sql`DELETE FROM ${repliesTable} WHERE request_id = ${int64(id)} AND id = ${int64(expected)}`)
          yield* requeue(id, true)
          return
        }
        yield* retryOnConflict(strict(sql`
          ALTER TABLE ${pendingTable}
          UPDATE last_reply_id = NULL, last_read = NULL, claim = ''
          WHERE id IN ${ids([id])} AND last_reply_id = ${int64(expected)}
        `))
      },
      quoted,
      PersistenceError.refail,
      withTracerDisabled
    ),

    requestIdForPrimaryKey: (primaryKey) =>
      messageIdForPrimaryKey(primaryKey).pipe(
        Effect.flatMap(requestIdForMessageId),
        Effect.map((id) => Option.map(Option.fromNullishOr(id), Snowflake.Snowflake)),
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    // Exit replies count only when `exits` points at them; chunk replies are
    // returned until they are acknowledged.
    repliesFor: (requestIds) =>
      sql<ReplyRow>`
        SELECT id, kind, request_id, payload, sequence FROM ${repliesTable}
        WHERE request_id IN ${ids(requestIds)}
        AND (
          (kind = ${sql.literal(String(replyKind.WithExit))}
            AND id IN (SELECT reply_id FROM ${exitsTable} WHERE request_id IN ${ids(requestIds)}))
          OR (kind IS NULL
            AND id NOT IN (SELECT reply_id FROM ${replyAcksTable} WHERE request_id IN ${ids(requestIds)}))
        )
        ORDER BY rowid ASC
        LIMIT 1 BY id
      `.pipe(
        Effect.map((rows) => rows.map(replyFromRow)),
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    repliesForUnfiltered: (requestIds) =>
      sql<ReplyRow>`
        SELECT id, kind, request_id, payload, sequence FROM ${repliesTable}
        WHERE request_id IN ${ids(requestIds)}
        AND (
          (kind = ${sql.literal(String(replyKind.WithExit))}
            AND id IN (SELECT reply_id FROM ${exitsTable} WHERE request_id IN ${ids(requestIds)}))
          OR kind IS NULL
        )
        ORDER BY rowid ASC
        LIMIT 1 BY id
      `.pipe(
        Effect.map((rows) => rows.map(replyFromRow)),
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    unprocessedMessages: (shardIds, now, options) =>
      (options?.addresses?.length === 0 ? Effect.succeed([]) : claim(shardIds, now, options)).pipe(
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    unprocessedMessagesById: (messageIds, now) =>
      pendingRows(sql`id IN ${ids(Array.from(messageIds, String))} AND ${deliverableAt(now)}`).pipe(
        Effect.flatMap(deliverable),
        Effect.flatMap(envelopesFor),
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    resetRequests: (requestIds) =>
      requestIds.length === 0
        ? Effect.void
        : resetClaims(sql`id IN ${ids(requestIds.map(String))}`).pipe(
          Effect.asVoid,
          quoted,
          PersistenceError.refail,
          withTracerDisabled
        ),

    resetAddresses: (addresses) =>
      addresses.length === 0
        ? Effect.void
        : resetClaims(sql`${addressCondition(addresses)}`).pipe(
          Effect.asVoid,
          quoted,
          PersistenceError.refail,
          withTracerDisabled
        ),

    clearAddress: (address) =>
      Effect.gen(function*() {
        const rows = yield* sql`
          SELECT DISTINCT request_id, message_id FROM ${messagesTable}
          WHERE entity_type = ${address.entityType} AND entity_id = ${address.entityId}
        `.values
        const requestIds = Array.from(new Set(rows.map((row) => String(row[0]))))
        const messageIds = rows.flatMap((row) => row[1] === null ? [] : [String(row[1])])
        yield* retryOnConflict(strict(sql`
          ALTER TABLE ${pendingTable}
          DELETE WHERE entity_type = ${address.entityType} AND entity_id = ${address.entityId}
        `))
        if (requestIds.length > 0) {
          yield* retryOnConflict(strict(sql`ALTER TABLE ${exitsTable} DELETE WHERE request_id IN ${ids(requestIds)}`))
          if (messageIds.length > 0) {
            yield* retryOnConflict(strict(sql`
              ALTER TABLE ${messageIdsTable}
              DELETE WHERE message_id IN ${strings(messageIds)} AND id IN ${ids(requestIds)}
            `))
          }
          yield* command(sql`DELETE FROM ${repliesTable} WHERE request_id IN ${ids(requestIds)}`)
          yield* command(sql`DELETE FROM ${replyAcksTable} WHERE request_id IN ${ids(requestIds)}`)
        }
        yield* command(sql`
          DELETE FROM ${messagesTable}
          WHERE entity_type = ${address.entityType} AND entity_id = ${address.entityId}
        `)
      }).pipe(
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    resetShards: (shardIds) =>
      resetClaims(sql`shard_id IN ${strings(shardIds)}`).pipe(
        Effect.asVoid,
        quoted,
        PersistenceError.refail,
        withTracerDisabled
      ),

    // ClickHouse has no transactions: each operation stands on its own.
    withTransaction: (effect) => effect
  }
  return encoded
}, withTracerDisabled)

/**
 * Creates a ClickHouse-backed `MessageStorage` implementation, running its
 * migrations and using the optional table prefix.
 *
 * @stability unstable
 * @category constructors
 * @since 4.0.0
 */
export const make: (options?: {
  readonly prefix?: string | undefined
}) => Effect.Effect<
  MessageStorage.MessageStorage["Service"],
  never,
  ClickhouseClient.ClickhouseClient | Snowflake.Generator | Crypto.Crypto
> = (options) => Effect.flatMap(makeEncoded(options), MessageStorage.makeEncoded)

/**
 * Migration loader for the ClickHouse message storage tables.
 *
 * **Details**
 *
 * History is recorded in `<prefix>_migrations`; the default prefix is
 * `cluster`. The `KeeperMap` tables are stored in Keeper under
 * `<keeper_map_path_prefix>/effect_cluster/<database>/<table>`.
 *
 * @stability unstable
 * @category migrations
 * @since 4.0.0
 */
export const migrations = (options?: {
  readonly prefix?: string | undefined
}): Migrator.Loader => {
  const tables = tableNames(options?.prefix)

  return ClickhouseMigrator.fromRecord({
    "0001_create_tables": Effect.gen(function*() {
      const sql = (yield* SqlClient.SqlClient).withoutTransforms()
      const keeperPath = yield* KeeperMap.keeperPath(sql)
      const mergeTree = (table: string, columns: string, orderBy: string) =>
        KeeperMap.asCommand(sql`
          CREATE TABLE IF NOT EXISTS ${sql(table)} (${sql.literal(columns)})
          ENGINE = MergeTree
          ORDER BY (${sql.literal(orderBy)})
        `)
      const keeperMap = (table: string, columns: string, key: string) =>
        KeeperMap.asCommand(sql`
          CREATE TABLE IF NOT EXISTS ${sql(table)} (${sql.literal(columns)})
          ENGINE = KeeperMap(${sql.literal(KeeperMap.stringLiteral(keeperPath(table)))})
          PRIMARY KEY ${sql(key)}
        `)

      yield* mergeTree(
        tables.messages,
        `id Int64,
        rowid UInt64 DEFAULT generateSnowflakeID(),
        message_id Nullable(String),
        shard_id String,
        entity_type String,
        entity_id String,
        kind UInt8,
        tag Nullable(String),
        payload Nullable(String),
        headers Nullable(String),
        trace_id Nullable(String),
        span_id Nullable(String),
        sampled Nullable(Bool),
        request_id Int64,
        reply_id Nullable(Int64),
        deliver_at Nullable(Int64),
        INDEX entity_idx (entity_type, entity_id) TYPE bloom_filter GRANULARITY 1`,
        "request_id, id"
      )
      yield* mergeTree(
        tables.replies,
        `id Int64,
        rowid UInt64 DEFAULT generateSnowflakeID(),
        kind Nullable(UInt8),
        request_id Int64,
        payload String,
        sequence Nullable(Int32)`,
        "request_id, rowid"
      )
      yield* mergeTree(tables.replyAcks, `reply_id Int64, request_id Int64`, "request_id, reply_id")
      yield* keeperMap(tables.messageIds, `message_id String, id Int64`, "message_id")
      yield* keeperMap(
        tables.pending,
        `id Int64,
        request_id Int64,
        rowid UInt64,
        kind UInt8,
        message_id Nullable(String),
        shard_id String,
        entity_type String,
        entity_id String,
        deliver_at Nullable(Int64),
        last_reply_id Nullable(Int64),
        last_read Nullable(DateTime64(3)),
        claim String`,
        "id"
      )
      yield* keeperMap(tables.exits, `request_id Int64, reply_id Int64`, "request_id")
    })
  })
}

const runMessageMigrations = (options?: {
  readonly prefix?: string | undefined
}): Effect.Effect<void, never, ClickhouseClient.ClickhouseClient> =>
  Effect.gen(function*() {
    const client = yield* ClickhouseClient.ClickhouseClient
    yield* ClickhouseMigrator.run({
      loader: migrations(options),
      table: `${options?.prefix ?? "cluster"}_migrations`
    }).pipe(Effect.provideService(SqlClient.SqlClient, client))
  }).pipe(Effect.orDie)

/**
 * Runs the ClickHouse message storage migrations without providing storage.
 *
 * **Details**
 *
 * History is recorded in `<prefix>_migrations`. Migration errors become
 * defects.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layerMigrations = (options: {
  readonly prefix?: string | undefined
}): Layer.Layer<never, never, ClickhouseClient.ClickhouseClient> => Layer.effectDiscard(runMessageMigrations(options))

/**
 * Provides ClickHouse-backed `MessageStorage` without DDL.
 *
 * **Details**
 *
 * Run `layerMigrations` separately before using this layer. This layer
 * supplies `Snowflake.layerGenerator` internally.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layerStorage = (options: {
  readonly prefix?: string | undefined
}): Layer.Layer<
  MessageStorage.MessageStorage,
  never,
  ClickhouseClient.ClickhouseClient | ShardingConfig | Crypto.Crypto
> =>
  Layer.effect(
    MessageStorage.MessageStorage,
    Effect.flatMap(makeEncodedStorage(options), MessageStorage.makeEncoded)
  ).pipe(
    Layer.provide(Snowflake.layerGenerator)
  )

/**
 * Provides ClickHouse-backed `MessageStorage` with a custom table prefix,
 * running migrations first.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layerWith = (options: {
  readonly prefix?: string | undefined
}): Layer.Layer<
  MessageStorage.MessageStorage,
  never,
  ClickhouseClient.ClickhouseClient | ShardingConfig | Crypto.Crypto
> =>
  layerStorage(options).pipe(
    Layer.provide(layerMigrations(options))
  )

/**
 * Provides ClickHouse-backed `MessageStorage` with the `cluster` table prefix,
 * running migrations first and supplying `Snowflake.layerGenerator`.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layer: Layer.Layer<
  MessageStorage.MessageStorage,
  never,
  ClickhouseClient.ClickhouseClient | ShardingConfig | Crypto.Crypto
> = layerWith({})

// -------------------------------------------------------------------------------------------------
// internal
// -------------------------------------------------------------------------------------------------

const tableNames = (prefix: string | undefined) => {
  const table = (name: string) => `${prefix ?? "cluster"}_${name}`
  return {
    messages: table("messages"),
    replies: table("replies"),
    replyAcks: table("reply_acks"),
    messageIds: table("message_ids"),
    pending: table("pending"),
    exits: table("exits")
  }
}

class ExitAlreadySaved extends Error {
  constructor(requestId: string) {
    super(`Request ${requestId} already has an exit reply`)
  }
}

const messageKind = {
  "Request": 0,
  "AckChunk": 1,
  "Interrupt": 2
} as const satisfies Record<Envelope.Envelope.Any["_tag"], number>

const replyKind = {
  "WithExit": 0,
  "Chunk": null
} as const satisfies Record<Reply.Reply<any>["_tag"], number | null>

const groupAddresses = (addresses: ReadonlyArray<EntityAddress.EntityAddress>) => {
  const byShard = new Map<string, Map<string, Array<EntityAddress.EntityAddress>>>()
  for (const address of addresses) {
    const shardId = address.shardId.toString()
    let byEntityType = byShard.get(shardId)
    if (byEntityType === undefined) {
      byShard.set(shardId, byEntityType = new Map())
    }
    const group = byEntityType.get(address.entityType)
    if (group === undefined) {
      byEntityType.set(address.entityType, [address])
    } else {
      group.push(address)
    }
  }
  return Array.from(
    byShard,
    ([shardId, byEntityType]) =>
      Array.from(byEntityType, ([entityType, addresses]) => ({ shardId, entityType, addresses }))
  ).flat()
}

const envelopeToRow = (
  envelope: Envelope.Encoded,
  message_id: string | null,
  deliver_at: number | null
): MessageRow => {
  const address = {
    shard_id: ShardId.toString(envelope.address.shardId),
    entity_type: envelope.address.entityType,
    entity_id: envelope.address.entityId
  }
  switch (envelope._tag) {
    case "Request":
      return {
        id: String(envelope.requestId),
        message_id,
        ...address,
        kind: messageKind.Request,
        tag: envelope.tag,
        payload: JSON.stringify(envelope.payload),
        headers: JSON.stringify(envelope.headers),
        trace_id: envelope.traceId ?? null,
        span_id: envelope.spanId ?? null,
        sampled: envelope.sampled ?? null,
        request_id: String(envelope.requestId),
        reply_id: null,
        deliver_at
      }
    case "AckChunk":
      return {
        id: String(envelope.id),
        message_id,
        ...address,
        kind: messageKind.AckChunk,
        tag: null,
        payload: null,
        headers: null,
        trace_id: null,
        span_id: null,
        sampled: null,
        request_id: String(envelope.requestId),
        reply_id: String(envelope.replyId),
        deliver_at
      }
    case "Interrupt":
      return {
        id: String(envelope.id),
        message_id,
        ...address,
        kind: messageKind.Interrupt,
        tag: null,
        payload: null,
        headers: null,
        trace_id: null,
        span_id: null,
        sampled: null,
        request_id: String(envelope.requestId),
        reply_id: null,
        deliver_at
      }
  }
}

const replyToRow = (reply: Reply.Encoded): ReplyRow => ({
  id: String(reply.id),
  kind: replyKind[reply._tag],
  request_id: String(reply.requestId),
  payload: reply._tag === "WithExit" ? JSON.stringify(reply.exit) : JSON.stringify(reply.values),
  sequence: reply._tag === "Chunk" ? reply.sequence : null
})

const replyFromRow = (row: ReplyRow): Reply.Encoded =>
  row.kind !== null && Number(row.kind) === replyKind.WithExit ?
    {
      _tag: "WithExit",
      id: String(row.id),
      requestId: String(row.request_id),
      exit: JSON.parse(row.payload)
    } :
    {
      _tag: "Chunk",
      id: String(row.id),
      requestId: String(row.request_id),
      values: JSON.parse(row.payload),
      sequence: Number(row.sequence!)
    }

const messageFromRow = (row: MessageRow, lastReply: ReplyRow | undefined): {
  readonly envelope: Envelope.Encoded
  readonly lastSentReply: Option.Option<Reply.Encoded>
} => {
  const address = {
    shardId: ShardId.fromStringEncoded(row.shard_id),
    entityType: row.entity_type,
    entityId: row.entity_id
  }
  switch (Number(row.kind) as 0 | 1 | 2) {
    case 0:
      return {
        envelope: {
          _tag: "Request",
          requestId: String(row.id),
          address,
          tag: row.tag!,
          payload: JSON.parse(row.payload!),
          headers: JSON.parse(row.headers!),
          ...(row.trace_id ?
            ({
              traceId: row.trace_id,
              spanId: row.span_id!,
              sampled: !!row.sampled
            }) :
            undefined)
        },
        lastSentReply: lastReply ?
          Option.some({
            _tag: "Chunk",
            id: String(lastReply.id),
            requestId: String(lastReply.request_id),
            sequence: Number(lastReply.sequence!),
            values: JSON.parse(lastReply.payload)
          }) :
          Option.none()
      }
    case 1:
      return {
        envelope: {
          _tag: "AckChunk",
          id: String(row.id),
          requestId: String(row.request_id),
          replyId: String(row.reply_id!),
          address
        },
        lastSentReply: Option.none()
      }
    case 2:
      return {
        envelope: {
          _tag: "Interrupt",
          id: String(row.id),
          requestId: String(row.request_id),
          address
        },
        lastSentReply: Option.none()
      }
  }
}

// Snowflake ids are signed 64-bit integers and arrive as decimal strings.
type MessageRow = {
  readonly id: string
  readonly message_id: string | null
  readonly shard_id: string
  readonly entity_type: string
  readonly entity_id: string
  readonly kind: number
  readonly tag: string | null
  readonly payload: string | null
  readonly headers: string | null
  readonly trace_id: string | null
  readonly span_id: string | null
  readonly sampled: boolean | null
  readonly request_id: string
  readonly reply_id: string | null
  readonly deliver_at: number | string | null
}

type ReplyRow = {
  readonly id: string
  readonly kind: number | null
  readonly request_id: string
  readonly payload: string
  readonly sequence: number | null
}

type PendingRow = {
  readonly id: string
  readonly request_id: string
  readonly kind: number
  readonly message_id: string | null
  readonly last_reply_id: string | null
}
