/**
 * Stores cluster runner registration and shard ownership in ClickHouse.
 *
 * The ClickHouse-backed `RunnerStorage` keeps runners, machine ids and shard
 * locks in `KeeperMap` tables, so every write is a ClickHouse Keeper
 * operation: a strict insert is an atomic insert-if-absent, and a strict
 * `ALTER TABLE ... UPDATE` or `DELETE` is a compare-and-set on the versions of
 * the rows it read. Shard locks are therefore exclusive across runners
 * without advisory locks or transactions.
 *
 * The server needs ClickHouse Keeper (embedded or external) and a
 * `keeper_map_path_prefix` in its configuration.
 *
 * `layer`, `layerWith`, and `make` run the migrations before building the
 * storage. To run migrations with a different connection, use
 * `layerMigrations` there and `layerStorage` in the runtime.
 *
 * @stability unstable
 * @since 4.0.0
 */
import { PersistenceError } from "effect/cluster/ClusterError"
import * as RunnerStorage from "effect/cluster/RunnerStorage"
import * as ShardingConfig from "effect/cluster/ShardingConfig"
import * as Duration from "effect/Duration"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"
import type * as Migrator from "effect/sql/Migrator"
import * as SqlClient from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import * as ClickhouseClient from "./ClickhouseClient.ts"
import * as ClickhouseMigrator from "./ClickhouseMigrator.ts"
import * as KeeperMap from "./internal/keeperMap.ts"

const withTracerDisabled = Effect.withTracerEnabled(false)

const makeStorage = Effect.fnUntraced(function*(options: {
  readonly prefix?: string | undefined
}) {
  const config = yield* ShardingConfig.ShardingConfig
  const client = yield* ClickhouseClient.ClickhouseClient
  const sql = client.withoutTransforms()
  const layerScope = yield* Effect.scope
  const prefix = options?.prefix ?? "cluster"
  const runnersTableSql = sql(`${prefix}_runners`)
  const machineIdsTableSql = sql(`${prefix}_machine_ids`)
  const locksTableSql = sql(`${prefix}_locks`)

  const lockOperationInterval = Duration.min(
    Duration.fromInputUnsafe(config.shardLockRefreshInterval),
    Duration.divideUnsafe(Duration.fromInputUnsafe(config.shardLockExpiration), 3)
  )
  const lockExpiresAt = sql.literal(
    `now64(3) - INTERVAL ${Duration.toMillis(Duration.fromInputUnsafe(config.shardLockExpiration))} MILLISECOND`
  )

  const command = <A>(statement: Effect.Effect<A, SqlError>) => client.asCommand(statement)
  const strict = <A>(statement: Effect.Effect<A, SqlError>) =>
    client.withClickhouseSettings(client.asCommand(statement), KeeperMap.strictMode)
  // Repeat a whole read-and-write step when a concurrent writer changed a row
  // it read: the predicates are evaluated again on the new versions.
  const retryOnConflict = <A, E extends SqlError>(effect: Effect.Effect<A, E>) =>
    Effect.retry(effect, { while: KeeperMap.isConflict, times: 10 })
  // Apply a batch as one Keeper multi-op, falling back to one shard at a time
  // when a concurrent writer conflicts with any of them, so a single contended
  // shard does not hold back the rest. A shard that still conflicts is left
  // to the next round.
  const perShardOnConflict = (
    shardIds: ReadonlyArray<string>,
    f: (shardIds: ReadonlyArray<string>) => Effect.Effect<unknown, SqlError>
  ) =>
    f(shardIds).pipe(
      Effect.catchIf(KeeperMap.isConflict, () =>
        Effect.forEach(
          shardIds,
          (shardId) => f([shardId]).pipe(Effect.catchIf(KeeperMap.isConflict, () => Effect.void)),
          { discard: true }
        ))
    )

  // `Effect.timeout` waits for the timed-out effect to finish interrupting,
  // and interrupting a query sends a `KILL QUERY` to a server that may be the
  // one not responding. Fork the operation and timeout the join instead,
  // leaving the cleanup to finish detached in the layer scope.
  const withDeadline = Effect.fnUntraced(function*<A, E, R>(operation: Effect.Effect<A, E, R>) {
    const fiber = yield* Effect.forkIn(operation, layerScope, { startImmediately: true })
    return yield* Fiber.join(fiber).pipe(
      Effect.timeout(lockOperationInterval),
      Effect.ensuring(Effect.suspend(() =>
        fiber.pollUnsafe() !== undefined ? Effect.void : Fiber.interrupt(fiber).pipe(
          Effect.forkIn(layerScope, { startImmediately: true }),
          Effect.asVoid
        )
      ))
    )
  })

  const inShards = (shardIds: ReadonlyArray<string>) => sql.literal(KeeperMap.stringList(shardIds))

  const inRequestOrder = (shardIds: ReadonlyArray<string>, rows: ReadonlyArray<ReadonlyArray<unknown>>) => {
    const held = new Set(rows.map((row) => String(row[0])))
    return shardIds.filter((shardId) => held.has(shardId))
  }

  const acquiredLocks = (address: string, shardIds: ReadonlyArray<string>) =>
    sql`
      SELECT shard_id FROM ${locksTableSql}
      WHERE shard_id IN ${inShards(shardIds)}
      AND address = ${address}
      AND acquired_at >= ${lockExpiresAt}
    `.values.pipe(Effect.map((rows) => inRequestOrder(shardIds, rows)))

  const insertLocks = (address: string) => (shardIds: ReadonlyArray<string>) =>
    strict(sql`
      INSERT INTO ${locksTableSql} (shard_id, address, acquired_at)
      SELECT arrayJoin(${shardIds}), ${address}, now64(3)
    `)

  // Refresh our own locks and take over expired ones in one compare-and-set.
  const takeLocks = (address: string) => (shardIds: ReadonlyArray<string>) =>
    strict(sql`
      ALTER TABLE ${locksTableSql}
      UPDATE address = ${address}, acquired_at = now64(3)
      WHERE shard_id IN ${inShards(shardIds)}
      AND (address = ${address} OR acquired_at < ${lockExpiresAt})
    `)

  const refreshLocks = (address: string) => (shardIds: ReadonlyArray<string>) =>
    strict(sql`
      ALTER TABLE ${locksTableSql}
      UPDATE acquired_at = now64(3)
      WHERE shard_id IN ${inShards(shardIds)}
      AND address = ${address}
    `)

  const acquire = Effect.fnUntraced(function*(address: string, shardIds: ReadonlyArray<string>) {
    const rows = yield* sql`SELECT shard_id FROM ${locksTableSql} WHERE shard_id IN ${inShards(shardIds)}`.values
    const existing = new Set(rows.map((row) => String(row[0])))
    const missing: Array<string> = []
    const present: Array<string> = []
    for (const shardId of shardIds) {
      ;(existing.has(shardId) ? present : missing).push(shardId)
    }
    if (missing.length > 0) {
      yield* perShardOnConflict(missing, insertLocks(address))
    }
    if (present.length > 0) {
      yield* perShardOnConflict(present, takeLocks(address))
    }
    return yield* acquiredLocks(address, shardIds)
  })

  const refresh = Effect.fnUntraced(function*(address: string, shardIds: ReadonlyArray<string>) {
    yield* perShardOnConflict(shardIds, refreshLocks(address))
    const rows = yield* sql`
      SELECT shard_id FROM ${locksTableSql}
      WHERE shard_id IN ${inShards(shardIds)}
      AND address = ${address}
    `.values
    return inRequestOrder(shardIds, rows)
  })

  const heartbeat = (address: string) =>
    retryOnConflict(strict(sql`
      ALTER TABLE ${runnersTableSql}
      UPDATE last_heartbeat = now64(3)
      WHERE address = ${address}
    `))

  const upsertRunner = (address: string, runner: string, healthy: boolean, machineId: number) =>
    sql`
      INSERT INTO ${runnersTableSql} (address, runner, healthy, last_heartbeat, machine_id)
      SELECT ${address}, ${runner}, ${healthy}, now64(3), ${client.param("UInt32", machineId)}
    `

  // Machine ids are allocated like a serial column: the next id after the
  // highest ever handed out, claimed with a strict insert so two runners
  // registering at once cannot get the same one.
  const allocateMachineId = (address: string) =>
    Effect.gen(function*() {
      const [[next]] = yield* sql`SELECT max(machine_id) + 1 FROM ${machineIdsTableSql}`.values
      const machineId = Number(next)
      yield* strict(sql`
        INSERT INTO ${machineIdsTableSql} (machine_id, address)
        SELECT ${client.param("UInt32", machineId)}, ${address}
      `)
      return machineId
    }).pipe(retryOnConflict)

  const register = (address: string, runner: string, healthy: boolean): Effect.Effect<number, SqlError> =>
    Effect.gen(function*() {
      const existing = yield* sql`SELECT machine_id FROM ${runnersTableSql} WHERE address = ${address}`.values
      if (existing.length > 0) {
        const machineId = Number(existing[0][0])
        yield* command(upsertRunner(address, runner, healthy, machineId))
        return machineId
      }
      const machineId = yield* allocateMachineId(address)
      return yield* strict(upsertRunner(address, runner, healthy, machineId)).pipe(
        Effect.as(machineId),
        // Another registration of the same address won: keep its machine id.
        Effect.catchIf(KeeperMap.isConflict, () => register(address, runner, healthy))
      )
    })

  return RunnerStorage.makeEncoded({
    getRunners: sql`SELECT runner, healthy FROM ${runnersTableSql} WHERE last_heartbeat > ${lockExpiresAt}`.values
      .pipe(
        Effect.map((rows) => rows.map(([runner, healthy]) => [String(runner), Boolean(healthy)] as const)),
        PersistenceError.refail,
        withTracerDisabled
      ),

    register: (address, runner, healthy) =>
      register(address, runner, healthy).pipe(
        PersistenceError.refail,
        withTracerDisabled
      ),

    unregister: (address) =>
      retryOnConflict(strict(sql`
        ALTER TABLE ${runnersTableSql}
        DELETE WHERE address = ${address} OR last_heartbeat < ${lockExpiresAt}
      `)).pipe(
        Effect.asVoid,
        PersistenceError.refail,
        withTracerDisabled
      ),

    setRunnerHealth: (address, healthy) =>
      retryOnConflict(strict(sql`
        ALTER TABLE ${runnersTableSql}
        UPDATE healthy = ${healthy}
        WHERE address = ${address}
      `)).pipe(
        Effect.asVoid,
        PersistenceError.refail,
        withTracerDisabled
      ),

    acquire: (address, shardIds) =>
      withDeadline(acquire(address, shardIds)).pipe(
        PersistenceError.refail,
        withTracerDisabled
      ),

    refresh: (address, shardIds) => {
      // An empty refresh is the liveness probe used while lock storage is
      // unhealthy.
      if (shardIds.length === 0) {
        return withDeadline(heartbeat(address)).pipe(
          Effect.as([]),
          PersistenceError.refail,
          withTracerDisabled
        )
      }
      return withDeadline(Effect.andThen(heartbeat(address), refresh(address, shardIds))).pipe(
        PersistenceError.refail,
        withTracerDisabled
      )
    },

    release: (address, shardId) =>
      withDeadline(retryOnConflict(strict(sql`
        ALTER TABLE ${locksTableSql}
        DELETE WHERE shard_id = ${shardId} AND address = ${address}
      `))).pipe(
        Effect.asVoid,
        PersistenceError.refail,
        withTracerDisabled
      ),

    releaseAll: (address) =>
      withDeadline(retryOnConflict(strict(sql`
        ALTER TABLE ${locksTableSql}
        DELETE WHERE address = ${address}
      `))).pipe(
        Effect.asVoid,
        PersistenceError.refail,
        withTracerDisabled
      )
  })
}, withTracerDisabled)

/**
 * Creates a ClickHouse-backed `RunnerStorage` implementation for registered
 * runners and shard locks, using the configured table prefix.
 *
 * **Details**
 *
 * `make` first runs the runner storage migrations, so the connection needs
 * permission to create tables. When `prefix` is omitted, `make` uses the
 * `cluster` prefix, creating the `KeeperMap` tables `cluster_runners`,
 * `cluster_machine_ids` and `cluster_locks`, and the
 * `cluster_runner_migrations` history table.
 *
 * **Gotchas**
 *
 * Changing `prefix` changes all generated table names, so runners using
 * different prefixes do not share registrations or shard locks.
 *
 * @see {@link layer} for the default ClickHouse-backed storage layer
 * @see {@link layerWith} for a storage layer with a custom table prefix
 * @see {@link layerStorage} for a storage layer that does not run migrations
 *
 * @stability unstable
 * @category constructors
 * @since 4.0.0
 */
export const make = (options: {
  readonly prefix?: string | undefined
}): Effect.Effect<
  RunnerStorage.RunnerStorage["Service"],
  never,
  ClickhouseClient.ClickhouseClient | ShardingConfig.ShardingConfig | Scope.Scope
> => Effect.andThen(runRunnerMigrations(options), makeStorage(options))

/**
 * Migration loader for the ClickHouse runner storage tables.
 *
 * **Details**
 *
 * History is recorded in `<prefix>_runner_migrations`; the default prefix is
 * `cluster`. The `KeeperMap` tables are stored in Keeper under
 * `<keeper_map_path_prefix>/effect_cluster/<database>/<table>`.
 *
 * @stability unstable
 * @category migrations
 * @since 4.0.0
 */
export const migrations = (options: {
  readonly prefix?: string | undefined
}): Migrator.Loader => {
  const prefix = options.prefix ?? "cluster"
  const table = (name: string) => `${prefix}_${name}`

  return ClickhouseMigrator.fromRecord({
    "0001_create_tables": Effect.gen(function*() {
      const sql = (yield* SqlClient.SqlClient).withoutTransforms()
      const keeperPath = yield* KeeperMap.keeperPath(sql)
      const create = (name: string, columns: string, key: string) =>
        KeeperMap.asCommand(sql`
          CREATE TABLE IF NOT EXISTS ${sql(table(name))} (${sql.literal(columns)})
          ENGINE = KeeperMap(${sql.literal(KeeperMap.stringLiteral(keeperPath(table(name))))})
          PRIMARY KEY ${sql(key)}
        `)

      yield* create(
        "runners",
        `address String, runner String, healthy Bool, last_heartbeat DateTime64(3), machine_id UInt32`,
        "address"
      )
      yield* create("machine_ids", `machine_id UInt32, address String`, "machine_id")
      yield* create("locks", `shard_id String, address String, acquired_at DateTime64(3)`, "shard_id")
    })
  })
}

const runRunnerMigrations = (options: {
  readonly prefix?: string | undefined
}): Effect.Effect<void, never, ClickhouseClient.ClickhouseClient> =>
  Effect.gen(function*() {
    const client = yield* ClickhouseClient.ClickhouseClient
    yield* ClickhouseMigrator.run({
      loader: migrations(options),
      // Message and runner migration ids overlap, so keep separate histories.
      table: `${options.prefix ?? "cluster"}_runner_migrations`
    }).pipe(Effect.provideService(SqlClient.SqlClient, client))
  }).pipe(Effect.orDie)

/**
 * Runs the ClickHouse runner storage migrations without providing storage.
 *
 * **Details**
 *
 * History is recorded in `<prefix>_runner_migrations`. Migration errors become
 * defects. This layer does not require `ShardingConfig`.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layerMigrations = (options: {
  readonly prefix?: string | undefined
}): Layer.Layer<never, never, ClickhouseClient.ClickhouseClient> => Layer.effectDiscard(runRunnerMigrations(options))

/**
 * Provides ClickHouse-backed `RunnerStorage` without DDL.
 *
 * **Details**
 *
 * Run `layerMigrations` separately before using this layer.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layerStorage = (options: {
  readonly prefix?: string | undefined
}): Layer.Layer<
  RunnerStorage.RunnerStorage,
  never,
  ClickhouseClient.ClickhouseClient | ShardingConfig.ShardingConfig
> => Layer.effect(RunnerStorage.RunnerStorage)(makeStorage(options))

/**
 * Provides ClickHouse-backed `RunnerStorage` with a custom table prefix,
 * running migrations first.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layerWith = (options: {
  readonly prefix?: string | undefined
}): Layer.Layer<
  RunnerStorage.RunnerStorage,
  never,
  ClickhouseClient.ClickhouseClient | ShardingConfig.ShardingConfig
> => Layer.effect(RunnerStorage.RunnerStorage)(make(options))

/**
 * Layer that provides ClickHouse-backed `RunnerStorage` using the default table
 * prefix, running the runner storage migrations first.
 *
 * @see {@link layerWith} for the same layer with a custom table prefix
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layer: Layer.Layer<
  RunnerStorage.RunnerStorage,
  never,
  ClickhouseClient.ClickhouseClient | ShardingConfig.ShardingConfig
> = layerWith({})
