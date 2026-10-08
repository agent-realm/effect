/**
 * ClickHouse adapter for the shared Effect SQL migration runner.
 *
 * This module re-exports the common `Migrator` loaders and error types, then
 * provides `run` and `layer` helpers that apply ordered migrations through the
 * current ClickHouse `SqlClient`. `run` returns the applied migration IDs and
 * names, while `layer` runs the migrations during layer construction and
 * provides no services.
 *
 * ClickHouse has no transactions, so `run` records each migration in the
 * history table after it succeeds instead of running the batch in a
 * transaction. A migration that fails is not recorded and runs again next
 * time. Two migrators racing on the same history table can both apply a
 * migration, so ClickHouse migrations should be idempotent (`CREATE TABLE IF
 * NOT EXISTS` and friends).
 *
 * @stability unstable
 * @since 4.0.0
 */
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Migrator from "effect/sql/Migrator"
import * as Client from "effect/sql/SqlClient"
import type { SqlError } from "effect/sql/SqlError"
import { ClientMethod } from "./ClickhouseClient.ts"

/**
 * @since 4.0.0
 */
export * from "effect/sql/Migrator"

const asCommand = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.provideService(effect, ClientMethod, "command")

const loadMigration = ([id, name, load]: Migrator.ResolvedMigration) =>
  Effect.catchDefect(load, (defect) =>
    Effect.fail(
      new Migrator.MigrationError({
        kind: "ImportError",
        message: `Could not import migration "${id}_${name}"\n\n${defect}`
      })
    )).pipe(
      Effect.flatMap((loaded) =>
        Effect.isEffect(loaded)
          ? Effect.succeed(loaded)
          : loaded.default
          ? Effect.succeed(loaded.default?.default ?? loaded.default)
          : Effect.fail(
            new Migrator.MigrationError({
              kind: "ImportError",
              message: `Default export not found for migration "${id}_${name}"`
            })
          )
      ),
      Effect.filterOrFail(
        (loaded): loaded is Effect.Effect<unknown, unknown, Client.SqlClient> => Effect.isEffect(loaded),
        () =>
          new Migrator.MigrationError({
            kind: "ImportError",
            message: `Default export was not an Effect for migration "${id}_${name}"`
          })
      )
    )

/**
 * Runs SQL migrations for ClickHouse using the supplied migrator options and
 * returns the applied migration IDs and names.
 *
 * **Details**
 *
 * The history table is a `MergeTree` table named by `table` (default
 * `effect_sql_migrations`). Each migration is recorded after it succeeds.
 * Failures inside a migration become `MigrationError` defects of kind
 * `"Failed"`, as with the shared migrator.
 *
 * @stability unstable
 * @category running
 * @since 4.0.0
 */
export const run = <R2 = never>(
  { loader, table = "effect_sql_migrations" }: Migrator.MigratorOptions<R2>
): Effect.Effect<
  ReadonlyArray<readonly [id: number, name: string]>,
  Migrator.MigrationError | SqlError,
  Client.SqlClient | R2
> =>
  Effect.gen(function*() {
    const sql = (yield* Client.SqlClient).withoutTransforms()
    const tableSql = sql(table)

    yield* asCommand(sql`
      CREATE TABLE IF NOT EXISTS ${tableSql} (
        migration_id UInt32,
        created_at DateTime64(3) DEFAULT now64(3),
        name String
      )
      ENGINE = MergeTree
      ORDER BY migration_id
    `)

    const [[latestMigrationId], current] = yield* Effect.all([
      Effect.map(
        sql`SELECT max(migration_id) FROM ${tableSql}`.values,
        (rows) => rows.map((row) => Number(row[0]))
      ),
      loader
    ])

    if (new Set(current.map(([id]) => id)).size !== current.length) {
      return yield* new Migrator.MigrationError({
        kind: "Duplicates",
        message: "Found duplicate migration id's"
      })
    }

    const applied: Array<readonly [id: number, name: string]> = []
    for (const resolved of current) {
      const [id, name] = resolved
      if (id <= latestMigrationId) {
        continue
      }
      const effect = yield* loadMigration(resolved)
      yield* Effect.logDebug(`Running migration`).pipe(
        Effect.andThen(Effect.catch(effect, (error: unknown) =>
          Effect.die(
            new Migrator.MigrationError({
              cause: error,
              kind: "Failed",
              message: `Migration "${id}_${name}" failed`
            })
          ))),
        Effect.annotateLogs("migration_id", String(id)),
        Effect.annotateLogs("migration_name", name),
        Effect.withSpan(`Migrator ${id}_${name}`)
      )
      yield* asCommand(sql`INSERT INTO ${tableSql} (migration_id, name) SELECT ${id}, ${name}`)
      applied.push([id, name])
    }

    yield* Effect.logDebug(`Migrations complete`)
    return applied
  })

/**
 * Creates a layer that runs the configured ClickHouse migrations during layer
 * construction and provides no services.
 *
 * @stability unstable
 * @category layers
 * @since 4.0.0
 */
export const layer = <R>(
  options: Migrator.MigratorOptions<R>
): Layer.Layer<
  never,
  Migrator.MigrationError | SqlError,
  Client.SqlClient | R
> => Layer.effectDiscard(run(options))
