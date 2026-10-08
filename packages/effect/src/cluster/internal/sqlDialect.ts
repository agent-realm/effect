import * as Effect from "../../Effect.ts"
import * as SqlClient from "../../sql/SqlClient.ts"

/**
 * The SQL cluster storages send every dialect they do not know down the
 * sqlite path. ClickHouse has dedicated storages, so stop with a defect that
 * names them instead of running sqlite SQL against it.
 *
 * @internal
 */
export const rejectClickhouse = (
  module: string,
  replacement: string
): Effect.Effect<void, never, SqlClient.SqlClient> =>
  Effect.gen(function*() {
    const sql = yield* SqlClient.SqlClient
    if (sql.onDialectOrElse({ clickhouse: () => true, orElse: () => false })) {
      return yield* Effect.die(
        new Error(`${module} does not support ClickHouse; use ${replacement} from @effect/sql-clickhouse`)
      )
    }
  })
