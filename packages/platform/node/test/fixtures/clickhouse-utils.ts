import { ClickhouseClient } from "@effect/sql-clickhouse"
import type { StartedClickHouseContainer } from "@testcontainers/clickhouse"
import { ClickHouseContainer } from "@testcontainers/clickhouse"
import { Context, Data, Effect, Layer, Schedule } from "effect"

export class ContainerError extends Data.TaggedError("ContainerError")<{
  cause: unknown
}> {}

// A single-node embedded Keeper, so KeeperMap tables work without a separate
// coordination container.
const keeperConfig = `<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <raft_configuration>
      <server>
        <id>1</id>
        <hostname>localhost</hostname>
        <port>9234</port>
      </server>
    </raft_configuration>
  </keeper_server>
  <zookeeper>
    <node>
      <host>localhost</host>
      <port>9181</port>
    </node>
  </zookeeper>
  <keeper_map_path_prefix>/keeper_map_tables</keeper_map_path_prefix>
</clickhouse>
`

// The HTTP interface answers before Keeper has elected its leader, so wait
// until a Keeper read succeeds.
const waitForKeeper = (container: StartedClickHouseContainer) =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(
        `${container.getHttpUrl()}/?query=${
          encodeURIComponent("SELECT count() FROM system.zookeeper WHERE path = '/'")
        }`,
        {
          headers: {
            "X-ClickHouse-User": container.getUsername(),
            "X-ClickHouse-Key": container.getPassword()
          }
        }
      )
      if (!response.ok) {
        throw new Error(await response.text())
      }
    },
    catch: (cause) => new ContainerError({ cause })
  }).pipe(
    Effect.retry({ schedule: Schedule.spaced(250), times: 120 })
  )

export class ClickhouseContainer extends Context.Service<
  ClickhouseContainer,
  StartedClickHouseContainer
>()("test/ClickhouseContainer") {
  static readonly layer = Layer.effect(this)(
    Effect.acquireRelease(
      Effect.tryPromise({
        try: () =>
          new ClickHouseContainer("clickhouse/clickhouse-server:26.8")
            .withCopyContentToContainer([{
              content: keeperConfig,
              target: "/etc/clickhouse-server/config.d/keeper.xml"
            }])
            .start(),
        catch: (cause) => new ContainerError({ cause })
      }).pipe(
        Effect.tap((container) =>
          waitForKeeper(container).pipe(
            Effect.onError(() => Effect.promise(() => container.stop()))
          )
        )
      ),
      (container) => Effect.promise(() => container.stop())
    )
  )

  static client = Layer.unwrap(
    Effect.gen(function*() {
      const container = yield* ClickhouseContainer
      return ClickhouseClient.layer(container.getClientOptions())
    })
  )

  static layerClient = this.client.pipe(Layer.provide(this.layer))
}
