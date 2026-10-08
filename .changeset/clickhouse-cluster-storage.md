---
"@effect/sql-clickhouse": patch
"effect": patch
---

Add ClickHouse cluster storage: `ClickhouseRunnerStorage` and `ClickhouseMessageStorage` keep runner registrations, shard locks and mailboxes in ClickHouse, with contended state in `KeeperMap` tables (the server needs ClickHouse Keeper and a `keeper_map_path_prefix`). `SqlRunnerStorage` and `SqlMessageStorage` now fail with a defect naming these modules when given a ClickHouse client, instead of running sqlite SQL against it.
