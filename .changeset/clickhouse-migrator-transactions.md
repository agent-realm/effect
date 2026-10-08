---
"@effect/sql-clickhouse": patch
---

Fix `ClickhouseMigrator`, which could not run on ClickHouse: it now keeps its history in a `MergeTree` table and records each migration after it succeeds, without a transaction.
