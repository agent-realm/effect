---
"@effect/sql-clickhouse": patch
---

Return no rows instead of failing for statements that answer with an empty body (DDL, `INSERT`, `ALTER`) on the default query path, and parse results with the client's configured `json.parse`.
