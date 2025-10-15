# SQLE Agents Guide (xun-compatible)

Audience: AI agents that need to read, modify, or extend this repository with minimal code reading.

Goal: Provide a concise, task-oriented mental model and safe extension points.


## 1) Mental model in 60 seconds
- sqle wraps database/sql with a SQL-first, ORM-lite experience.
- Core pillars:
  - SQLBuilder builds parameterized SQL with two token types: {param} → positional/named args; <input> → raw text substitution.
  - Binder-based scanning binds rows into struct/map/slices or primitives.
  - Shard/rotate aware DB and Query abstractions (Map/Reduce queryer, shardid integration).
  - Prepared statement caching on Client/Tx with idle eviction.
  - Migration + rotation utilities (migrate/).


## 2) Repo map (what to read when)
- High level API: db.go, client.go, tx.go, connector.go, query.go, queryer.go, queryer_mapr.go
- SQL builder: sqlbuilder*.go, tokenizer.go, token.go, use.go, sqlbuilder_* files
- Row binding: row.go, rows.go, scan.go, binder*.go
- Types: null.go, time.go, string.go, duration.go, bool.go
- Sharding/rotation: shardid/, db.go (On, DHT), query_option.go
- Distributed Tx: dtc.go
- Migration/rotation: migrate/
- Examples: README.md, *_test.go files


## 3) Core types and flows
- DB: sharding-aware wrapper over multiple Client instances; Open(*sql.DB...), Add, On(shardid.ID), NewDHT/GetDHT/OnDHT.
- Client: wraps *sql.DB and caches prepared statements (Stmt). Provides Query/Exec and *Builder variants.
- Tx: wraps *sql.Tx with local prepared statement cache.
- Query[T]: high-level query facade over Queryer[T] (default MapR[T]). Supports First/Count/Query/QueryLimit and rotation window options.
- Queryer[T]: interface to implement backends. Default MapR[T] fans out over dbs and rotated tables, merges and sorts.
- Builder: SQL string builder with inputs (raw) and params (bound). Sub-builders: InsertBuilder, UpdateBuilder, WhereBuilder, OrderByBuilder.
- Binder: reflection-based binding to structs (with db tags or snake_case fallback), maps, and lists.


## 4) SQLBuilder essentials
Tokens
- {name}: parameter; becomes ?, $1, :name depending on Parameterize.
- <name>: input; copied verbatim into SQL (for trusted identifiers like rotated suffix). Do not use for user data.

Construction
- b := sqle.New("SELECT * FROM users").Where("id={id}").Param("id", 1)
- b.Select("users").Where().And("email={email}").Param("email", v)
- Insert: New().Insert("users").Set("email", v).End()
- Update: New().Update("users").Set("name", v).Where("id={id}").Param("id", id)
- Delete: New().Delete("users").Where("id={id}").Param("id", id)

Drivers
- UsePostgres/UseMySQL/UseOracle alter Parameterize (and Quote). Call once per Builder before Build.

Safety
- Inputs are raw. Only use <rotate> and trusted identifiers via b.Input or b.On(id).
- Params are safe and prepared.

Order/Where
- WhereBuilder: If(predicate) to conditionally append; And/Or to chain.
- OrderByBuilder: restrict columns via WithAllow/WithToName to prevent injection.


## 5) Binding and scanning
- Row.Bind(&T) or Rows.Bind(&[]T) auto-binds:
  - Structs: fields matched by `db:"name"` tag, else snake_case of field; column names normalized to lower + no underscores.
  - Maps: key must be string; value type inferred.
  - Slices of primitives/slices/structs/maps supported.
- Direct Scan works too. Errors: ErrMustPointer, ErrMustSlice, ErrMustStruct, ErrTypeNotBindable, ErrMustStringKey.


## 6) Prepared statements and performance
- Client caches prepared Stmt by query string with lastUsed and isUsing flags.
- StmtMaxIdleTime governs eviction (default 3m). Reuse() marks stmt usable again; Rows/Row Close calls handle this.
- Tx maintains its own short-lived stmt cache.


## 7) Sharding and rotation
- shardid.ID encodes DatabaseID and rotation suffix.
- Rotation: table names may include <rotate> placeholder, e.g., orders<rotate>.
- b.On(id) sets input rotate to id.RotateName(); db.On(id) selects the Client by id.DatabaseID.
- DHT-based sharding: db.NewDHT(name, dbIndexes...), db.OnDHT(key, name?) → *Client.
- Query time windows: WithMonths/WithWeeks/WithDays add rotated suffixes scanned by MapR.


## 8) High-level querying with MapR
- NewQuery[T](db, options...).Query(ctx, builder, less) runs across:
  - each rotated table in withRotatedTables ("" means base table)
  - each Client (database shard)
- First returns first completed result (race via async.WaitAny).
- Count sums counts from all shards/rotations.
- QueryLimit appends LIMIT per-db then trims to limit.


## 9) Transactions
- db.Transaction(ctx, opts, func(ctx, tx) error { ... }) with auto commit/rollback.
- Tx supports Query/Exec and *Builder variants; local prepare cache used when args present.


## 10) Distributed transactions (DTC)
- NewDTC(ctx, opts).Prepare(client, exec, revert) per-participant.
- Commit(): begins tx per participant, executes exec list, commits all; if later failure occurs use Rollback().
- Rollback(): if committed, call revert with Client; else rollback the Tx.
- Use Connector interface methods in exec/revert for portability over Client/Tx.


## 11) Migration and rotation management
- migrate.New(db1, db2, ...).Discover(fs, WithModule(...), WithSuffix(".sql"?))
- Version folders must be semver (e.g., 1.2.3) with files named {rank}_{name}.sql; rank controls order.
- Optional header to fan-out to rotated tables during Migrate:
  /* rotate: monthly = 20240201 - 20240401 */
- Tables created with <rotate> placeholder will be executed for each computed rotation.
- Init(ctx) creates sqle_migrations and sqle_rotations metadata tables.
- Rotate(ctx) ensures rotated tables for the current and next period using files under migrate/{monthly|weekly|daily} containing <rotate>.


## 12) Extending sqle safely
- Add a new driver style: implement a function like UseX(b *Builder) to set Quote/Parameterize.
- Custom query engine: implement Queryer[T] and pass WithQueryer(qr) to NewQuery[T].
- Custom binding: implement Binder to control Scan targets; see structBinder/mapBinder patterns.
- Column whitelisting/renaming: WithAllow and WithToName on BuilderOptions and OrderBy.


## 13) Error handling and invariants
- On missing param during Build → ErrInvalidParamVariable.
- OnDHT without DHT → ErrMissingDHT.
- Scanning requires pointers and correct kinds; see exported Err* in row.go.


## 14) Safe usage checklist for agents
- Always prefer params {name} over string concatenation.
- Only use <rotate> via b.On(id) or trusted inputs.
- For cross-shard queries, prefer NewQuery[T] + MapR; for single shard, use db.On(id) or db.OnDHT.
- When adding fields to structs, set `db:"column_name"` if database naming isn’t snake_case of the field.
- Use OrderByBuilder with WithAllow to sanitize order by clauses.
- Close Rows/Row or use Bind which handles Close.


## 15) Common recipes
- CRUD: see README sections Create/Query/Update/Delete (mirrors tests).
- Paginated + ordered list: build WHERE + Order(WithAllow(...)).ByAsc/ByDesc and add LIMIT/OFFSET via SQL.
- MapR query last N days: NewQuery[T](db, WithDays(start, end)).QueryLimit(ctx, b, less, N)
- Sharded write: db.On(id).ExecBuilder(ctx, b.On(id))


## 16) File references (quick jump)
- Builders: sqlbuilder.go, sqlbuilder_insert.go, sqlbuilder_update.go, sqlbuilder_where.go, sqlbuilder_orderby.go, sqlbuilder_option.go, use.go
- Query: query.go, queryer.go, queryer_mapr.go, query_option.go
- DB/Client/Tx: db.go, client.go, tx.go, client_stmt.go
- Binding: binder.go, binder_struct.go, binder_map.go, scan.go, row.go, rows.go
- Types: null.go, time.go, string.go, duration.go, bool.go
- Sharding: shardid/*, db.go (On, DHT)
- Migration: migrate/*


## 17) Notes for xun agents
- Keep diffs minimal and localized.
- Prefer adding small helper functions over changing core behavior, unless explicitly required.
- Maintain backward compatibility of public APIs.
- Update README/AGENTS.md only when behavior changes.
