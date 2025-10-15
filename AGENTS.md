# SQLE Agents Guide

Audience: AI agents that need to integrate and use the SQLE library without reading the entire codebase.

Goal: Provide a concise, task-oriented overview of SQLE features and safe usage patterns for calling its APIs.


## 1) Mental model in 60 seconds
- sqle wraps database/sql with a SQL-first, ORM-lite experience.
- Core pillars:
  - SQLBuilder builds parameterized SQL with two token types: {param} → positional/named args; <input> → raw text substitution.
  - Binder-based scanning binds rows into struct/map/slices or primitives.
  - Shard/rotate aware DB and Query abstractions (Map/Reduce queryer, shardid integration).
  - Prepared statement caching on Client/Tx with idle eviction.
  - Migration + rotation utilities (migrate/).


## Quickstart
- Connect and select
```
sqldb, _ := sql.Open("mysql", dsn)
db := sqle.Open(sqldb)
b := sqle.New().Select("users").Where("id={id}").Param("id", 1)
var u User
_ = db.QueryRowBuilder(ctx, b).Bind(&u)
```
- Insert
```
b = sqle.New().Insert("users").Set("email", email).Set("name", name).End()
_, _ = db.ExecBuilder(ctx, b)
```
- With Postgres placeholders
```
b := sqle.New("SELECT * FROM users WHERE id={id}")
sqle.UsePostgres(b)
_, _, _ = b.Build() // uses $1, $2 ...
```
- Configure statement cache and multi-DB
```
sqle.StmtMaxIdleTime = 2 * time.Minute
sqldb1, _ := sql.Open("mysql", dsn1)
sqldb2, _ := sql.Open("mysql", dsn2)
db := sqle.Open(sqldb1, sqldb2) // two shards
// later scale out
sqldb3, _ := sql.Open("mysql", dsn3)
db.Add(sqldb3)
```




## Cookbook: feature coverage

- Update and Delete
```
// Update
b := sqle.New().Update("users").
    Set("name", name).
    Where("id={id}").Param("id", id)
_, _ = db.ExecBuilder(ctx, b)

// Delete
b = sqle.New().Delete("users").Where("id={id}").Param("id", id)
_, _ = db.ExecBuilder(ctx, b)
```

- Conditional WHERE and ORDER BY (safe)
```
b := sqle.New().Select("users").Where().
    If(q.Name != "").And("name={name}").Param("name", q.Name).
    If(q.Email != "").And("email={email}").Param("email", q.Email).
    End()
// Restrict allowed columns
ob := b.Order(sqle.WithAllow("created_at","name"))
- Insert from map and model
```
inputs := map[string]any{"email": email, "name": name}
b := sqle.New().Insert("users").SetMap(inputs).End()
_, _ = db.ExecBuilder(ctx, b)

// ORM-like: from struct
b = sqle.New().Insert("users").SetModel(user).End()
_, _ = db.ExecBuilder(ctx, b)
- Update: SetExpr and SetMap
```
// Expression update
b := sqle.New().Update("users").SetExpr("last_login = NOW()")
// Mixed with params
b.Set("name", name).Where("id={id}").Param("id", id)
_, _ = db.ExecBuilder(ctx, b)

// Map update (allow-list columns)
changes := map[string]any{"name": name, "email": email, "role": role}
b = sqle.New().Update("users").SetMap(changes, sqle.WithAllow("name","email"))
b.Where("id={id}").Param("id", id)
_, _ = db.ExecBuilder(ctx, b)
```

```

- Order builder reuse
```
base := sqle.New().Select("users").Where().End()
ob := base.Order(sqle.WithAllow("created_at","name"))
ob.ByDesc("created_at")
base.WithOrderBy(ob)
rows, _ := db.QueryBuilder(ctx, base)
```

ob.ByDesc("created_at").ByAsc("name")
// Pagination
b.SQL(" LIMIT {limit} OFFSET {offset}").Param("limit", q.Limit).Param("offset", q.Offset)
rows, _ := db.QueryBuilder(ctx, b)
var users []User
_ = rows.Bind(&users)
```

- Binding variations
```
// Single primitive
var n int64
_ = db.QueryRow("SELECT COUNT(*) FROM users").Scan(&n)

// Single struct
var u User
_ = db.QueryRowBuilder(ctx, sqle.New().Select("users").Where("id={id}").Param("id", id)).Bind(&u)

// Slice of structs
var list []User
_ = db.QueryBuilder(ctx, sqle.New().Select("users")).Bind(&list)

// Slice of maps
var items []map[string]any
_ = db.QueryBuilder(ctx, sqle.New().Select("users")).Bind(&items)
```

- Transactions
```
_ = db.Transaction(ctx, nil, func(ctx context.Context, tx *sqle.Tx) error {
    if _, err := tx.Exec("UPDATE accounts SET balance=balance-? WHERE id=?", amt, from); err != nil { return err }
    if _, err := tx.Exec("UPDATE accounts SET balance=balance+? WHERE id=?", amt, to); err != nil { return err }
    // Read within tx
    var u User
    if err := tx.QueryRowBuilder(ctx, sqle.New().Select("users").Where("id={id}").Param("id", to)).Bind(&u); err != nil { return err }
    return nil
})
```

- Distributed transactions (DTC)
```
dtc := sqle.NewDTC(ctx, nil)
dtc.Prepare(db1, func(ctx context.Context, c sqle.Connector) error {
    _, err := c.Exec("INSERT INTO a(id,val) VALUES(?,?)", id, v); return err
}, func(ctx context.Context, c sqle.Connector) error {
    _, err := c.Exec("DELETE FROM a WHERE id=?", id); return err
})
- QueryLimit across shards
```
q := sqle.NewQuery[User](db)
- Error examples
```
_, _, err := sqle.New("SELECT * FROM t WHERE a={a}").Build() // missing Param("a", ...)
if errors.Is(err, sqle.ErrInvalidParamVariable) { /* handle */ }

var m map[int]any
err = db.QueryBuilder(ctx, sqle.New().Select("t")).Bind(&m)
// -> ErrMustStringKey because map key must be string
```

b := sqle.New().Select("users").Where("role={r}").Param("r", "admin")
list, _ := q.QueryLimit(ctx, b, func(i, j User) bool { return i.ID < j.ID }, 20)
```

dtc.Prepare(db2, func(ctx context.Context, c sqle.Connector) error {
    _, err := c.Exec("INSERT INTO b(id,val) VALUES(?,?)", id, v); return err
}, func(ctx context.Context, c sqle.Connector) error {
    _, err := c.Exec("DELETE FROM b WHERE id=?", id); return err
})
if err := dtc.Commit(); err != nil { _ = dtc.Rollback() }
```

- Sharding + table rotation with shardid
```
// Configure generator: 4 databases, monthly rotation
gen := shardid.New(shardid.WithDatabase(4), shardid.WithMonthlyRotate())
id := gen.Next()

// Write to rotated table on selected DB
b := sqle.New().On(id).Insert("orders<rotate>").
    Set("order_id", oid).Set("amount", amt).End()
_, _ = db.On(id).ExecBuilder(ctx, b)

// Query across sharded DBs for a time window using MapR
- MapR Query with custom Queryer
```
// Implement a custom Queryer[T] if needed and inject
var my Queryer[User] = &MyQueryer{}
q := sqle.NewQuery[User](db, sqle.WithQueryer(my))
users, _ := q.Query(ctx, sqle.New().Select("users"), nil)
```

q := sqle.NewQuery[Order](db, sqle.WithMonths(start, end))
b = sqle.New().Select("orders<rotate>").Where("member_id={mid}").Param("mid", mid)
list, _ := q.Query(ctx, b, func(i, j Order) bool { return i.CreatedAt.After(j.CreatedAt) })
```

- DHT-based sharding (consistent hashing)
```
db.NewDHT("users", 0,1,2,3)
c, _ := db.OnDHT(email, "users")
_ = c.QueryRowBuilder(ctx, sqle.New().Select("profiles").Where("email={email}").Param("email", email)).Bind(&u)
```

- Map/Reduce querying helpers
```
// Count across rotations and shards
qb := sqle.New("SELECT COUNT(*) FROM logs<rotate> WHERE level={lv}").Param("lv", "warn")
q := sqle.NewQuery[int64](db, sqle.WithDays(start, end))
_, _ = q.Count(ctx, qb)

// First returns fastest shard/rotation result
var one Log
one, _ = sqle.NewQuery[Log](db, sqle.WithWeeks(w1, w2)).First(ctx, sqle.New().Select("logs<rotate>").Where("id={id}").Param("id", lid))
```

- Migration and rotation
```
//go:embed db
var migrations embed.FS
m := migrate.New(db)
_ = m.Discover(migrations, migrate.WithModule("service"))
_ = m.Init(ctx)
_ = m.Migrate(ctx) // apply semver directories
_ = m.Rotate(ctx)  // create rotated tables for current/next periods
```

- Driver parameter styles
```
b := sqle.New("SELECT * FROM t WHERE a={a} AND b={b}").Param("a",1).Param("b",2)
sqle.UsePostgres(b) // $1, $2 ...
// sqle.UseOracle(b) // :a, :b
// sqle.UseMySQL(b)  // ? (default)
```

- Nullable/time/duration/bool helpers
```

- Limited queries (LimitOptions/LimitResult)
```
// Define allowed order columns and page size
ob := sqle.New().Order(sqle.WithAllow("created_at","id"))
ob.ByDesc("created_at")

opts := &sqle.LimitOptions{Offset: (page-1)*size, Limit: size, OrderBy: ob}

// Build base query then apply order/limit
b := sqle.New().Select("users").Where().
    If(filter != "").And("name LIKE {kw}").Param("kw", "%"+filter+"%").End()
if opts.OrderBy != nil {
    b.WithOrderBy(opts.OrderBy)
}
if opts.Limit > 0 { b.SQL(" LIMIT {limit}").Param("limit", opts.Limit) }
if opts.Offset > 0 { b.SQL(" OFFSET {offset}").Param("offset", opts.Offset) }

// Fetch items and total
rows, _ := db.QueryBuilder(ctx, b)
var items []User
_ = rows.Bind(&items)

var total int64
_ = db.QueryRowBuilder(ctx, sqle.New("SELECT COUNT(*) FROM users")).Scan(&total)

res := sqle.LimitResult[User]{Items: items, Total: total}
```

var s sqle.String = sqle.NewString("v")
var t sqle.Time = sqle.NewTime(time.Now(), true)
var d sqle.Duration = sqle.Duration(5*time.Minute)
var bflag sqle.Bool = sqle.Bool(true)
_, _ = db.Exec("INSERT INTO x(s,t,d,b) VALUES(?,?,?,?)", s, t, d, bflag)
```


## Cookbook: comprehensive feature coverage

- Update with SetModel
```
b := sqle.New().Update("users").SetModel(user).Where("id={id}").Param("id", user.ID)
_, _ = db.ExecBuilder(ctx, b)
```

- WhereBuilder reuse via WithWhere
```
wb := sqle.NewWhere().And("status={st}").And("role={r}").
    Param("st", "active").Param("r", "admin")
base := sqle.New().Select("users").WithWhere(wb).Where("org_id={org}").Param("org", orgID)
rows, _ := db.QueryBuilder(ctx, base)
```

- Inputs and Params (raw inputs vs bound params)
```
// Trusted identifier/table name via Input (ensure validated!)
b := sqle.New("SELECT * FROM <tbl> WHERE id={id}").
    Input("tbl", "`users`").Param("id", 1)
_, _, _ = b.Build()
```

- Params and Params(map)
```
b := sqle.New("UPDATE t SET a={a}, b={b} WHERE id={id}").
    Params(map[string]any{"a":1, "b":2, "id": id})
_, _ = db.ExecBuilder(ctx, b)
```

- Column name transform with WithToName
```
changes := map[string]any{"UserName": name, "CreatedAt": createdAt}
b := sqle.New().Insert("users").SetMap(changes, sqle.WithToName(strcase.ToSnake)).End()
_, _ = db.ExecBuilder(ctx, b)
```

- OrderBy via NewOrderBy builder
```
ob := sqle.NewOrderBy(sqle.WithAllow("created_at","name")).ByDesc("created_at").ByAsc("name")
b := sqle.New().Select("users")
b.WithOrderBy(ob)
rows, _ := db.QueryBuilder(ctx, b)
```

- Explicit transaction with BeginTx
```
tx, _ := db.BeginTx(ctx, &sql.TxOptions{})
defer tx.Rollback()
_, _ = tx.ExecContext(ctx, "UPDATE t SET v=? WHERE id=?", v, id)
if err := tx.Commit(); err != nil { /* handle */ }
```

- OnDHT error handling
```
_, err := db.OnDHT(email, "users")
if errors.Is(err, sqle.ErrMissingDHT) {
    db.NewDHT("users", 0,1,2,3)
}
c, _ := db.OnDHT(email, "users")
_ = c.QueryRow("SELECT 1").Err()
```

- Generic Null[T] and JSON
```
var n sqle.Null[int64] = sqle.NewNull[int64](123, true)
buf, _ := json.Marshal(n)   // => 123
_ = n.UnmarshalJSON([]byte("null")) // sets Valid=false
```

- Migration: rotated script header
```
/* rotate: monthly = 20240201 - 20240401 */
CREATE TABLE IF NOT EXISTS monthly_logs<rotate> (
  id BIGINT PRIMARY KEY,
  msg VARCHAR(64) NOT NULL
);
```

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


## 17) Notes for AI agents
- Keep changes minimal and localized when proposing edits.
- Prefer adding small helpers over altering core behavior, unless required.
- Maintain backward compatibility of public APIs.
- Update README/AGENTS.md when behavior changes.
