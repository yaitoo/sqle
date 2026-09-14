# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed (breaking)
- fix(db): `DB.On(shardid.ID)` now returns `(*Client, error)` instead of `*Client`.
  It returns the new exported `ErrInvalidShardID` when `id.DatabaseID` is out
  of range for the current sharded DB pool (negative or `>= len(dbs)`),
  replacing the previous index-out-of-range panic (#69). The check covers
  negative values too, so a forged ID whose `DatabaseID` would cast to a
  negative `int` is also caught. This is observable: callers that previously
  received `*Client` must now handle the error explicitly. The signature
  change matches the existing `DB.OnDHT` return shape. Callers that only
  generate IDs through `shardid.New(WithDatabase(N)).Next()` against an
  `Open(dbs...)` of size N see no behaviour change.

### Fixed
- fix(mapr): require a non-nil `less` comparator in `MapR.QueryLimit` when
  `limit > 0` (#68). Previously, `less == nil` silently skipped the
  in-memory sort after merging rows from each shard, so the per-shard
  `LIMIT N` results were returned in whatever order each DB chose. That
  made pagination page boundaries non-deterministic across runs and
  gave different requests different "top-N" answers. `QueryLimit` now
  returns the new exported `ErrInvalidArgument` early in that case, so
  callers can `errors.Is` against it. Callers that genuinely want the
  unordered merged result should keep `less == nil` and pass `limit <= 0`.

### Fixed
- fix(exec): forward caller ctx in `Client.ExecContext` and `Tx.ExecContext`
  empty-args branches instead of silently substituting `context.Background()`
  (#59). Callers that passed a `context.WithTimeout` / `context.WithCancel`
  to `ExecContext` on argument-less calls (e.g. DDL like `CREATE TABLE`)
  previously had their cancellation and deadline dropped; they are now
  honoured. This is a bug fix, but it is observable: a call that returned
  `(Result, nil)` for a pre-cancelled ctx will now return
  `context.Canceled`. No in-repo caller relied on the discarded-ctx
  behaviour.

### Fixed
- fix(migrate): report non-`ErrNoRows` errors from `getMigrationStatus` as
  a new `MigrationStatusUnknown` instead of misclassifying the script as
  `MigrationStatusNew` (#67). Previously a transient DB error during the
  `sqle_migrations` lookup (driver timeout, lost connection, missing
  table) caused the caller to abort the transaction but report the
  script as fresh, which (a) made operator dashboards show "new
  migration" when the real problem was a DB outage and (b) risked
  re-executing a non-idempotent script on the next run. The status check
  now propagates the underlying error and surfaces a `[?]` log line;
  `startMigrate` aborts before any DDL or insert into `sqle_migrations`.

### Fixed
- fix(migrate): propagate caller ctx to `tx.ExecContext` inside
  `db.Transaction(ctx, ...)` blocks (#59). Previously the migrator's
  per-statement Exec calls used the ctx-less wrapper and silently dropped
  the outer cancellation/timeout.

## [1.5.2] - 2024-12-12
- fix(rows): don't close rows in rows.Scan (#49)

## [1.5.1] - 2024-06-12
- !fix(orderby): use BuildOption instead of allowedColumns (#46)
- feat(string): added nullable String/Null for sql/json (#47)

## [1.5.0] - 2024-04-30
### Changed
- !renamed `Context` with `Client` (#45)
  
### Added
- added `Connector` interface (#43)
- added nullable `Time` with better json support (#44)
- added `DTC` (#45)
  
### Changed
- !renamed `BitBool` with shorter name `Bool` (#44)

## [1.4.6] - 2014-04-23
### Changed
- implements json.Marshaler and json.Unmarshaler on `ID` (#41)
- added `context` support in `tx.QueryRowBuilder` and `tx.QueryBuilder` (#42)

## [1.4.5] - 2014-04-20
### Fixes
- fix(sqlbuilder): fixed WithWhere/WithOrderBy for empty builder (#39)
- fixed timer performance issue (#38)
- fixed StmtMaxIdleTime missing issue (#38)
- used int64 instead of int in `Queryer.Count` (#37)

## [1.4.4] - 2014-04-19
### Added
- added `NewWhere` and `WithWhere` (#35)
- added `NewOrderBy` and `WithOrderBy` (#36)
- added `LimitResult` and `LimitOption` (#36)


## [1.4.3] - 2014-04-12
### Fixes
- fixed close issue when it fails to build prepareStmt (#33)
- improved `OrderByBuilder` for api input (#34)
  
## [1.4.2] - 2014-04-10
### Added
- added `OrderByBuilder` to prevent sql injection (#32, #33)

## [1.4.1] - 2014-04-09
### Added
- added multi-dht support on `DB` (#31)

### Fixes
- stmt that is in using should not be close in background clean worker (#31)

## [1.4.0] - 2014-04-06
### Added
- added DHT/HashRing in shardid (#30)
- added NewDHT/DHTAdd/DHTAdded/OnDHT on db (#30)

## [1.3.2] - 2014-03-28
### Added
- added module name in migration (#29)

## [1.3.1] - 2014-03-19
### Added
- added `Duration` to support `Sacnner` and `Valuer` in sql driver (#27)

## [1.3.0] - 2014-03-11
### Added 
- added `Query[T]` feature and `MapR[T]` Queryer for cross-databases query (#21)

## [1.2.2] - 2024-03-05
### Added 
- added detail logs on migration (#17)
- added rotated table migration support (#17)
- added `Rotate` method for rotate service (#18)

### Fixed
- fixed rollback issue on `Transaction` (#17)

## [1.2.1] - 2024-02-28
### Fixed
- fixed missed input variable issue (#15)
- fixed sql.Scanner/driver.Valuer support in BitBool/shardid.ID (#16)
  

## [1.2.0] - 2024-02-26
### Added
- added `BitBool` for mysql bit type (#11)
- added `sharding` feature (#12)
- added `On` on `DB` to enable AutoSharding feature (#13)
- added `On` on `SQLBuilder` to enable AutoRotation feature (#13)

### Fixed
- fixed parameterized placeholder for postgresql (#12)
- sorted columns in `SetMap` for PrepareStmt performance (#14)

## [1.1.0] - 2024-02-13
### Added
- added `BuilderOption` on `SetMap` on `InsertBuilder` and `UpdateBuilder` (#1)
- added `WithAllow` BuilderOption (#2)
- added custom `Binder` support on `Bind` (#4)
- added `Select` and `Delete` on `SQLBuilder` (#6)
- added `PreparedStmt` support on `Query` and `Exec` (#7)
- added `PreparedStmt` support on `Tx` (#8)
### Fixed
- fixed `sql.Scanner` support on `Bind` (#2)
  
## [1.0.0] - 2024-01-31
