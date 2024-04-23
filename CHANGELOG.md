# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


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
