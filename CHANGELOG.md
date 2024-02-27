# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Fixes
- fixed missed input variable issue (#15)
- fixed sql.Scanner/driver.Valuer support in BitBool/shardid.ID (#16)

## [1.2.0] - 2024-02-36
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
