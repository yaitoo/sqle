# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]


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
