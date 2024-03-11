# SQLE
A SQL-First/ORM-like Golang SQL enhanced package.

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Tests](https://github.com/yaitoo/sqle/actions/workflows/tests.yml/badge.svg)](https://github.com/yaitoo/sqle/actions/workflows/tests.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/yaitoo/sqle.svg)](https://pkg.go.dev/github.com/yaitoo/sqle)
[![Codecov](https://codecov.io/gh/yaitoo/sqle/branch/main/graph/badge.svg)](https://codecov.io/gh/yaitoo/sqle)
[![GitHub Release](https://img.shields.io/github/v/release/yaitoo/sqle)](https://github.com/yaitoo/sqle/blob/main/CHANGELOG.md)
[![Go Report Card](https://goreportcard.com/badge/yaitoo/sqle)](http://goreportcard.com/report/yaitoo/sqle)

The SQLE package provides extensions to Go’s built-in `database/sql` package for more efficient, comprehensive interactions with databases in Go. The SQLE package is backward-compatible and extendable, so you can easily use it with the database/sql package.

The SQLE package takes the sql-first approach and provides functionalities for marshaling rows into struct, map,slice and primitive types.

The SQLE package also provides functionalities for schema auto migration, logging, contexts, prepared statements, advanced database operations like sharding, and much more.

You’ll find the SQLE package useful if you’re not a fan of full-featured ORMs and you prefer to use the database/sql package with extra support and functionalities.

## Features
- Works with any database engine(eg MySQL, Postgres, SQLite...etc) by [Go-MySQL-Driver](https://github.com/go-sql-driver/mysql/)
- [ORM-like](rows_test.go) experience using good old SQL. SQLE supports structs, maps, primitive types, and
  slices of map/structs/primitive types.
- [SQLBuilder](sqlbuilder_test.go)
- 100% compatible drop-in replacement of "database/sql". Code is really easy to migrate from `database/sql` to `SQLE`. see [examples](row_test.go)
- [ShardID](shardid/README.md) is a `snowflakes-like` distributed unique identifier with extended metadata : worker, table rotation and database sharding, and sortable by time
- Table AutoRotation 
- Database AutoSharding
- [Migration](migrate/migrator_test.go): migrate database with sql files organized in filesystem. it supports to migrate table and multiple rotated tables on all sharding database instances.

## Tutorials
> All examples on https://go.dev/doc/tutorial/database-access can directly work with `sqle.DB` instance. 
>

### Install SQLE
- install latest commit from `main` branch
```
go get github.com/yaitoo/sqle@main
```

- install latest release
```
go get github.com/yaitoo/sqle@latest
```

### Connecting to a Database
SQLE directly connects to a database by `sql.DB` instance. 
```
    driver := viper.GetString("db.driver")
    dsn := viper.GetString("db.dsn")

    var db *sqle.DB

    switch driver {
        case "sqlite":
            sqldb, err := sql.Open("sqlite3", "file:"+dsn+"?cache=shared")
            if err != nil {
                panic(fmt.Sprintf("db: failed to open sqlite database %s", dsn))
            }

            db = sqle.Open(sqldb)


        case "mysql":
            sqldb, err := sql.Open("mysql", dsn)
            if err != nil {
                panic(fmt.Sprintf("db: failed to open mysql database %s", dsn))
            }

            db = sqle.Open(sqldb)
        
        default:
            panic(fmt.Sprintf("db: driver %s is not supported yet", driver))
    }
    
    if  err := db.Ping(); err == nil {
        panic("db: database is unreachable")
    }
```

### Create
- create album by sql
```
func addAlbum(alb Album) (int64, error) {
    result, err := db.Exec("INSERT INTO album (title, artist, price) VALUES (?, ?, ?)", alb.Title, alb.Artist, alb.Price)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    return id, nil
}
```
- create album by named sql statement
```
func addAlbum(alb Album) (int64, error) {
    cmd := sqle.New("INSERT INTO album (title, artist, price) VALUES ({title}, {artist}, {price}").
      Param("title", alb.Title).
      Param("artist", alb.Artist).
      Param("price", alb.Price)

    result, err := db.ExecBuilder(context.TODO(),cmd)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    return id, nil
}
```
- create album by `InsertBuilder` feature
```
func addAlbum(alb Album) (int64, error) {
    cmd := sqle.New().Insert("album").
      Set("title", alb.Title).
      Set("artist", alb.Artist).
      Set("price", alb.Price).
      End()

    result, err := db.ExecBuilder(context.TODO(),cmd)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    return id, nil
}
```
- create album by `map` object
```
func addAlbum(alb Album) (int64, error) {
    inputs := map[string]any{
      "title":alb.Title,
      "artist":alb.Artist,
      "price":alb.Price,
    }

    cmd := sqle.New().Insert("album").
      SetMap(inputs)
      End()

    result, err := db.ExecBuilder(context.TODO(),cmd)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    return id, nil
}
```
- create album by ORM-like feature
```
func addAlbum(alb Album) (int64, error) {
    cmd := sqle.New().Insert("album").
      SetModel(alb).
      End()

    result, err := db.ExecBuilder(context.TODO(),cmd)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    id, err := result.LastInsertId()
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    return id, nil
}
```

### Query
#### query for multiple rows
- query albums by sql
```
func albumsByArtist(name string) ([]Album, error) {
    // An albums slice to hold data from returned rows.
    var albums []Album

    rows, err := db.Query("SELECT * FROM album WHERE artist = ?", name)
    if err != nil {
        return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
    }
    defer rows.Close()
    // Loop through rows, using Scan to assign column data to struct fields.
    for rows.Next() {
        var alb Album
        if err := rows.Scan(&alb.ID, &alb.Title, &alb.Artist, &alb.Price); err != nil {
            return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
        }
        albums = append(albums, alb)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
    }
    return albums, nil
}
```
- query albums by named sql statement
```
func albumsByArtist(name string) ([]Album, error) {
    // An albums slice to hold data from returned rows.
    var albums []Album

    cmd := sql.New("SELECT * FROM album WHERE artist = {artist}").
    Param("artist",name)

    rows, err := db.QueryBuilder(context.TODO(), cmd)
    if err != nil {
        return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
    }
    defer rows.Close()
    // Loop through rows, using Scan to assign column data to struct fields.
    for rows.Next() {
        var alb Album
        if err := rows.Scan(&alb.ID, &alb.Title, &alb.Artist, &alb.Price); err != nil {
            return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
        }
        albums = append(albums, alb)
    }
    if err := rows.Err(); err != nil {
        return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
    }
    return albums, nil
}
```
- query albums by `WhereBuilder` feature
```
func albumsByArtist(name string) ([]Album, error) {
    // An albums slice to hold data from returned rows.
    var albums []Album

    cmd := sql.New("SELECT * FROM album").Where().
    If(name != "").And("artist = {artist}").Param("artist",name)

    err := db.QueryBuilder(context.TODO(), cmd).Bind(&albums)
    if err != nil {
        return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
    }
    
    return albums, nil
}
```

- query albums by ORM-like feature
```
func albumsByArtist(name string) ([]Album, error) {
    // An albums slice to hold data from returned rows.
    var albums []Album

    cmd := sql.New().Select("album").Where().
        If(name != "").And("artist = {artist}").
        Param("artist",name)

    err := db.QueryBuilder(context.TODO(), cmd).Bind(&albums)
    if err != nil {
        return nil, fmt.Errorf("albumsByArtist %q: %v", name, err)
    }
    
    return albums, nil
}
```

#### query for a single row
- query album by sql
```
func albumByID(id int64) (Album, error) {
    // An album to hold data from the returned row.
    var alb Album

    row := db.QueryRow("SELECT * FROM album WHERE id = ?", id)
    if err := row.Scan(&alb.ID, &alb.Title, &alb.Artist, &alb.Price); err != nil {
        if err == sql.ErrNoRows {
            return alb, fmt.Errorf("albumsById %d: no such album", id)
        }
        return alb, fmt.Errorf("albumsById %d: %v", id, err)
    }
    return alb, nil
}
```

- query album by named sql statement
```
func albumByID(id int64) (Album, error) {
    // An album to hold data from the returned row.
    var alb Album
    cmd := sqle.New("SELECT * FROM album").
        Where("id = {id}").
        Param("id",id)

    row := db.QueryRowBuilder(context.TODO(),cmd)
    if err := row.Scan(&alb.ID, &alb.Title, &alb.Artist, &alb.Price); err != nil {
        if err == sql.ErrNoRows {
            return alb, fmt.Errorf("albumsById %d: no such album", id)
        }
        return alb, fmt.Errorf("albumsById %d: %v", id, err)
    }
    return alb, nil
}
```

- query album by ORM-like feature
```
func albumByID(id int64) (Album, error) {
    // An album to hold data from the returned row.
    var alb Album
    cmd := sqle.New().Select("album").
        Where("id = {id}").
        Param("id",id)

    err := db.QueryRowBuilder(context.TODO(),cmd).Bind(&alb)
    if err != nil {
        if err == sql.ErrNoRows {
            return alb, fmt.Errorf("albumsById %d: no such album", id)
        }
        return alb, fmt.Errorf("albumsById %d: %v", id, err)
    }
    return alb, nil
}
```

### Update
- update album by sql
```
func updateAlbum(alb Album) error {
    _, err := db.Exec("UPDATE album SET title=?, artist=?, price=? WHERE id=?", alb.Title, alb.Artist, alb.Price, alb.ID)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
    return err
}
```

- update album by named sql statement
```
func updateAlbum(alb Album) error {
    cmd := sqle.New("UPDATE album SET title={title}, artist={artist}, price={price} WHERE id={id}").
        Param("title", alb.Title).
        Param("artist", alb.Artist).
        Param("price", alb.Price).
        Param("id", alb.ID)

    _, err := db.ExecBuilder(context.TODO(), cmd)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
   
    return err
}
```

- update album by `UpdateBuilder` feature
```
func updateAlbum(alb Album) error {
    cmd := sqle.New().Update("album").
        Set("title", alb.Title).
        Set("artist", alb.Artist).
        Set("price", alb.Price).
        Set("id", alb.ID).
        Where("id={id}").Param("id", alb.ID)

    _, err := db.ExecBuilder(context.TODO(), cmd)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
   
    return err
}
```

- update album by ORM-like feature
```
func updateAlbum(alb Album) error {
    cmd := sqle.New().Update("album").
        SetModel(alb).
        Where("id={id}").Param("id", alb.ID)

    _, err := db.ExecBuilder(context.TODO(), cmd)
    if err != nil {
        return 0, fmt.Errorf("addAlbum: %v", err)
    }
   
    return err
}
```

### Delete
- delete album by sql
```
func deleteAlbumByID(id int64) error {
    _, err := db.Exec("DELETE FROM album WHERE id = ?", id)

    return err 
}
```
- delete album by named sql statement
```
func deleteAlbumByID(id int64) error {
    _, err := db.ExecBuilder(context.TODO(), sqle.New().Delete("album").Where("id = {id}").
        Param("id",id))

    return err 
}
```

### Transaction
perform a set of operations within a transaction
```
func deleteAlbums(ids []int64) error {

    return db.Transaction(ctx, &sql.TxOptions{}, func(ctx context.Context,tx *sqle.Tx) error {
        var err error
        for _, id := range ids {
            _, err = tx.Exec("DELETE FROM album WHERE id=?",id)
            if err != nil {
                return err
            }
        }
    })
}
```


## Table Rotation
use `shardid.ID` to enable rotate feature for a table based on option (NoRotate/MonthlyRotate/WeeklyRotate/DailyRotate)

```
gen := shardid.New(shardid.WithMonthlyRotate())
id := gen.Next()

b := New().On(id) //call `On` to enable rotate feature, and setup an input variable <rotate>
b.Delete("orders<rotate>").Where().
    If(true).And("order_id = {order_id}").
    If(false).And("member_id").
    Param("order_id", "order_123456")


db.ExecBuilder(context.TODO(),b) //DELETE FROM `orders_20240220` WHERE order_id = ?
```
see more [examples](sqlbuilder_test.go#L490)


## Database Sharding
use `shardid.ID` to enable sharding feature for any sql
```
gen := shardid.New(WithDatabase(10)) // 10 database instances
id := gen.Next()

b := New().On(id) //call `On` to setup an input variable named `rotate`, and enable rotation feature
b.Delete("orders<rotate>").Where().
    If(true).And("order_id = {order_id}").
    If(false).And("member_id").
    Param("order_id", "order_123456")


db.On(id). //automatically select database based on `id.DatabaseID`
 ExecBuilder(context.TODO(),b) //DELETE FROM `orders` WHERE order_id = ?

```

see more [examples](db_test.go#L49)

## Migration
SQLE discovers migrations from local file system or go embed file system. There are two objects here.
- version directory
> it should be named as semver style. eg `0.0.1`/`1.0.2`. invalid naming will be ignored.
- migration script file
> the file format is `{rank}_{description}.{suffix}`  eg `1_locale.sql`. the suffix can be filtered in `Discover`. eg `m.Discover(os.DirFS("./db")), WithSuffix(".postgres"))`. `.sql` is default suffix filter.
the files should be organized as
```
├── db
│   └── 0.0.1
│       ├── 1_locale.sql
│       ├── 2_member.sql
│       ├── 3_order.sql
│   └── 0.0.2
│       ├── 1_add_times_orders.sql
│       ├── 2_logs.sql
```
and database can be migrated in code
```
    //go:embed db
	var migrations embed.FS

	m := migrate.New(db)
    // m.Discover(os.DirFS("./db"))
	if err := m.Discover(migrations); err != nil {
		panic(" db: migrate " + err.Error())
	}

	err = m.Init(context.TODO())
	if err != nil {
		panic(" db: migrate " + err.Error())
	}

	err = m.Migrate(context.TODO())
	if err != nil {
		panic(" db: migrate " + err.Error())
	}
```
see more [examples](./migrate/migrator_test.go?L40) 

if a table has been rotated, and migration should be started with `/* rotate: monthly=20240201-20240401*/` as first line. so the migration is applied automatically on the table and all it's rotated tables from 20240201 to 20240401.
```
/* rotate: monthly = 20240201 - 20240401 */
CREATE TABLE IF NOT EXISTS monthly_logs<rotate> (
    id int NOT NULL,
    msg varchar(50) NOT NULL,
    PRIMARY KEY (id)
);
```

`monthly_logs`, `monthly_logs_202402`, `monthly_logs_202403` and `monthly_logs_202404` will be migrated automatically.

see more [examples](./migrate/migrator_test.go?L360) 

if rotate is enabled for any table, rotate should be executed periodically in a cron job. so rotated tables will be created periodically. 
```
├── db
│   └── monthly
│       ├── members.sql
│   └── weekly
│       ├── orders.sql
│   └── daily
│       ├── logs.sql
```
see more [examples](./migrate/migrator_test.go?L581) 

## Security: SQL Injection
SQLE uses the database/sql‘s argument placeholders to build parameterized SQL statement, which will automatically escape arguments to avoid SQL injection. eg if it is PostgreSQL, please apply [UsePostgres](use.go#L5) on SQLBuilder or change [DefaultSQLQuote](sqlbuilder.go?L16) and [DefaultSQLParameterize](sqlbuilder.go?L17) to update parameterization options.

```
func UsePostgres(b *Builder) {
	b.Quote = "`"
	b.Parameterize = func(name string, index int) string {
		return "$" + strconv.Itoa(index)
	}
}
```

## Contributing
Contributions are welcome! If you're interested in contributing, please feel free to [contribute to SQLE](CONTRIBUTING.md)


## License
[MIT License](LICENSE)