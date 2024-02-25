# SQLE
A SQL-First/ORM-like Golang SQL enhanced package.

![License](https://img.shields.io/badge/license-MIT-green.svg)
[![Tests](https://github.com/yaitoo/sqle/actions/workflows/tests.yml/badge.svg)](https://github.com/yaitoo/sqle/actions/workflows/tests.yml)
[![GoDoc](https://godoc.org/github.com/yaitoo/sqle?status.png)](https://godoc.org/github.com/yaitoo/sqle)
[![Codecov](https://codecov.io/gh/yaitoo/sqle/branch/main/graph/badge.svg)](https://codecov.io/gh/yaitoo/sqle)
[![Go Report Card](https://goreportcard.com/badge/yaitoo/sqle)](http://goreportcard.com/report/yaitoo/sqle)

The SQLE package provides extensions to Go’s built-in `database/sql` package for more efficient, comprehensive interactions with databases in Go. The SQLE package is backward-compatible and extendable, so you can easily use it with the database/sql package.

The SQLE package takes the sql-first approach and provides functionalities for marshaling rows into struct, map,slice and golang primitive types.

The SQLE package also provides functionalities for schema auto migration, logging, contexts, prepared statements, advanced database operations like sharding, and much more.

You’ll find the SQLE package useful if you’re not a fan of full-featured ORMs and you prefer to use the database/sql package with extra support and functionalities.

## Features
- Works with any database engine(eg MySQL, Postgres, SQLite...etc) by [Go-MySQL-Driver](https://github.com/go-sql-driver/mysql/)
- [ORM-like](row_test.go) experience using good old SQL. SQLE supports structs, maps, primitive types, and
  slices of map/structs/primitive types.
- 100% compatible drop-in replacement of "database/sql". Code is really easy to migrate from `database/sql` to `SQLE`. see [examples](row_test.go)
- [Migration](migrate/migrator_test.go)
- Table AutoRotation 
- Database AutoSharding

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


### Table Rotate
use `sharding.ID` to enable rotate feature for a table based on option (None/Monthly/Weekly/Daily)

```
gen := sharding.New(WithTableRotate(sharing.Daily))
id := gen.Next()

b := New().On(id) //call `On` to enable rotate feature, and setup a input variable <rotate>
b.Delete("orders<rotate>").Where().
    If(true).And("order_id = {order_id}").
    If(false).And("member_id").
    Param("order_id", "order_123456")


db.ExecBuilder(context.TODO(),b) //DELETE FROM `orders_20240220` WHERE order_id = ?
```
see more [examples](sqlbuilder_test.go#L490)


### Database sharding
use `sharding.ID` to enable auto-sharding for any sql
```
gen := sharding.New(WithDatabase(10))
id := gen.Next()

b := New().On(id) //call `On` to enable rotate feature, and setup a input variable <rotate>
b.Delete("orders<rotate>").Where().
    If(true).And("order_id = {order_id}").
    If(false).And("member_id").
    Param("order_id", "order_123456")


db.On(id). //automatically select database based on `id.DatabaseID`
 ExecBuilder(context.TODO(),b) //DELETE FROM `orders` WHERE order_id = ?

```

see more [examples](db_test.go#L49)