package sqle

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
	"github.com/yaitoo/sqle/shardid"
)

func createSQLite3() *sql.DB {
	// f, err := os.CreateTemp(".", "*.db")
	// f.Close()

	// clean := func() {
	// 	os.Remove(f.Name()) //nolint
	// }

	// if err != nil {
	// 	return nil, clean, err
	// }

	// db, err := sql.Open("sqlite3", "file:"+f.Name()+"?cache=shared")
	db, err := sql.Open("sqlite3", "file::memory:")

	if err != nil {
		return nil
	}
	// https://github.com/mattn/go-sqlite3/issues/209
	// db.SetMaxOpenConns(1)
	return db
}

func TestOn(t *testing.T) {
	dbs := make([]*sql.DB, 0, 10)

	for i := 0; i < 10; i++ {
		db3 := createSQLite3()

		db3.Exec("CREATE TABLE `users` (`id` bigint , `status` tinyint,`email` varchar(50),`passwd` varchar(120), `salt` varchar(45), `created` DATETIME, PRIMARY KEY (`id`))") //nolint: errcheck

		dbs = append(dbs, db3)
	}

	db := Open(dbs...)
	gen := shardid.New(shardid.WithDatabase(10))

	ids := make([]shardid.ID, 10)

	for i := 0; i < 10; i++ {
		id := gen.Next()
		b := New().On(id).
			Insert("users").
			Set("id", id.Int64).
			Set("status", 1).
			Set("created", time.Now()).
			End()
		result, err := db.On(id).ExecBuilder(context.TODO(), b)

		require.NoError(t, err)
		rows, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)

		ids[i] = id
	}

	for i, id := range ids {
		b := New().On(id).Select("users", "id")

		ctx := db.On(id)

		require.Equal(t, i, ctx.index)

		var userID int64
		err := ctx.QueryRowBuilder(context.TODO(), b).Scan(&userID)
		require.NoError(t, err)
		require.Equal(t, id.Int64, userID)
	}

}

func TestDHT(t *testing.T) {
	db := Open(createSQLite3())

	// always work if only single server
	ctx, err := db.OnDHT("")
	require.Equal(t, 0, ctx.index)
	require.Nil(t, err)

	// MUST NOT panic even DHT is missing
	db.DHTAdd(1)
	db.DHTAdded()

	db.Add(createSQLite3())

	ctx, err = db.OnDHT("")
	require.ErrorIs(t, err, ErrMissingDHT)
	require.Nil(t, ctx)
}

func TestOnDHT(t *testing.T) {
	dbs := make([]*sql.DB, 0, 10)

	for i := 0; i < 10; i++ {
		db3 := createSQLite3()

		db3.Exec("CREATE TABLE `dht` (`v` varchar(50), PRIMARY KEY (`v`))") // nolint: errcheck

		dbs = append(dbs, db3)
	}

	db := Open(dbs...)

	//	2 dbs   ->   3 dbs  -> data
	//  -> 2439456            1149
	// 46916880 E0 0  !
	// 63694499 E1 1
	//	<-	80472118 E2 2
	//	<-  84017712 S2 2
	//  ->  111074370         638
	// 117572950 S0 0 !
	// 134350569 S1 1
	//	<-  214987260 G2 2
	// 248542498 G0 0 !
	// 265320117 G1 1
	// 316638712 M0 0
	// 333416331 M1 1
	//	<-  350193950 M2 2
	//	<-  351179688 K2 2
	// 384734926 K0 0 !
	// 401512545 K1 1
	//	<-  484709092 O2 2
	// 518264330 O0 0 !
	// 535041949 O1 1
	//	<-  2228889920 C2 2
	// 2262445158 C0 0 !
	// 2279222777 C1 1
	// 2330541372 I0 0
	// 2347318991 I1 1
	//	<-  2364096610 I2 2
	// 2597703348 A0 0 !
	// 2600263204 Q0 0
	// 2614480967 A1 1
	// 2617040823 Q1 1
	//	<-  2631258586 A2 2
	//	<-  2633818442 Q2 2
	//  -> 4113327457          150

	db.NewDHT(1, 2)

	values := map[string]int{
		"1149": 1,
		"S0":   2,
		"I2":   1,
	}

	for v, i := range values {

		b := New().Insert("dht").
			Set("v", v).
			End()

		c, err := db.OnDHT(v)
		require.NoError(t, err)
		require.Equal(t, i, c.index)

		result, err := c.ExecBuilder(context.TODO(), b)

		require.NoError(t, err)
		rows, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rows)
	}

	for v, i := range values {
		b := New().Select("dht", "v").Where("v = {v}").Param("v", v)

		ctx := db.dbs[i]

		var val string
		err := ctx.QueryRowBuilder(context.TODO(), b).Scan(&val)
		require.NoError(t, err)
		require.Equal(t, v, val)
	}
}

func TestDHTScaling(t *testing.T) {
	dbs := make([]*sql.DB, 0, 10)

	for i := 0; i < 10; i++ {
		db3 := createSQLite3()

		db3.Exec("CREATE TABLE `dht` (`v` varchar(50), PRIMARY KEY (`v`))") // nolint: errcheck

		dbs = append(dbs, db3)
	}

	db := Open(dbs...)

	//	2 dbs   ->   3 dbs  -> data
	//  -> 2439456            1149
	// 46916880 E0 0  !
	// 63694499 E1 1
	//	<-	80472118 E2 2
	//  ->  83143427          3850
	//	<-  84017712 S2 2
	//  ->  111074370         638
	// 117572950 S0 0 !
	// 134350569 S1 1
	//	<-  214987260 G2 2
	// 248542498 G0 0 !
	// 265320117 G1 1
	// 316638712 M0 0
	// 333416331 M1 1
	//	<-  350193950 M2 2
	//	<-  351179688 K2 2
	// 384734926 K0 0 !
	// 401512545 K1 1
	//	<-  484709092 O2 2
	// 518264330 O0 0 !
	// 535041949 O1 1
	//	<-  2228889920 C2 2
	// 2262445158 C0 0 !
	// 2279222777 C1 1
	// 2330541372 I0 0
	// 2347318991 I1 1
	//	<-  2364096610 I2 2
	// 2597703348 A0 0 !
	// 2600263204 Q0 0
	// 2614480967 A1 1
	// 2617040823 Q1 1
	//	<-  2631258586 A2 2
	//	<-  2633818442 Q2 2
	//  -> 4113327457          150

	db.NewDHT(0, 1)

	type item struct {
		current int
		busy    bool
		next    int
	}

	values := make(map[string]item)

	values["1149"] = item{current: 0, busy: false, next: 0} // Busy
	values["E1"] = item{current: 0, busy: true, next: 2}    // move from S0 to E2
	values["3850"] = item{current: 0, busy: true, next: 2}  // move from S0 to S2
	values["638"] = item{current: 0, busy: false, next: 0}  // keep on S0
	values["150"] = item{current: 0, busy: false, next: 0}  // keep on E0

	for v, it := range values {
		ctx, err := db.OnDHT(v)
		require.NoError(t, err)
		require.Equal(t, it.current, ctx.index)
	}

	db.DHTAdd(2)

	for v, it := range values {
		ctx, err := db.OnDHT(v)
		if it.busy {
			require.ErrorIs(t, err, shardid.ErrItemIsBusy)
		} else {
			require.NoError(t, err)
			require.Equal(t, it.current, ctx.index)
		}

	}

	db.DHTAdded()
	for v, it := range values {
		ctx, err := db.OnDHT(v)
		require.NoError(t, err)
		require.Equal(t, it.next, ctx.index)
	}
}
