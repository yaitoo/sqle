package sqle

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	m202402 = time.Date(2024, 02, 01, 0, 0, 0, 0, time.UTC)

	m202403 = time.Date(2024, 03, 01, 0, 0, 0, 0, time.UTC)

	w20240201 = time.Date(2024, 02, 01, 0, 0, 0, 0, time.UTC)
	w20240208 = time.Date(2024, 02, 8, 0, 0, 0, 0, time.UTC)

	d20240201 = time.Date(2024, 02, 01, 0, 0, 0, 0, time.UTC)
	d20240202 = time.Date(2024, 02, 02, 0, 0, 0, 0, time.UTC)
)

type MRUser struct {
	ID int
}

func createSQLite3OnDisk() (*sql.DB, func(), error) {
	f, err := os.CreateTemp(".", "*.db")
	f.Close()

	clean := func() {
		os.Remove(f.Name()) //nolint
	}

	if err != nil {
		return nil, clean, err
	}

	db, err := sql.Open("sqlite3", "file:"+f.Name())

	if err != nil {
		return nil, clean, err
	}
	//https://github.com/mattn/go-sqlite3/issues/209
	// db.SetMaxOpenConns(1)
	return db, clean, nil
}

func createSQLites() ([]*sql.DB, func()) {
	dbs := make([]*sql.DB, 0, 10)
	var tasks []func()

	for i := 0; i < 10; i++ {
		db3, clean, _ := createSQLite3OnDisk()

		tasks = append(tasks, clean)

		db3.Exec("CREATE TABLE `users` (`id` int , PRIMARY KEY (`id`))") //nolint: errcheck
		db3.Exec("INSERT INTO `users` (`id`) VALUES (?)", i*10+1)        //nolint: errcheck
		db3.Exec("INSERT INTO `users` (`id`) VALUES (?)", i*10+2)        //nolint: errcheck
		db3.Exec("INSERT INTO `users` (`id`) VALUES (?)", i*10+3)        //nolint: errcheck
		db3.Exec("INSERT INTO `users` (`id`) VALUES (?)", i*10+4)        //nolint: errcheck

		db3.Exec("CREATE TABLE `users_202402` (`id` int , PRIMARY KEY (`id`))")   //nolint: errcheck
		db3.Exec("INSERT INTO `users_202402` (`id`) VALUES (?)", 20240200+i*10+1) //nolint: errcheck
		db3.Exec("INSERT INTO `users_202402` (`id`) VALUES (?)", 20240200+i*10+2) //nolint: errcheck
		db3.Exec("INSERT INTO `users_202402` (`id`) VALUES (?)", 20240200+i*10+3) //nolint: errcheck
		db3.Exec("INSERT INTO `users_202402` (`id`) VALUES (?)", 20240200+i*10+4) //nolint: errcheck

		db3.Exec("CREATE TABLE `users_202403` (`id` int , PRIMARY KEY (`id`))")   //nolint: errcheck
		db3.Exec("INSERT INTO `users_202403` (`id`) VALUES (?)", 20240300+i*10+1) //nolint: errcheck
		db3.Exec("INSERT INTO `users_202403` (`id`) VALUES (?)", 20240300+i*10+2) //nolint: errcheck
		db3.Exec("INSERT INTO `users_202403` (`id`) VALUES (?)", 20240300+i*10+3) //nolint: errcheck
		db3.Exec("INSERT INTO `users_202403` (`id`) VALUES (?)", 20240300+i*10+4) //nolint: errcheck

		db3.Exec("CREATE TABLE `users_2024005` (`id` int , PRIMARY KEY (`id`))")    //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024005` (`id`) VALUES (?)", 202400500+i*10+1) //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024005` (`id`) VALUES (?)", 202400500+i*10+2) //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024005` (`id`) VALUES (?)", 202400500+i*10+3) //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024005` (`id`) VALUES (?)", 202400500+i*10+4) //nolint: errcheck

		db3.Exec("CREATE TABLE `users_2024006` (`id` int , PRIMARY KEY (`id`))")    //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024006` (`id`) VALUES (?)", 202400600+i*10+1) //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024006` (`id`) VALUES (?)", 202400600+i*10+2) //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024006` (`id`) VALUES (?)", 202400600+i*10+3) //nolint: errcheck
		db3.Exec("INSERT INTO `users_2024006` (`id`) VALUES (?)", 202400600+i*10+4) //nolint: errcheck

		db3.Exec("CREATE TABLE `users_20240201` (`id` int , PRIMARY KEY (`id`))")     //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240201` (`id`) VALUES (?)", 2024020100+i*10+1) //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240201` (`id`) VALUES (?)", 2024020100+i*10+2) //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240201` (`id`) VALUES (?)", 2024020100+i*10+3) //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240201` (`id`) VALUES (?)", 2024020100+i*10+4) //nolint: errcheck

		db3.Exec("CREATE TABLE `users_20240202` (`id` int , PRIMARY KEY (`id`))")     //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240202` (`id`) VALUES (?)", 2024020200+i*10+1) //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240202` (`id`) VALUES (?)", 2024020200+i*10+2) //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240202` (`id`) VALUES (?)", 2024020200+i*10+3) //nolint: errcheck
		db3.Exec("INSERT INTO `users_20240202` (`id`) VALUES (?)", 2024020200+i*10+4) //nolint: errcheck

		dbs = append(dbs, db3)
	}

	return dbs, func() {
		for _, task := range tasks {
			task()
		}
	}
}

func TestFirst(t *testing.T) {

	dbs, clean := createSQLites()
	defer clean()

	db := Open(dbs...)

	tests := []struct {
		name    string
		wanted  MRUser
		wantErr error
		query   func() *Query[MRUser]
		first   func(q *Query[MRUser]) (MRUser, error)
	}{
		{
			name: "1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithQueryer[MRUser](&MapR[MRUser]{
					dbs: db.dbs,
				}))
			},
			wanted: MRUser{ID: 2},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users", "id").
					Where("id = 2").End())
			},
		},
		{
			name: "3rd_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db)
			},
			wanted: MRUser{ID: 31},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users", "id").
					Where("id = 31").End())
			},
		},
		{
			name: "last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db)
			},
			wanted: MRUser{ID: 94},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users", "id").
					Where("id = 94").End())
			},
		},
		{
			name: "month_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: MRUser{ID: 20240204},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 20240204").End())
			},
		},
		{
			name: "month_on_6th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: MRUser{ID: 20240354},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 20240354").End())
			},
		},
		{
			name: "month_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: MRUser{ID: 20240394},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 20240394").End())
			},
		},
		{
			name: "week_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: MRUser{ID: 202400504},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 202400504").End())
			},
		},
		{
			name: "week_on_5th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: MRUser{ID: 202400654},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 202400654").End())
			},
		},
		{
			name: "week_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: MRUser{ID: 202400694},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 202400694").End())
			},
		},
		{
			name: "day_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: MRUser{ID: 2024020104},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 2024020104").End())
			},
		},
		{
			name: "day_on_5th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: MRUser{ID: 2024020154},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 2024020154").End())
			},
		},
		{
			name: "day_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: MRUser{ID: 2024020294},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 2024020294").End())
			},
		},
		{
			name: "day_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: MRUser{ID: 2024020294},
			first: func(q *Query[MRUser]) (MRUser, error) {
				return q.First(context.Background(), New().
					Select("users<rotate>", "id").
					Where("id = 2024020294").End())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			i, err := test.first(test.query())

			require.Equal(t, test.wanted, i)
			require.Equal(t, test.wantErr, err)
		})
	}

}

func TestCount(t *testing.T) {
	dbs, clean := createSQLites()
	defer clean()

	db := Open(dbs...)

	tests := []struct {
		name    string
		wanted  int
		wantErr error
		query   func() *Query[int]
		count   func(q *Query[int]) (int, error)
	}{
		{
			name: "1st_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithQueryer[int](&MapR[int]{
					dbs: db.dbs,
				}))
			},
			wanted: 3,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users", "count(id)").
					Where("id < 4").
					End())
			},
		},
		{
			name: "3_dbs_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db)
			},
			wanted: 11,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users", "count(id)").
					Where("id < 24").End())
			},
		},
		{
			name: "all_dbs_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db)
			},
			wanted: 40,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users", "count(id)"))
			},
		},
		{
			name: "month_on_1st_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithMonths[int](m202402, m202403))
			},
			wanted: 7,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("( id > 20240200 AND id < 20240204) OR ( id >= 20240300 AND id < 20240305)").End())
			},
		},
		{
			name: "month_on_6th_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithMonths[int](m202402, m202403))
			},
			wanted: 7,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 20240250 AND id < 20240254) OR ( id >= 20240350 AND id < 20240355)").End())
			},
		},
		{
			name: "month_on_last_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithMonths[int](m202402, m202403))
			},
			wanted: 7,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 20240290 AND id < 20240294) OR ( id >= 20240390 AND id < 20240395)").End())
			},
		},
		{
			name: "week_on_1st_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithWeeks[int](w20240201, w20240208))
			},
			wanted: 6,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 202400500 AND id < 202400504) OR ( id >= 202400600 AND id < 202400604)").End())
			},
		},
		{
			name: "week_on_5th_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithWeeks[int](w20240201, w20240208))
			},
			wanted: 6,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 202400550 AND id < 202400554) OR ( id >= 202400650 AND id < 202400654)").End())
			},
		},
		{
			name: "week_on_last_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithWeeks[int](w20240201, w20240208))
			},
			wanted: 6,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 202400590 AND id < 202400594) OR ( id >= 202400690 AND id < 202400694)").End())
			},
		},
		{
			name: "day_on_1st_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithDays[int](d20240201, d20240202))
			},
			wanted: 6,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 2024020100 AND id < 2024020104) OR ( id > 2024020200 AND id < 2024020204)").End())
			},
		},
		{
			name: "day_on_5th_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithDays[int](d20240201, d20240202))
			},
			wanted: 6,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 2024020150 AND id < 2024020154) OR ( id > 2024020250 AND id < 2024020254)").End())
			},
		},
		{
			name: "day_on_last_db_should_work",
			query: func() *Query[int] {
				return NewQuery[int](db, WithDays[int](d20240201, d20240202))
			},
			wanted: 6,
			count: func(q *Query[int]) (int, error) {
				return q.Count(context.Background(), New().
					Select("users<rotate>", "count(id)").
					Where("(id > 2024020190 AND id < 2024020194) OR ( id > 2024020290 AND id < 2024020294)").End())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			i, err := test.count(test.query())

			require.Equal(t, test.wanted, i)
			require.Equal(t, test.wantErr, err)
		})
	}
}

func TestQuery(t *testing.T) {
	dbs, clean := createSQLites()
	defer clean()

	db := Open(dbs...)

	tests := []struct {
		name      string
		wanted    []MRUser
		wantErr   error
		query     func() *Query[MRUser]
		queryRows func(q *Query[MRUser]) ([]MRUser, error)
	}{
		{
			name: "1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithQueryer[MRUser](&MapR[MRUser]{
					dbs: db.dbs,
				}))
			},
			wanted: []MRUser{
				{ID: 1},
				{ID: 2},
				{ID: 3}},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users", "id").
					Where("id < 4").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "3_dbs_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db)
			},
			wanted: []MRUser{{ID: 1},
				{ID: 2}, {ID: 3},
				{ID: 4}, {ID: 11},
				{ID: 12}, {ID: 13},
				{ID: 14}, {ID: 21},
				{ID: 22}, {ID: 23},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users", "id").
					Where("id < 24").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "all_dbs_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db)
			},
			wanted: []MRUser{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4},
				{ID: 11}, {ID: 12}, {ID: 13}, {ID: 14},
				{ID: 21}, {ID: 22}, {ID: 23}, {ID: 24},
				{ID: 31}, {ID: 32}, {ID: 33}, {ID: 34},
				{ID: 41}, {ID: 42}, {ID: 43}, {ID: 44},
				{ID: 51}, {ID: 52}, {ID: 53}, {ID: 54},
				{ID: 61}, {ID: 62}, {ID: 63}, {ID: 64},
				{ID: 71}, {ID: 72}, {ID: 73}, {ID: 74},
				{ID: 81}, {ID: 82}, {ID: 83}, {ID: 84},
				{ID: 91}, {ID: 92}, {ID: 93}, {ID: 94}},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users", "id").
					Write("ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "month_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: []MRUser{
				{ID: 20240201}, {ID: 20240202}, {ID: 20240203},
				{ID: 20240301}, {ID: 20240302}, {ID: 20240303}, {ID: 20240304},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("( id > 20240200 AND id < 20240204) OR ( id >= 20240300 AND id < 20240305)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "month_on_6th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: []MRUser{
				{ID: 20240251}, {ID: 20240252}, {ID: 20240253},
				{ID: 20240351}, {ID: 20240352}, {ID: 20240353}, {ID: 20240354},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 20240250 AND id < 20240254) OR ( id >= 20240350 AND id < 20240355)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "month_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: []MRUser{
				{ID: 20240291}, {ID: 20240292}, {ID: 20240293},
				{ID: 20240391}, {ID: 20240392}, {ID: 20240393}, {ID: 20240394},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 20240290 AND id < 20240294) OR ( id >= 20240390 AND id < 20240395)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "week_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: []MRUser{
				{ID: 202400501}, {ID: 202400502}, {ID: 202400503},
				{ID: 202400601}, {ID: 202400602}, {ID: 202400603},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 202400500 AND id < 202400504) OR ( id >= 202400600 AND id < 202400604)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "week_on_5th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: []MRUser{
				{ID: 202400551}, {ID: 202400552}, {ID: 202400553},
				{ID: 202400651}, {ID: 202400652}, {ID: 202400653},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 202400550 AND id < 202400554) OR ( id >= 202400650 AND id < 202400654)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "week_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: []MRUser{
				{ID: 202400591}, {ID: 202400592}, {ID: 202400593},
				{ID: 202400691}, {ID: 202400692}, {ID: 202400693},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 202400590 AND id < 202400594) OR ( id >= 202400690 AND id < 202400694)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "day_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: []MRUser{
				{ID: 2024020101}, {ID: 2024020102}, {ID: 2024020103},
				{ID: 2024020201}, {ID: 2024020202}, {ID: 2024020203},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 2024020100 AND id < 2024020104) OR ( id > 2024020200 AND id < 2024020204)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "day_on_5th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: []MRUser{
				{ID: 2024020151}, {ID: 2024020152}, {ID: 2024020153},
				{ID: 2024020251}, {ID: 2024020252}, {ID: 2024020253},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 2024020150 AND id < 2024020154) OR ( id > 2024020250 AND id < 2024020254)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
		{
			name: "day_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: []MRUser{
				{ID: 2024020191}, {ID: 2024020192}, {ID: 2024020193},
				{ID: 2024020291}, {ID: 2024020292}, {ID: 2024020293},
			},
			queryRows: func(q *Query[MRUser]) ([]MRUser, error) {
				return q.Query(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 2024020190 AND id < 2024020194) OR ( id > 2024020290 AND id < 2024020294)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			i, err := test.queryRows(test.query())

			require.Equal(t, test.wantErr, err)
			require.Equal(t, test.wanted, i)

		})
	}
}

func TestQueryLimit(t *testing.T) {
	dbs, clean := createSQLites()
	defer clean()

	db := Open(dbs...)

	tests := []struct {
		name       string
		wanted     []MRUser
		wantErr    error
		limit      int
		query      func() *Query[MRUser]
		queryLimit func(q *Query[MRUser], limit int) ([]MRUser, error)
	}{
		{
			name: "1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithQueryer[MRUser](&MapR[MRUser]{
					dbs: db.dbs,
				}))
			},
			wanted: []MRUser{
				{ID: 1},
				{ID: 2},
				// {ID: 3},
			},
			limit: 2,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users", "id").
					Where("id < 4").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
		{
			name: "3_dbs_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db)
			},
			wanted: []MRUser{
				// db1
				{ID: 1},
				{ID: 2},
				{ID: 3},
				{ID: 4},
				// db2
				{ID: 11},
				// {ID: 12},
				// {ID: 13},
				// {ID: 14},
				// db3
				// {ID: 21},
				// {ID: 22},
				// {ID: 23},
			},
			limit: 5,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users", "id").
					Where("id < 24").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
		{
			name: "all_dbs_desc_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db)
			},
			wanted: []MRUser{
				// {ID: 1}, {ID: 2}, {ID: 3}, {ID: 4},
				// {ID: 11}, {ID: 12}, {ID: 13}, {ID: 14},
				// {ID: 21}, {ID: 22}, {ID: 23}, {ID: 24},
				// {ID: 31}, {ID: 32}, {ID: 33}, {ID: 34},
				// {ID: 41}, {ID: 42}, {ID: 43}, {ID: 44},
				// {ID: 51}, {ID: 52}, {ID: 53}, {ID: 54},
				// {ID: 61}, {ID: 62}, {ID: 63}, {ID: 64},
				// {ID: 71}, {ID: 72}, {ID: 73}, {ID: 74},
				// {ID: 81}, {ID: 82}, {ID: 83}, {ID: 84},
				// {ID: 91}, {ID: 92}, {ID: 93}, {ID: 94},
				{94}, {93}, {92}, {91},
				{84}, {83}, {82}, {81},
				{74}, {73}, {72}, {71},
				{64}, {63}, {62}, {61},
			},
			limit: 16,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users", "id").
					Write("ORDER BY id DESC"), func(i, j MRUser) bool {
					// DESC
					return j.ID < i.ID
				}, 16)
			},
		},
		{
			name: "month_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: []MRUser{
				{ID: 20240201},
				{ID: 20240202},
				{ID: 20240203},
				{ID: 20240301},
				{ID: 20240302},
				// {ID: 20240303},
				// {ID: 20240304},
			},
			limit: 5,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("( id > 20240200 AND id < 20240204) OR ( id >= 20240300 AND id < 20240305)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
		{
			name: "month_on_6th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: []MRUser{
				{ID: 20240251},
				{ID: 20240252},
				{ID: 20240253},
				{ID: 20240351},
				{ID: 20240352},
				{ID: 20240353},
				// {ID: 20240354},
			},
			limit: 6,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 20240250 AND id < 20240254) OR ( id >= 20240350 AND id < 20240355)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
		{
			name: "month_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithMonths[MRUser](m202402, m202403))
			},
			wanted: []MRUser{
				{ID: 20240291},
				{ID: 20240292},
				{ID: 20240293},
				{ID: 20240391},
				{ID: 20240392},
				{ID: 20240393},
				{ID: 20240394},
			},
			limit: 8,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 20240290 AND id < 20240294) OR ( id >= 20240390 AND id < 20240395)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
		{
			name: "week_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: []MRUser{
				{ID: 202400501},
				{ID: 202400502},
				{ID: 202400503},
				{ID: 202400601},
				{ID: 202400602},
				{ID: 202400603},
			},
			limit: 6,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 202400500 AND id < 202400504) OR ( id >= 202400600 AND id < 202400604)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
		{
			name: "week_on_5th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: []MRUser{
				{ID: 202400551},
				{ID: 202400552},
				{ID: 202400553},
				{ID: 202400651},
				{ID: 202400652},
				// {ID: 202400653},
			},
			limit: 5,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 202400550 AND id < 202400554) OR ( id >= 202400650 AND id < 202400654)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, 5)
			},
		},
		{
			name: "week_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithWeeks[MRUser](w20240201, w20240208))
			},
			wanted: []MRUser{
				{ID: 202400591},
				{ID: 202400592},
				{ID: 202400593},
				{ID: 202400691},
				{ID: 202400692},
				{ID: 202400693},
			},
			limit: 8,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 202400590 AND id < 202400594) OR ( id >= 202400690 AND id < 202400694)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
		{
			name: "day_desc_on_1st_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: []MRUser{
				// {ID: 2024020101},
				// {ID: 2024020102},
				// {ID: 2024020103},
				// {ID: 2024020201},
				// {ID: 2024020202},
				{ID: 2024020203},
			},
			limit: 1,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 2024020100 AND id < 2024020104) OR ( id > 2024020200 AND id < 2024020204)").
					End().Write(" ORDER BY id DESC"), func(i, j MRUser) bool {
					return j.ID < i.ID
				}, limit)
			},
		},
		{
			name: "day_on_5th_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery(db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: []MRUser{
				{ID: 2024020151},
				{ID: 2024020152},
				{ID: 2024020153},
				{ID: 2024020251},
				// {ID: 2024020252},
				// {ID: 2024020253},
			},
			limit: 4,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 2024020150 AND id < 2024020154) OR ( id > 2024020250 AND id < 2024020254)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, 4)
			},
		},
		{
			name: "day_on_last_db_should_work",
			query: func() *Query[MRUser] {
				return NewQuery[MRUser](db, WithDays[MRUser](d20240201, d20240202))
			},
			wanted: []MRUser{
				{ID: 2024020191},
				{ID: 2024020192},
				{ID: 2024020193},
				// {ID: 2024020291},
				// {ID: 2024020292},
				// {ID: 2024020293},
			},
			limit: 3,
			queryLimit: func(q *Query[MRUser], limit int) ([]MRUser, error) {
				return q.QueryLimit(context.Background(), New().
					Select("users<rotate>", "id").
					Where("(id > 2024020190 AND id < 2024020194) OR ( id > 2024020290 AND id < 2024020294)").
					End().Write(" ORDER BY id"), func(i, j MRUser) bool {
					return i.ID < j.ID
				}, limit)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			i, err := test.queryLimit(test.query(), test.limit)

			require.Equal(t, test.wantErr, err)
			require.Equal(t, test.wanted, i)

		})
	}
}
