package sqle

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type NullStr struct {
	String string
	Valid  bool // Valid is true if String is not NULL
}

// Scan implements the Scanner interface.
func (ns *NullStr) Scan(value any) error {
	if value == nil {
		ns.String, ns.Valid = "", false
		return nil
	}

	sv, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}

	if s, ok := sv.(string); ok {
		ns.Valid = true
		ns.String = s
		return nil
	}

	// otherwise, return an error
	return errors.New("failed to scan string")
}

type customBinder struct {
	UserID int
	Status int
	Email  string
}

func (cb *customBinder) Bind(_ reflect.Value, columns []string) []any {
	var missed any
	values := make([]any, 0, len(columns))
	for _, col := range columns {
		switch col {
		case "id":
			values = append(values, &cb.UserID)
		case "status":
			values = append(values, &cb.Status)
		case "email":
			values = append(values, &cb.Email)
		default:
			values = append(values, &missed)
		}
	}

	return values
}

func TestRow(t *testing.T) {

	d, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `users` (`id` int , `status` tinyint,`email` varchar(50),`passwd` varchar(120), `salt` varchar(45), `created` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	now := time.Now()

	_, err = d.Exec("INSERT INTO `users`(`id`,`status`,`email`,`passwd`,`salt`,`created`) VALUES(1, 1,'test1@mail.com','1xxxx','1zzzz', ?)", now)
	require.NoError(t, err)
	_, err = d.Exec("INSERT INTO `users`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(2, 2,'test2@mail.com','2xxxx','2zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `users`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(3, 3,'test3@mail.com','3xxxx','3zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `users`(`id`, `email`) VALUES(4, 'test4@mail.com')")
	require.NoError(t, err)

	db := Open(d)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "close_should_always_work",
			run: func(*testing.T) {
				var row *Row
				row.Close()
				row = &Row{}
				row.Close()
			},
		},
		{
			name: "bind_only_work_with_non_nil_pointer",
			run: func(t *testing.T) {

				row := &Row{}
				var dest int
				err := row.Bind(dest)
				require.ErrorIs(t, err, ErrMustPointer)

				var dest2 *int
				err = row.Bind(dest2)
				require.ErrorIs(t, err, ErrMustNotNilPointer)
			},
		},
		{
			name: "full_columns_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID      uint
					Status  int
					Email   string
					Passwd  string
					Salt    string
					Created *time.Time
				}

				row := db.QueryRow("SELECT * FROM users WHERE id=?", 1)
				var u user
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, uint(1), u.ID)
				require.Equal(t, 1, u.Status)
				require.Equal(t, "test1@mail.com", u.Email)
				require.Equal(t, "1xxxx", u.Passwd)
				require.Equal(t, "1zzzz", u.Salt)

				var u2 user
				row2 := db.QueryRow("SELECT * FROM users WHERE id=?", 2)
				err = row2.Bind(&u2)
				require.NoError(t, err)

				require.Equal(t, uint(2), u2.ID)
				require.Equal(t, 2, u2.Status)
				require.Equal(t, "test2@mail.com", u2.Email)
				require.Equal(t, "2xxxx", u2.Passwd)
				require.Equal(t, "2zzzz", u2.Salt)

			},
		},
		{
			name: "missed_column_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID     uint
					Status int
					Email  string
					Passwd string
					Salt   string
				}

				row := db.QueryRow("SELECT id,status,email,salt FROM users WHERE id=?", 1)
				var u user
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, uint(1), u.ID)
				require.Equal(t, 1, u.Status)
				require.Equal(t, "test1@mail.com", u.Email)
				require.Equal(t, "", u.Passwd)
				require.Equal(t, "1zzzz", u.Salt)

			},
		},
		{
			name: "missed_field_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID     uint
					Status int
					Salt   string
				}

				row := db.QueryRow("SELECT * FROM users WHERE id=?", 1)
				var u user
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, uint(1), u.ID)
				require.Equal(t, 1, u.Status)
				require.Equal(t, "1zzzz", u.Salt)
			},
		},
		{
			name: "alias_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID       uint
					Status   int
					Password string `db:"passwd"`
				}

				row := db.QueryRow("SELECT * FROM users WHERE id=?", 1)
				var u user
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, uint(1), u.ID)
				require.Equal(t, 1, u.Status)
				require.Equal(t, "1xxxx", u.Password)
			},
		},
		{
			name: "bind_is_case_insensitive",
			run: func(t *testing.T) {
				type user struct {
					ID       uint
					Status   int
					Password string `db:"passwd"`
				}

				row := db.QueryRow("SELECT id,StaTus,PassWD FROM users WHERE id=?", 1)
				var u user
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, uint(1), u.ID)
				require.Equal(t, 1, u.Status)
				require.Equal(t, "1xxxx", u.Password)
			},
		},
		{
			name: "hyphen_in_column_should_work",
			run: func(t *testing.T) {
				type user struct {
					UserID uint
					Status int
					Passwd string
				}

				row := db.QueryRow("SELECT id as user_id,StaTus,PassWD  FROM users WHERE id=?", 1)
				var u user
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, uint(1), u.UserID)
				require.Equal(t, 1, u.Status)
				require.Equal(t, "1xxxx", u.Passwd)
			},
		},
		{
			name: "bind_map_should_work",
			run: func(t *testing.T) {

				row := db.QueryRow("SELECT id,status,passwd FROM users WHERE id=?", 1)
				u := make(map[string]interface{})
				err := row.Bind(&u)
				require.NoError(t, err)

				require.EqualValues(t, uint(1), u["id"])
				require.EqualValues(t, 1, u["status"])
				require.Equal(t, "1xxxx", u["passwd"])

				row = db.QueryRow("SELECT id,status FROM users WHERE id=?", 2)
				u2 := make(map[string]int)
				err = row.Bind(&u2)
				require.NoError(t, err)

				require.Equal(t, 2, u2["id"])
				require.Equal(t, 2, u2["status"])

				row = db.QueryRow("SELECT id,status FROM users WHERE id=?", 3)
				u3 := make(map[string]string)
				err = row.Bind(&u3)
				require.NoError(t, err)

				require.Equal(t, "3", u3["id"])
				require.Equal(t, "3", u3["status"])

			},
		},
		{
			name: "bind_scanner_should_work",
			run: func(t *testing.T) {

				row := db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var b sql.NullBool
				err := row.Bind(&b)
				require.NoError(t, err)

				require.False(t, b.Bool)
				require.False(t, b.Valid)

				row = db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var i16 sql.NullInt16
				err = row.Bind(&i16)
				require.NoError(t, err)

				require.EqualValues(t, 0, i16.Int16)
				require.False(t, i16.Valid)

				row = db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var i32 sql.NullInt32
				err = row.Bind(&i32)
				require.NoError(t, err)

				require.EqualValues(t, 0, i32.Int32)
				require.False(t, i32.Valid)

				row = db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var i64 sql.NullInt64
				err = row.Bind(&i64)
				require.NoError(t, err)

				require.EqualValues(t, 0, i64.Int64)
				require.False(t, i64.Valid)

				row = db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var f64 sql.NullFloat64
				err = row.Bind(&f64)
				require.NoError(t, err)

				require.EqualValues(t, 0, f64.Float64)
				require.False(t, f64.Valid)

				row = db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var nt sql.NullTime
				err = row.Bind(&nt)
				require.NoError(t, err)

				require.Equal(t, time.Time{}, nt.Time)
				require.False(t, nt.Valid)

				row = db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var str sql.NullString
				err = row.Bind(&str)
				require.NoError(t, err)

				require.Equal(t, "", str.String)
				require.False(t, str.Valid)

				row = db.QueryRow("SELECT status FROM users WHERE id=?", 4)
				var ns NullStr
				err = row.Bind(&ns)
				require.NoError(t, err)

				require.Equal(t, "", ns.String)
				require.False(t, ns.Valid)

			},
		},
		{
			name: "bind_primitive_types_should_work",
			run: func(t *testing.T) {

				row := db.QueryRow("SELECT id FROM users WHERE id=?", 2)
				var i int
				err := row.Bind(&i)
				require.NoError(t, err)

				require.Equal(t, 2, i)

				row = db.QueryRow("SELECT id FROM users WHERE id=?", 2)
				var str string
				err = row.Bind(&str)
				require.NoError(t, err)

				require.Equal(t, "2", str)

				row = db.QueryRow("SELECT created FROM users WHERE id=?", 1)
				var tm time.Time
				err = row.Bind(&tm)
				require.NoError(t, err)

				require.Equal(t, now.UnixNano(), tm.UnixNano())

			},
		},

		{
			name: "bind_null_column_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID      int
					Status  *int
					Email   *string
					Passwd  *string
					Salt    *string
					Created *time.Time

					NullStatus  sql.NullInt32
					NullEmail   sql.NullString
					NullPasswd  sql.NullString
					NullSalt    sql.NullString
					NullCreated sql.NullTime
				}

				row := db.QueryRow(`SELECT id, status, email, passwd, salt, created, 
				status as null_status, email as null_email, passwd as null_passwd, salt as null_salt, created as null_created
				FROM users WHERE id=?`, 4)
				var u user
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, 4, u.ID)
				require.Nil(t, u.Status)
				require.NotNil(t, u.Email)
				require.Equal(t, "test4@mail.com", *u.Email)
				require.Nil(t, u.Passwd)
				require.Nil(t, u.Salt)
				require.Nil(t, u.Created)

				require.False(t, u.NullStatus.Valid)
				require.True(t, u.NullEmail.Valid)
				require.Equal(t, "test4@mail.com", u.NullEmail.String)
				require.False(t, u.NullPasswd.Valid)
				require.False(t, u.NullSalt.Valid)
				require.False(t, u.NullCreated.Valid)

			},
		},

		{
			name: "bind_custom_binder_should_work",
			run: func(t *testing.T) {

				row := db.QueryRow("SELECT id,status,email,salt FROM users WHERE id=?", 1)
				var u customBinder
				err := row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, 1, u.UserID)
				require.Equal(t, 1, u.Status)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t)
		})
	}

}
