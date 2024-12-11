package sqle

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

func TestRowsBind(t *testing.T) {

	d, err := sql.Open("sqlite3", "file::memory:")
	require.NoError(t, err)

	_, err = d.Exec("CREATE TABLE `rows` (`id` int , `status` tinyint,`email` varchar(50),`passwd` varchar(120), `salt` varchar(45), `created` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	now := time.Now()

	_, err = d.Exec("INSERT INTO `rows`(`id`,`status`,`email`,`passwd`,`salt`,`created`) VALUES(1, 1,'test1@mail.com','1xxxx','1zzzz', ?)", now)
	require.NoError(t, err)
	_, err = d.Exec("INSERT INTO `rows`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(2, 2,'test2@mail.com','2xxxx','2zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `rows`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(3, 3,'test3@mail.com','3xxxx','3zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `rows`(`id`) VALUES(4)")
	require.NoError(t, err)

	db := Open(d)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "scan_on_rows_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID      int
					Status  int
					Email   string
					Passwd  string
					Salt    string
					Created *time.Time
				}
				rows, err := db.Query("SELECT id,email FROM rows WHERE id<4")
				require.NoError(t, err)

				var id int
				var email string

				err = rows.Scan(&id, &email)
				require.NoError(t, err)
				require.Equal(t, 1, id)
				require.Equal(t, "test1@mail.com", email)

				err = rows.Scan(&id, &email)
				require.NoError(t, err)
				require.Equal(t, 2, id)
				require.Equal(t, "test2@mail.com", email)

				err = rows.Scan(&id, &email)
				require.NoError(t, err)
				require.Equal(t, 3, id)
				require.Equal(t, "test3@mail.com", email)

			},
		},
		{
			name: "bind_slice_of_struct_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID      int
					Status  int
					Email   string
					Passwd  string
					Salt    string
					Created *time.Time
				}
				rows, err := db.Query("SELECT * FROM rows WHERE id<4")
				require.NoError(t, err)

				var users []user
				err = rows.Bind(&users)
				require.NoError(t, err)
				require.Len(t, users, 3)
				require.Equal(t, 1, users[0].ID)
				require.Equal(t, 2, users[1].ID)
				require.Equal(t, 3, users[2].ID)

				users2 := make([]user, 0, 3)
				rows, err = db.Query("SELECT * FROM rows WHERE id<4")
				require.NoError(t, err)
				err = rows.Bind(&users2)
				require.NoError(t, err)
				require.Len(t, users2, 3)
				require.Equal(t, 1, users2[0].ID)
				require.Equal(t, 2, users2[1].ID)
				require.Equal(t, 3, users2[2].ID)
			},
		},
		{
			name: "bind_slice_of_map_should_work",
			run: func(t *testing.T) {

				rows, err := db.Query("SELECT * FROM rows WHERE id<4")
				require.NoError(t, err)

				var users []map[string]interface{}
				err = rows.Bind(&users)
				require.NoError(t, err)
				require.Len(t, users, 3)
				require.EqualValues(t, 1, users[0]["id"])
				require.EqualValues(t, 2, users[1]["id"])
				require.EqualValues(t, 3, users[2]["id"])

				rows, err = db.Query("SELECT * FROM rows WHERE id<4")
				require.NoError(t, err)
				var strUsers []map[string]interface{}
				err = rows.Bind(&strUsers)
				require.NoError(t, err)
				require.Len(t, users, 3)
				require.EqualValues(t, int64(1), strUsers[0]["id"])
				require.EqualValues(t, int64(2), strUsers[1]["id"])
				require.EqualValues(t, int64(3), strUsers[2]["id"])
			},
		},
		{
			name: "bind_slice_of_primitive_should_work",
			run: func(t *testing.T) {

				rows, err := db.Query("SELECT id,status FROM rows WHERE id<4")
				require.NoError(t, err)

				var users [][]int
				err = rows.Bind(&users)
				require.NoError(t, err)
				require.Len(t, users, 3)
				require.EqualValues(t, 1, users[0][0])
				require.EqualValues(t, 2, users[1][0])
				require.EqualValues(t, 3, users[2][0])

				var strUsers [][]string
				rows, err = db.Query("SELECT email, passwd FROM rows WHERE id<4")
				require.NoError(t, err)
				err = rows.Bind(&strUsers)
				require.NoError(t, err)

				require.Len(t, users, 3)
				require.EqualValues(t, "test1@mail.com", strUsers[0][0])
				require.EqualValues(t, "test2@mail.com", strUsers[1][0])
				require.EqualValues(t, "test3@mail.com", strUsers[2][0])

				require.EqualValues(t, "1xxxx", strUsers[0][1])
				require.EqualValues(t, "2xxxx", strUsers[1][1])
				require.EqualValues(t, "3xxxx", strUsers[2][1])
			},
		},
		{
			name: "bind_slice_of_scanner_should_work",
			run: func(t *testing.T) {

				rows, err := db.Query("SELECT id,null as status, email FROM rows WHERE id<4")
				require.NoError(t, err)

				var ns [][]NullStr
				err = rows.Bind(&ns)
				require.NoError(t, err)
				require.Len(t, ns, 3)
				require.EqualValues(t, "1", ns[0][0].String)
				require.EqualValues(t, "2", ns[1][0].String)
				require.EqualValues(t, "3", ns[2][0].String)

				require.False(t, ns[0][1].Valid)
				require.False(t, ns[1][1].Valid)
				require.False(t, ns[2][1].Valid)

				rows, err = db.Query("SELECT id,null as status, email FROM rows WHERE id<4")
				require.NoError(t, err)
				var users [][]sql.NullString
				err = rows.Bind(&users)
				require.NoError(t, err)
				require.Len(t, users, 3)
				require.EqualValues(t, "1", users[0][0].String)
				require.EqualValues(t, "2", users[1][0].String)
				require.EqualValues(t, "3", users[2][0].String)

				require.False(t, users[0][1].Valid)
				require.False(t, users[1][1].Valid)
				require.False(t, users[2][1].Valid)

			},
		},

		{
			name: "bind_slice_of_custom_binder_should_work",
			run: func(t *testing.T) {

				rows, err := db.Query("SELECT * FROM rows WHERE id<4")
				require.NoError(t, err)

				var users []customBinder
				err = rows.Bind(&users)
				require.NoError(t, err)
				require.Len(t, users, 3)
				require.EqualValues(t, 1, users[0].UserID)
				require.EqualValues(t, 2, users[1].UserID)
				require.EqualValues(t, 3, users[2].UserID)

				require.EqualValues(t, 1, users[0].Status)
				require.EqualValues(t, 2, users[1].Status)
				require.EqualValues(t, 3, users[2].Status)

				require.EqualValues(t, "test1@mail.com", users[0].Email)
				require.EqualValues(t, "test2@mail.com", users[1].Email)
				require.EqualValues(t, "test3@mail.com", users[2].Email)

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t)
		})
	}
}
