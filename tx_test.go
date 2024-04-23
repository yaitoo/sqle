package sqle

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTx(t *testing.T) {
	d := createSQLite3()

	_, err := d.Exec("CREATE TABLE `users` (`id` int , `status` tinyint,`email` varchar(50),`passwd` varchar(120), `salt` varchar(45), `created` DATETIME, PRIMARY KEY (`id`))")
	require.NoError(t, err)

	now := time.Now()

	_, err = d.Exec("INSERT INTO `users`(`id`,`status`,`email`,`passwd`,`salt`,`created`) VALUES(1, 1,'test1@mail.com','1xxxx','1zzzz', ?)", now)
	require.NoError(t, err)
	_, err = d.Exec("INSERT INTO `users`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(2, 2,'test2@mail.com','2xxxx','2zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `users`(`id`,`status`,`email`,`passwd`,`salt`) VALUES(3, 3,'test3@mail.com','3xxxx','3zzzz')")
	require.NoError(t, err)

	_, err = d.Exec("INSERT INTO `users`(`id`) VALUES(4)")
	require.NoError(t, err)

	db := Open(d)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "query_should_work_in_tx",
			run: func(t *testing.T) {
				type user struct {
					ID      int
					Status  int
					Email   string
					Passwd  string
					Salt    string
					Created *time.Time
				}

				tx, err := db.Begin(nil)
				require.NoError(t, err)

				rows, err := tx.Query("SELECT * FROM users WHERE id<4")
				require.NoError(t, err)

				var users []user
				err = rows.Bind(&users)
				require.NoError(t, err)
				require.Len(t, users, 3)
				require.Equal(t, 1, users[0].ID)
				require.Equal(t, 2, users[1].ID)
				require.Equal(t, 3, users[2].ID)

				users2 := make([]user, 0, 3)
				rows, err = tx.QueryBuilder(context.Background(), New("SELECT * FROM users WHERE id<{id}").Param("id", 4))
				require.NoError(t, err)
				err = rows.Bind(&users2)
				require.NoError(t, err)
				require.Len(t, users2, 3)
				require.Equal(t, 1, users2[0].ID)
				require.Equal(t, 2, users2[1].ID)
				require.Equal(t, 3, users2[2].ID)

				err = tx.Commit()
				require.NoError(t, err)
			},
		},
		{
			name: "query_row_should_work_in_tx",
			run: func(t *testing.T) {
				type user struct {
					ID      uint
					Status  int
					Email   string
					Passwd  string
					Salt    string
					Created *time.Time
				}

				tx, err := db.Begin(nil)

				require.NoError(t, err)

				row := tx.QueryRow("SELECT * FROM users WHERE id=?", 1)
				var u user
				err = row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, uint(1), u.ID)
				require.Equal(t, 1, u.Status)
				require.Equal(t, "test1@mail.com", u.Email)
				require.Equal(t, "1xxxx", u.Passwd)
				require.Equal(t, "1zzzz", u.Salt)

				var u2 user
				row2 := tx.QueryRowBuilder(context.Background(), New("SELECT * FROM users WHERE id={id}").Param("id", 2))
				err = row2.Bind(&u2)
				require.NoError(t, err)

				require.Equal(t, uint(2), u2.ID)
				require.Equal(t, 2, u2.Status)
				require.Equal(t, "test2@mail.com", u2.Email)
				require.Equal(t, "2xxxx", u2.Passwd)
				require.Equal(t, "2zzzz", u2.Salt)

				err = tx.Commit()
				require.NoError(t, err)
			},
		},

		{
			name: "exec_should_work_in_tx",
			run: func(t *testing.T) {
				type user struct {
					ID    int
					Email string
				}

				tx, err := db.Begin(nil)
				require.NoError(t, err)

				result, err := tx.ExecBuilder(context.TODO(), New().
					Insert("users").
					Set("id", 50).
					Set("email", "test50@mail.com").
					End())
				require.NoError(t, err)

				i, err := result.RowsAffected()
				require.NoError(t, err)
				require.Equal(t, int64(1), i)

				err = tx.Commit()
				require.NoError(t, err)

				row := db.QueryRow("SELECT * FROM users WHERE id=?", 50)
				var u user
				err = row.Bind(&u)
				require.NoError(t, err)

				require.Equal(t, 50, u.ID)
				require.Equal(t, "test50@mail.com", u.Email)

			},
		},
		{
			name: "transaction_should_work",
			run: func(t *testing.T) {
				type user struct {
					ID    int
					Email string
				}

				err := db.Transaction(context.TODO(), nil, func(ctx context.Context, tx *Tx) error {

					for i := 100; i < 110; i++ {
						_, err := tx.ExecBuilder(ctx, New().
							Insert("users").
							Set("id", i).
							Set("email", fmt.Sprintf("test%v@mail.com", i)).
							End())

						require.NoError(t, err)

						if err != nil {
							return err
						}
					}

					return nil

				})
				require.NoError(t, err)

				rows, err := db.Query("SELECT * FROM users WHERE id>=100")
				require.NoError(t, err)
				var users []user
				err = rows.Bind(&users)

				require.NoError(t, err)

				require.Len(t, users, 10)

				for i := 0; i < 10; i++ {
					require.Equal(t, 100+i, users[i].ID)
					require.Equal(t, fmt.Sprintf("test%v@mail.com", 100+i), users[i].Email)
				}

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.run(t)
		})
	}
}
