package sqle

import (
	"testing"
	"time"

	"github.com/iancoleman/strcase"
	"github.com/stretchr/testify/require"
	"github.com/yaitoo/sqle/shardid"
)

func TestBuilder(t *testing.T) {

	now := time.Now()

	tests := []struct {
		name   string
		build  func() *Builder
		assert func(t *testing.T, b *Builder)
	}{
		{
			name: "build_no_token",
			build: func() *Builder {
				b := New("SELECT * FROM orders")
				b.SQL(" WHERE created>=now()")
				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "SELECT * FROM orders WHERE created>=now()", s)
				require.Nil(t, vars)
			},
		},
		{
			name: "build_if",
			build: func() *Builder {
				b := New("SELECT * FROM orders")
				b.SQL(" WHERE created>=now()").
					If(true).SQL(" LIMIT 5").
					If(false).SQL(" OFFSET 5")
				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "SELECT * FROM orders WHERE created>=now() LIMIT 5", s)
				require.Nil(t, vars)
			},
		},
		{
			name: "build_with_input_tokens",
			build: func() *Builder {
				b := New("SELECT * FROM", "orders_<yyyyMM> as orders")
				b.SQL(" WHERE orders.created>=now()")
				b.Input("yyyyMM", "202401")
				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "SELECT * FROM orders_202401 as orders WHERE orders.created>=now()", s)
				require.Nil(t, vars)
			},
		},
		{
			name: "build_with_param_tokens",
			build: func() *Builder {
				b := New("SELECT * FROM orders")
				b.SQL(" WHERE cancelled>={now} and id={order_id} and created>={now}")
				b.Param("order_id", 123456)
				b.Param("now", now)
				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "SELECT * FROM orders WHERE cancelled>=? and id=? and created>=?", s)
				require.Len(t, vars, 3)
				require.Equal(t, now, vars[0])
				require.Equal(t, 123456, vars[1])
				require.Equal(t, now, vars[2])
			},
		},
		{
			name: "build_with_input_param_tokens",
			build: func() *Builder {
				b := New("SELECT * FROM orders_<yyyy> as orders LEFT JOIN users_<dbid>")
				b.SQL(" ON users_<dbid>.id=orders.user_id")
				b.SQL(" WHERE users_<dbid>.id={user_id} and orders.user_id={user_id} and orders.status={order_status} and orders.created>={now}")
				b.Inputs(map[string]string{
					"dbid": "db2",
					"yyyy": "2024",
				})

				b.Param("order_status", 1)
				b.Param("now", now)
				b.Param("user_id", "u123456")
				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "SELECT * FROM orders_2024 as orders LEFT JOIN users_db2 ON users_db2.id=orders.user_id WHERE users_db2.id=? and orders.user_id=? and orders.status=? and orders.created>=?", s)
				require.Len(t, vars, 4)
				require.Equal(t, "u123456", vars[0])
				require.Equal(t, "u123456", vars[1])
				require.Equal(t, 1, vars[2])
				require.Equal(t, now, vars[3])
			},
		},
		{
			name: "build_where",
			build: func() *Builder {
				b := New().Select("orders")
				b.Where("cancelled>={now}").
					If(true).SQL("AND", "id={order_id}")
				b.SQL(" AND created>={now}")
				b.Param("order_id", 123456)
				b.Param("now", now)
				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "SELECT * FROM `orders` WHERE cancelled>=? AND id=? AND created>=?", s)
				require.Len(t, vars, 3)
				require.Equal(t, now, vars[0])
				require.Equal(t, 123456, vars[1])
				require.Equal(t, now, vars[2])
			},
		},
		{
			name: "build_where_if",
			build: func() *Builder {

				var cancelledTime *time.Time
				orderID := 123456

				b := New().Select("orders")
				b.Where().
					If(orderID > -1).SQL("AND", "id={order_id}").
					If(cancelledTime != nil).SQL("AND", "cancelled>={cancelled_time}").
					And("created>={now}")

				b.Param("cancelled_time", cancelledTime)
				b.Param("order_id", orderID)
				b.Param("now", now)
				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "SELECT * FROM `orders` WHERE id=? AND created>=?", s)
				require.Len(t, vars, 2)

				require.Equal(t, 123456, vars[0])
				require.Equal(t, now, vars[1])
			},
		},
		{
			name: "build_update",
			build: func() *Builder {
				b := New()
				b.Update("orders").
					Set("member_id", 1234).
					Set("amount", 100).
					Set("created_time", now)

				b.Where("cancelled>={now}").
					If(true).SQL("AND", "id={order_id}")

				b.SQL(" AND created>={now}")
				b.Param("order_id", "order_123456")
				b.Param("now", now)

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "UPDATE `orders` SET `member_id`=?, `amount`=?, `created_time`=? WHERE cancelled>=? AND id=? AND created>=?", s)
				require.Len(t, vars, 6)
				require.Equal(t, 1234, vars[0])
				require.Equal(t, 100, vars[1])
				require.Equal(t, now, vars[2])
				require.Equal(t, now, vars[3])
				require.Equal(t, "order_123456", vars[4])
				require.Equal(t, now, vars[5])
			},
		},
		{
			name: "build_update_expr",
			build: func() *Builder {
				b := New()
				b.Update("orders").
					Set("member_id", 1234).
					SetExpr("`amount`=amount+1").
					Set("created_time", now)

				b.Where("cancelled>={now}").
					If(true).SQL("AND", "id={order_id}")

				b.SQL(" AND created>={now}")
				b.Param("order_id", "order_123456")
				b.Param("now", now)

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "UPDATE `orders` SET `member_id`=?, `amount`=amount+1, `created_time`=? WHERE cancelled>=? AND id=? AND created>=?", s)
				require.Len(t, vars, 5)
				require.Equal(t, 1234, vars[0])
				require.Equal(t, now, vars[1])
				require.Equal(t, now, vars[2])
				require.Equal(t, "order_123456", vars[3])
				require.Equal(t, now, vars[4])
			},
		},
		{
			name: "build_update_model",
			build: func() *Builder {
				type User struct {
					MemberID string
					Amount   int
					Created  time.Time `db:"created_time"`
					Alias    string    `db:"-"`
				}

				u := User{
					MemberID: "id123",
					Amount:   100,
					Created:  now,
					Alias:    "alias",
				}

				b := New()
				b.Update("orders").
					SetModel(u)
				b.Where("cancelled>={now}").
					If(true).SQL("AND", "id={order_id}")

				b.SQL(" AND created_time>={now}")
				b.Param("order_id", "order_123456")
				b.Param("now", now)

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "UPDATE `orders` SET `member_id`=?, `amount`=?, `created_time`=? WHERE cancelled>=? AND id=? AND created_time>=?", s)
				require.Len(t, vars, 6)
				require.Equal(t, "id123", vars[0])
				require.Equal(t, 100, vars[1])
				require.Equal(t, now, vars[2])
				require.Equal(t, now, vars[3])
				require.Equal(t, "order_123456", vars[4])
				require.Equal(t, now, vars[5])
			},
		},
		{
			name: "build_update_map",
			build: func() *Builder {
				m := map[string]any{
					"MemberID":     "id123",
					"Amount":       100,
					"Created_time": now,
					"alias":        "alias",
				}

				b := New()
				b.Update("orders").
					SetMap(m, WithToName(strcase.ToSnake), WithAllow("member_id", "amount", "created_time"))
				b.Where("cancelled>={now}").
					If(true).SQL("AND", "id={order_id}")

				b.SQL(" AND created_time>={now}")
				b.Param("order_id", "order_123456")
				b.Param("now", now)

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "UPDATE `orders` SET `member_id`=?, `amount`=?, `created_time`=? WHERE cancelled>=? AND id=? AND created_time>=?", s)
				require.Len(t, vars, 6)
				require.Equal(t, "id123", vars[0])
				require.Equal(t, 100, vars[1])
				require.Equal(t, now, vars[2])
				require.Equal(t, now, vars[3])
				require.Equal(t, "order_123456", vars[4])
				require.Equal(t, now, vars[5])
			},
		},
		{
			name: "build_update_if",
			build: func() *Builder {

				b := New()
				b.Update("orders").
					If(false).Set("member_id", 1234).
					Set("amount", 100).
					Set("created_time", now)

				b.Where("cancelled>={now}").
					If(true).SQL("AND", "id={order_id}")

				b.SQL(" AND created>={now}")
				b.Param("order_id", "order_123456")
				b.Param("now", now)

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "UPDATE `orders` SET `amount`=?, `created_time`=? WHERE cancelled>=? AND id=? AND created>=?", s)
				require.Len(t, vars, 5)

				require.Equal(t, 100, vars[0])
				require.Equal(t, now, vars[1])
				require.Equal(t, now, vars[2])
				require.Equal(t, "order_123456", vars[3])
				require.Equal(t, now, vars[4])
			},
		},
		{
			name: "build_insert",
			build: func() *Builder {
				b := New()
				b.Insert("orders").
					Set("order_id", "order_123456").
					Set("member_id", 1234).
					Set("amount", 100).
					Set("created_time", now).
					End()

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "INSERT INTO `orders` (`order_id`, `member_id`, `amount`, `created_time`) VALUES (?, ?, ?, ?)", s)
				require.Len(t, vars, 4)
				require.Equal(t, "order_123456", vars[0])
				require.Equal(t, 1234, vars[1])
				require.Equal(t, 100, vars[2])
				require.Equal(t, now, vars[3])

			},
		},
		{
			name: "build_insert_model",
			build: func() *Builder {
				type User struct {
					MemberID    string `db:"id"`
					Amount      int
					CreatedTime time.Time
					Alias       string `db:"-"`
				}

				u := User{
					MemberID:    "id123",
					Amount:      100,
					CreatedTime: now,
					Alias:       "alias",
				}

				b := New()
				b.Insert("orders").
					SetModel(&u).
					End()

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "INSERT INTO `orders` (`id`, `amount`, `created_time`) VALUES (?, ?, ?)", s)
				require.Len(t, vars, 3)
				require.Equal(t, "id123", vars[0])
				require.Equal(t, 100, vars[1])
				require.Equal(t, now, vars[2])

			},
		},
		{
			name: "build_insert_map",
			build: func() *Builder {
				m := map[string]any{
					"ID":          "id123",
					"Amount":      100,
					"CreatedTime": now,
					"Alias":       "alias",
				}

				b := New()
				b.Insert("orders").
					SetMap(m, WithToName(strcase.ToSnake), WithAllow("id", "amount", "created_time")).
					End()

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "INSERT INTO `orders` (`id`, `amount`, `created_time`) VALUES (?, ?, ?)", s)
				require.Len(t, vars, 3)
				require.Equal(t, "id123", vars[0])
				require.Equal(t, 100, vars[1])
				require.Equal(t, now, vars[2])

			},
		},
		{
			name: "build_insert_if",
			build: func() *Builder {
				b := New()
				b.Insert("orders").
					If(true).Set("order_id", "order_123456").
					If(false).Set("member_id", 1234).
					Set("amount", 100).
					Set("created_time", now).
					End()

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "INSERT INTO `orders` (`order_id`, `amount`, `created_time`) VALUES (?, ?, ?)", s)
				require.Len(t, vars, 3)
				require.Equal(t, "order_123456", vars[0])
				require.Equal(t, 100, vars[1])
				require.Equal(t, now, vars[2])

			},
		},
		{
			name: "build_delete",
			build: func() *Builder {
				b := New()
				b.Delete("orders").Where().
					If(true).And("order_id = {order_id}").
					If(false).And("member_id").
					Param("order_id", "order_123456")

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "DELETE FROM `orders` WHERE order_id = ?", s)
				require.Len(t, vars, 1)
				require.Equal(t, "order_123456", vars[0])

			},
		},
		{
			name: "build_none_rotate_should_work",
			build: func() *Builder {
				id := shardid.Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, shardid.NoRotate, 0)
				b := New().On(id)
				b.Delete("orders<rotate>").Where().
					If(true).And("order_id = {order_id}").
					If(false).And("member_id").
					Param("order_id", "order_123456")

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "DELETE FROM `orders` WHERE order_id = ?", s)
				require.Len(t, vars, 1)
				require.Equal(t, "order_123456", vars[0])

			},
		},
		{
			name: "build_monthly_rotate_should_work",
			build: func() *Builder {
				id := shardid.Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, shardid.MonthlyRotate, 0)
				b := New().On(id)
				b.Delete("orders<rotate>").Where().
					If(true).And("order_id = {order_id}").
					If(false).And("member_id").
					Param("order_id", "order_123456")

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "DELETE FROM `orders_202402` WHERE order_id = ?", s)
				require.Len(t, vars, 1)
				require.Equal(t, "order_123456", vars[0])

			},
		},
		{
			name: "build_weekly_rotate_should_work",
			build: func() *Builder {
				id := shardid.Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, shardid.WeeklyRotate, 0)
				b := New().On(id)
				b.Delete("orders<rotate>").Where().
					If(true).And("order_id = {order_id}").
					If(false).And("member_id").
					Param("order_id", "order_123456")

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "DELETE FROM `orders_2024008` WHERE order_id = ?", s)
				require.Len(t, vars, 1)
				require.Equal(t, "order_123456", vars[0])

			},
		},
		{
			name: "build_daily_rotate_should_work",
			build: func() *Builder {
				id := shardid.Build(time.Date(2024, 2, 20, 0, 0, 0, 0, time.UTC).UnixMilli(), 0, 0, shardid.DailyRotate, 0)
				b := New().On(id)
				b.Delete("orders<rotate>").Where().
					If(true).And("order_id = {order_id}").
					If(false).And("member_id").
					Param("order_id", "order_123456")

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "DELETE FROM `orders_20240220` WHERE order_id = ?", s)
				require.Len(t, vars, 1)
				require.Equal(t, "order_123456", vars[0])

			},
		},
		{
			name: "build_with_map_should_be_sorted",
			build: func() *Builder {
				m := make(map[string]any)

				m["user_id"] = "x1234"
				m["id"] = 1

				b := New().Insert("users").SetMap(m).End()

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "INSERT INTO `users` (`id`, `user_id`) VALUES (?, ?)", s)
				require.Len(t, vars, 2)
				require.Equal(t, 1, vars[0])
				require.Equal(t, "x1234", vars[1])

			},
		},
		{
			name: "build_with_map_and_allow_should_not_be_sorted",
			build: func() *Builder {
				m := make(map[string]any)

				m["user_id"] = "x1234"
				m["id"] = 1

				b := New().Insert("users").SetMap(m, WithAllow("user_id", "id")).End()

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "INSERT INTO `users` (`user_id`, `id`) VALUES (?, ?)", s)
				require.Len(t, vars, 2)
				require.Equal(t, "x1234", vars[0])
				require.Equal(t, 1, vars[1])

			},
		},
		{
			name: "build_missed_input_should_work",
			build: func() *Builder {
				m := make(map[string]any)

				m["user_id"] = "x1234"
				m["id"] = 1

				b := New().Insert("users<rotate>").SetMap(m, WithAllow("user_id", "id")).End()

				return b
			},
			assert: func(t *testing.T, b *Builder) {
				s, vars, err := b.Build()
				require.NoError(t, err)
				require.Equal(t, "INSERT INTO `users` (`user_id`, `id`) VALUES (?, ?)", s)
				require.Len(t, vars, 2)
				require.Equal(t, "x1234", vars[0])
				require.Equal(t, 1, vars[1])

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sb := test.build()

			test.assert(t, sb)
		})
	}
}
