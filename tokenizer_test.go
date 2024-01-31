package sqle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTokenize(t *testing.T) {

	tests := []struct {
		name   string
		sql    string
		assert func(t *testing.T, tz *Tokenizer)
	}{
		{
			name: "no_tokens",
			sql:  "select * from orders",
			assert: func(t *testing.T, tz *Tokenizer) {
				require.Len(t, tz.Tokens, 1)
				require.Equal(t, Text("select * from orders"), tz.Tokens[0])
			},
		},
		{
			name: "input_tokens",
			sql:  "select * from orders_<year> left join <sharding_id>_users_<year>",
			assert: func(t *testing.T, tz *Tokenizer) {
				require.Len(t, tz.Tokens, 6)
				require.Equal(t, Text("select * from orders_"), tz.Tokens[0])
				require.Equal(t, Input("year"), tz.Tokens[1])
				require.Equal(t, Text(" left join "), tz.Tokens[2])
				require.Equal(t, Input("sharding_id"), tz.Tokens[3])
				require.Equal(t, Text("_users_"), tz.Tokens[4])
				require.Equal(t, Input("year"), tz.Tokens[5])
			},
		},
		{
			name: "param_tokens",
			sql:  "select * from orders left join users where users.id={user_id} and orders.created>={created_time}",
			assert: func(t *testing.T, tz *Tokenizer) {
				require.Len(t, tz.Tokens, 4)
				require.Equal(t, Text("select * from orders left join users where users.id="), tz.Tokens[0])
				require.Equal(t, Param("user_id"), tz.Tokens[1])
				require.Equal(t, Text(" and orders.created>="), tz.Tokens[2])
				require.Equal(t, Param("created_time"), tz.Tokens[3])
			},
		},
		{
			name: "input_and_param_tokens",
			sql:  "select * from order_<year> left join <sharding_id>_users where <sharding_id>_users.id={user_id} and orders_<year>.created>={created_time}",
			assert: func(t *testing.T, tz *Tokenizer) {
				require.Len(t, tz.Tokens, 12)
				require.Equal(t, Text("select * from order_"), tz.Tokens[0])
				require.Equal(t, Input("year"), tz.Tokens[1])
				require.Equal(t, Text(" left join "), tz.Tokens[2])
				require.Equal(t, Input("sharding_id"), tz.Tokens[3])
				require.Equal(t, Text("_users where "), tz.Tokens[4])
				require.Equal(t, Input("sharding_id"), tz.Tokens[5])
				require.Equal(t, Text("_users.id="), tz.Tokens[6])
				require.Equal(t, Param("user_id"), tz.Tokens[7])
				require.Equal(t, Text(" and orders_"), tz.Tokens[8])
				require.Equal(t, Input("year"), tz.Tokens[9])
				require.Equal(t, Text(".created>="), tz.Tokens[10])
				require.Equal(t, Param("created_time"), tz.Tokens[11])

			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tz := Tokenize(test.sql)
			require.Equal(t, test.sql, tz.Raw)
			test.assert(t, tz)
		})
	}

}
