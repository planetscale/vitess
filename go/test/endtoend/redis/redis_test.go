package redis

import (
	"flag"
	"os"
	"testing"
	"vitess.io/vitess/go/redis"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

var (
	clusterInstance *cluster.LocalProcessCluster
	keyspaceName    = "redis"
	cell            = "zone1"
	hostname        = "localhost"
	sqlSchema       = `
	create table redis_store(
		rkey varbinary(256),
		rvalue varbinary(1024),
		primary key(rkey)
	)Engine=InnoDB;`

	vSchema = `
		{	
			"sharded":true,
			"vindexes": {
				"hash_index": {
					"type": "hash"
				}
			},	
			"tables": {
				"redis_store":{
					"column_vindexes": [
						{
							"column": "rkey",
							"name": "hash_index"
						}
					]
				}
			}
		}
	`
)

func TestMain(m *testing.M) {
	defer cluster.PanicHandler(nil)
	flag.Parse()

	exitCode := func() int {
		clusterInstance = cluster.NewCluster(cell, hostname)
		defer clusterInstance.Teardown()

		// Start topo server
		if err := clusterInstance.StartTopo(); err != nil {
			return 1
		}

		// Start keyspace
		keyspace := &cluster.Keyspace{
			Name:      keyspaceName,
			SchemaSQL: sqlSchema,
			VSchema:   vSchema,
		}
		if err := clusterInstance.StartKeyspace(*keyspace, []string{"-80", "80-"}, 1, false); err != nil {
			return 1
		}

		// Start vtgate
		if err := clusterInstance.StartVtgate(); err != nil {
			return 1
		}

		//clusterInstance.
		return m.Run()
	}()
	os.Exit(exitCode)
}

func exec(t *testing.T, conn *mysql.Conn, query string) *sqltypes.Result {
	t.Helper()
	qr, err := conn.ExecuteFetch(query, 1000, true)
	require.Nil(t, err)
	return qr
}

func TestRedis(t *testing.T) {
	defer cluster.PanicHandler(t)
	//ctx := context.Background()
	//vtParams := mysql.ConnParams{
	//	Host: "localhost",
	//	Port: clusterInstance.VtgateMySQLPort,
	//}
	type key struct {
		rKey   string
		rValue string
	}

	allKeys := []key{
	{
		rKey:   "a",
		rValue: "abc",
	},
	{
		rKey:   "x",
		rValue: "xyz",
	},
	{
		rKey:   "m",
		rValue: "mno",
	},
	}

	//conn, err := mysql.Connect(ctx, &vtParams)
	//require.NoError(t, err)
	//defer conn.Close()

	redisCon := redis.NewRedisListener(clusterInstance)

	for _, tc := range allKeys {
		t.Run(tc.rKey, func(t *testing.T) {
			getRes := redisCon.Get([]byte(tc.rKey))
			require.Nil(t, getRes)
			setRes := redisCon.Set([]byte(tc.rKey), []byte(tc.rValue))
			require.Nil(t, setRes)
			getRes = redisCon.Get([]byte(tc.rKey))
			require.Equal(t, tc.rValue, string(getRes))
		})
	}
}
