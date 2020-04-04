package redis

import (
	"context"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	"vitess.io/vitess/go/vt/vtgate"
)

/*
redis - keyspace
redis_store - table
(rkey varbinary(512), rvalue varbinary(1024), primary key (rkey)) - per row

GET - select rvalue from redis_store where rkey = :rkey
PUT - insert into redis_store(rkey, rvalue) values(:rkey, :rvalue) on duplicate key update rvalue = :rvalue

*/

type redisCon interface {
	Get(rKey []byte) []byte
	Set(rKey []byte, rValue []byte) []byte
}

var _ redisCon = (*redisListener)(nil)

type redisListener struct {
	vtg *vtgate.VTGate
	//executor *vtgate.Executor
}

func newRedisListener() *redisListener {
	vtg := vtgate.Init(context.Background(), nil, nil, "", 1, nil)
	return &redisListener{
		vtg: vtg,
	}
}

func (r redisListener) Get(rKey []byte) []byte {
	bindVar := map[string]*query.BindVariable{"rkey": sqltypes.BytesBindVariable(rKey)}
	_, qr, err := r.vtg.Execute(context.Background(), &vtgatepb.Session{}, "select rvalue from redis_store where rkey = :rkey", bindVar)
	if err != nil {
		fmt.Println(err)
	}
	if len(qr.Rows) < 1 {
		return nil
	}
	return qr.Rows[0][0].ToBytes()
}

func (r redisListener) Set(rKey []byte, rValue []byte) []byte {
	bindVar := map[string]*query.BindVariable{
		"rkey":   sqltypes.BytesBindVariable(rKey),
		"rValue": sqltypes.BytesBindVariable(rValue),
	}
	_, qr, err := r.vtg.Execute(context.Background(), &vtgatepb.Session{}, "insert into redis_store(rkey, rvalue) values(:rkey, :rvalue) on duplicate key update rvalue = :rvalue", bindVar)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Rows Affected : %d", qr.RowsAffected)
	return nil
}
