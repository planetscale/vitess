package binlog

import (
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/dbconfigs"
)

type FakeBinlogConnection struct {
	*mysql.Conn
	cp           dbconfigs.Connector
	streamerPool *streamerPool
}

func (f FakeBinlogConnection) getServerID() uint32 {
	return 1
}

func (f FakeBinlogConnection) setConnection(conn *mysql.Conn) {
	f.Conn = conn
}

func (f FakeBinlogConnection) getConnector() dbconfigs.Connector {
	return f.cp
}

func (f FakeBinlogConnection) closeConnection() {

}

var _ IBinlogConnection = (*FakeBinlogConnection)(nil)

func NewFakeBinlogConnection(cp dbconfigs.Connector) (*FakeBinlogConnection, error) {
	return &FakeBinlogConnection{cp: cp}, nil
}
