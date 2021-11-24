package kvstore

import (
	"fmt"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	tiap_client "github.com/tutububug/tiap/client"
)

const (
	kvstoreApAddr = "kvstore.ap.addr"
	// raw, hbase
	kvstoreTableType = "kvstore.table.type"
	//kvstoreConnCount = "kvstore.conncount"
	//kvstoreBatchSize = "kvstore.batchsize"
	kvstoreDbName = "kvstore.dbname"
	kvstoreTableName = "kvstore.tablename"
	kvstoreToken = "kvstore.token"
	kvstoreAppName = "kvstore.appname"

)

type kvStore struct {
	db      *tiap_client.KVClient
	r       *util.RowCodec
	bufPool *util.BufPool
	dbName string
	tableName string
	token string
	appName string
}

type kvstoreCreator struct {
}

func (c kvstoreCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	dbName := p.GetString(kvstoreDbName, "")
	tableName := p.GetString(kvstoreTableName, "")
	token := p.GetString(kvstoreToken, "")

	tp := p.GetString(kvstoreTableType, "raw")
	switch tp {
	case "raw":
		return createRawDB(p, dbName, tableName, token)
	case "hbase":
		return createHBaseDB(p, dbName, tableName, token)
	default:
		return nil, fmt.Errorf("unsupported type %s", tp)
	}
}

func init() {
	ycsb.RegisterDBCreator("kvstore", kvstoreCreator{})
}
