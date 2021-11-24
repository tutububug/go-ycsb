package kvstore

import (
	"context"
	"database/sql"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	tiap_client "github.com/tutububug/tiap/client"
)

type rawDB struct {
	kvStore
}

func createRawDB(p *properties.Properties, dbName, tableName, token string) (ycsb.DB, error) {
	pdAddr := p.GetString(kvstoreApAddr, "127.0.0.1:3379")
	db := tiap_client.NewKVClient(pdAddr)

	bufPool := util.NewBufPool()

	return &rawDB{
		kvStore{db: db,
			r:         util.NewRowCodec(p),
			bufPool:   bufPool,
			dbName:    dbName,
			tableName: tableName,
			token:     token,
		},
	}, nil
}

func (db *rawDB) Close() error {
	return nil
}

func (db *rawDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *rawDB) CleanupThread(ctx context.Context) {
}

func (db *rawDB) getRowKey(table string, key string) []byte {
	return []byte(key)
}

func (db *rawDB) ToSqlDB() *sql.DB {
	return nil
}

func (db *rawDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	row, err := db.db.RawGet(ctx, db.dbName, db.tableName, db.token, db.appName, db.getRowKey(table, key))
	if err != nil {
		return nil, err
	} else if row == nil {
		return nil, nil
	}

	return db.r.Decode(row, fields)
}

func (db *rawDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	values, err := db.db.RawBatchGet(ctx, db.dbName, db.tableName, db.token, db.appName, rowKeys)
	if err != nil {
		return nil, err
	}
	rowValues := make([]map[string][]byte, len(keys))

	for i, value := range values {
		if len(value) > 0 {
			rowValues[i], err = db.r.Decode(value, fields)
		} else {
			rowValues[i] = nil
		}
	}
	return rowValues, nil
}

func (db *rawDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	_, rows, err := db.db.RawScan(ctx, db.dbName, db.tableName, db.token, db.appName,
		db.getRowKey(table, startKey), nil, int64(count), false)
	if err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		v, err := db.r.Decode(row, fields)
		if err != nil {
			return nil, err
		}
		res[i] = v
	}

	return res, nil
}

func (db *rawDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	row, err := db.db.RawGet(ctx, db.dbName, db.tableName, db.token, db.appName, db.getRowKey(table, key))
	if err != nil {
		return nil
	}

	data, err := db.r.Decode(row, nil)
	if err != nil {
		return err
	}

	for field, value := range values {
		data[field] = value
	}

	// Update data and use Insert to overwrite.
	return db.Insert(ctx, table, key, data)
}

func (db *rawDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		// TODO should we check the key exist?
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.RawBatchPut(ctx, db.dbName, db.tableName, db.token, db.appName,
		rawKeys, rawValues)
}

func (db *rawDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	// Simulate TiDB data
	buf := db.bufPool.Get()
	defer db.bufPool.Put(buf)

	rowData, err := db.r.Encode(buf.Bytes(), values)
	if err != nil {
		return err
	}

	return db.db.RawPut(ctx, db.dbName, db.tableName, db.token, db.appName,
		db.getRowKey(table, key), rowData)
}

func (db *rawDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var rawKeys [][]byte
	var rawValues [][]byte
	for i, key := range keys {
		rawKeys = append(rawKeys, db.getRowKey(table, key))
		rawData, err := db.r.Encode(nil, values[i])
		if err != nil {
			return err
		}
		rawValues = append(rawValues, rawData)
	}
	return db.db.RawBatchPut(ctx, db.dbName, db.tableName, db.token, db.appName,
		rawKeys, rawValues)
}

func (db *rawDB) Delete(ctx context.Context, table string, key string) error {
	return db.db.RawDelete(ctx, db.dbName, db.tableName, db.token, db.appName,
		db.getRowKey(table, key))
}

func (db *rawDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	rowKeys := make([][]byte, len(keys))
	for i, key := range keys {
		rowKeys[i] = db.getRowKey(table, key)
	}
	return db.db.RawBatchDelete(ctx, db.dbName, db.tableName, db.token, db.appName, rowKeys)
}
