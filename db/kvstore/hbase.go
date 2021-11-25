package kvstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tutububug/kvproto/pkg/tiap_hbaselike_kvrpcpb"
	tiap_client "github.com/tutububug/tiap/client"
)

type hbaseDB struct {
	*tiap_client.HBaseTable
	r       *util.RowCodec
	bufPool *util.BufPool
}

func createHBaseDB(p *properties.Properties, dbName, tableName, token string) (ycsb.DB, error) {
	pdAddr := p.GetString(kvstoreApAddr, "127.0.0.1:3379")
	table := tiap_client.OpenHBaseTable(pdAddr, dbName, tableName, token)

	bufPool := util.NewBufPool()

	return &hbaseDB{
		HBaseTable: table,
		r:       util.NewRowCodec(p),
		bufPool: bufPool,
	}, nil
}

func (db *hbaseDB) Close() error {
	return nil
}

func (db *hbaseDB) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (db *hbaseDB) CleanupThread(ctx context.Context) {
}

func (db *hbaseDB) getRowKey(table string, key string) []byte {
	return []byte(key)
}

func (db *hbaseDB) ToSqlDB() *sql.DB {
	return nil
}

func genColumnFamilyValue(values map[string][]byte) *tiap_hbaselike_kvrpcpb.ColumnFamilyValue {
	colVals := make([]*tiap_hbaselike_kvrpcpb.ColumnValue, 0, len(values))
	for field, value := range values {
		colVals = append(colVals, &tiap_hbaselike_kvrpcpb.ColumnValue{
			Name: field,
			Value: value,
		})
	}

	m := make(map[string]*tiap_hbaselike_kvrpcpb.ColumnValues)
	m["default"] = &tiap_hbaselike_kvrpcpb.ColumnValues{
		ColumnValues: colVals,
	}

	return &tiap_hbaselike_kvrpcpb.ColumnFamilyValue{
		M: m,
	}
}

func genColumnFamily(fields []string) *tiap_hbaselike_kvrpcpb.ColumnFamily {
	colArr := make([]*tiap_hbaselike_kvrpcpb.Column, 0, len(fields))
	for _, f := range fields {
		colArr = append(colArr, &tiap_hbaselike_kvrpcpb.Column{
			Name: f,
		})
	}
	cols := &tiap_hbaselike_kvrpcpb.Columns{
		Columns: colArr,
	}
	m := make(map[string]*tiap_hbaselike_kvrpcpb.Columns)
	m["default"] = cols

	return &tiap_hbaselike_kvrpcpb.ColumnFamily{
		M: m,
	}
}

func columnValuesDecode(colVals *tiap_hbaselike_kvrpcpb.ColumnValues, fields []string) map[string][]byte {
	ret := make(map[string][]byte)
	for _, f := range fields {
		for _, colVal := range colVals.GetColumnValues() {
			if strings.Compare(f, colVal.GetName()) == 0 {
				ret[colVal.GetName()] = colVal.GetValue()
			}
		}
	}
	return ret
}

func (db *hbaseDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	family := genColumnFamily(fields)
	rows, err := db.HBaseTable.BatchGetRow(ctx, [][]byte{[]byte(key)}, family)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, errors.New("read row len != 1")
	}
	if rows[0] == nil {
		return nil, nil
	}

	return columnValuesDecode(rows[0].GetFamilyValues().GetM()["default"], fields), nil
}

func (db *hbaseDB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	rowKeys := genRowKeys(keys)
	cf := genColumnFamily(fields)
	values, err := db.HBaseTable.BatchGetRow(ctx, rowKeys, cf)
	if err != nil {
		return nil, err
	}
	rowValues := make([]map[string][]byte, len(keys))
	for _, v := range values {
		rowVal := columnValuesDecode(v.GetFamilyValues().GetM()["default"], fields)
		rowValues = append(rowValues, rowVal)
	}

	return rowValues, nil
}

func (db *hbaseDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	cf := genColumnFamily(fields)
	rows, err := db.HBaseTable.ScanRow(ctx, db.getRowKey(table, startKey), nil, cf, int64(count))
	if err != nil {
		return nil, err
	}

	res := make([]map[string][]byte, len(rows))
	for i, row := range rows {
		if row == nil {
			res[i] = nil
			continue
		}

		res[i] = columnValuesDecode(row.GetFamilyValues().GetM()["default"], fields)
	}

	return res, nil
}

func (db *hbaseDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	var fields []string
	for f, _ := range values {
		fields = append(fields, f)
	}
	cf := genColumnFamily(fields)
	rows, err := db.HBaseTable.BatchGetRow(ctx, [][]byte{[]byte(key)}, cf)
	if err != nil {
		return nil
	}
	if len(rows) != 1 {
		return errors.New("read row len != 1")
	}

	data := columnValuesDecode(rows[0].GetFamilyValues().GetM()["default"], fields)
	for field, value := range values {
		data[field] = value
	}

	// Update data and use Insert to overwrite.
	return db.Insert(ctx, table, key, data)
}

func (db *hbaseDB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
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
	rowKeys := genRowKeys(keys)
	rowVals := genRowValues(rowKeys, values)
	return db.HBaseTable.BatchPutRow(ctx, rowVals)
}

func genRowValues(rowKeys [][]byte, columnValuesArr []map[string][]byte) (rowVals []*tiap_hbaselike_kvrpcpb.RowValue) {
	for i := 0; i < len(rowKeys); i++ {
		key := rowKeys[i]
		values := columnValuesArr[i]

		colVals := make([]*tiap_hbaselike_kvrpcpb.ColumnValue, 0, len(values))
		for field, value := range values {
			colVals = append(colVals, &tiap_hbaselike_kvrpcpb.ColumnValue{
				Name: field,
				Value: value,
			})
		}

		m := make(map[string]*tiap_hbaselike_kvrpcpb.ColumnValues)
		m["default"] = &tiap_hbaselike_kvrpcpb.ColumnValues{
			ColumnValues: colVals,
		}
		rowVal := &tiap_hbaselike_kvrpcpb.RowValue{
			RowKey: key,
			FamilyValues: &tiap_hbaselike_kvrpcpb.ColumnFamilyValue{
				M: m,
			},
		}

		rowVals = append(rowVals, rowVal)
	}
	return
}

func genRowKeys(keys []string) [][]byte {
	rowKeys := make([][]byte, 0, len(keys))
	for _, k := range keys {
		rowKeys = append(rowKeys, []byte(k))
	}
	return rowKeys
}

func (db *hbaseDB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	rowVals := genRowValues([][]byte{[]byte(key)}, []map[string][]byte{values})
	err := db.HBaseTable.BatchPutRow(ctx, rowVals)
	if err != nil {
		fmt.Println("hbase insert error: ", err)
	}
	return err
}

func (db *hbaseDB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	rowKeys := genRowKeys(keys)
	rowVals := genRowValues(rowKeys, values)
	return db.HBaseTable.BatchPutRow(ctx, rowVals)
}

func (db *hbaseDB) Delete(ctx context.Context, table string, key string) error {
	return db.HBaseTable.BatchDeleteRow(ctx, [][]byte{[]byte(key)})
}

func (db *hbaseDB) BatchDelete(ctx context.Context, table string, keys []string) error {
	rowKeys := genRowKeys(keys)
	return db.HBaseTable.BatchDeleteRow(ctx, rowKeys)
}
