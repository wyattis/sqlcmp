package datasource

import (
	"database/sql"
	"fmt"

	dbdsn "sqlcmp/datasource/dsn"
	"sqlcmp/datasource/schema"
)

type OpenFunc = func(cfg dbdsn.DataSourceConfig) (source DataSource, err error)

type DataSource interface {
	DB() *sql.DB
	GetTableNames() (tables []string, err error)
	GetSchema(tables []string) (schema []schema.Table, err error)
	TableIterator(table string, columns, orderBy []string) (iterator schema.RecordIterator, err error)
	Close() (err error)
}

var sources = map[string]OpenFunc{}

func RegisterSource(driver string, source OpenFunc) {
	if _, ok := sources[driver]; ok {
		panic("source already registered: " + driver)
	}
	sources[driver] = source
}

func OpenDSN(dsn string) (source DataSource, err error) {
	cfg, err := dbdsn.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("Invalid dsn: %s\n%w", dsn, err)
	}

	return Open(cfg)
}

func Open(cfg dbdsn.DataSourceConfig) (source DataSource, err error) {
	opener, ok := sources[cfg.Driver]
	if !ok {
		return nil, fmt.Errorf("no source registered for driver: %s", cfg.Driver)
	}
	return opener(cfg)
}
