package mysql

import (
	"database/sql"
	"fmt"
	"strings"

	db "sqlcmp/datasource"
	"sqlcmp/datasource/dsn"
	"sqlcmp/datasource/schema"

	_ "github.com/go-sql-driver/mysql"
)

type mysqlColumn struct {
	Field   string
	Type    string
	Null    string
	Key     string
	Default *string
	Extra   string
}

type mysqlFk struct {
	TableName            string
	ColumnName           string
	ConstraintName       string
	ReferencedTableName  string
	ReferencedColumnName string
}

func init() {
	db.RegisterSource("mysql", func(cfg dsn.DataSourceConfig) (source db.DataSource, err error) {
		if cfg.Host == "" {
			cfg.Host = "localhost"
		}
		if cfg.Port == 0 {
			cfg.Port = 3306
		}

		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database))
		if err != nil {
			return nil, err
		}
		return &dataSource{db: db}, nil
	})
}

type dataSource struct {
	db *sql.DB
}

func (d *dataSource) DB() *sql.DB {
	return d.db
}

func (d *dataSource) Close() (err error) {
	return d.db.Close()
}

func (d *dataSource) GetTableNames() (tables []string, err error) {
	rows, err := d.db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		t := ""
		if err = rows.Scan(&t); err != nil {
			return nil, err
		}
		tables = append(tables, t)
	}
	return
}

func (d *dataSource) GetSchema(tableNames []string) (tables []schema.Table, err error) {
	tables = make([]schema.Table, len(tableNames))
	for i, table := range tables {
		tables[i].Name = tableNames[i]
		table = tables[i]
		rows, err := d.db.Query(fmt.Sprintf("SHOW COLUMNS FROM `%s`", table.Name))
		if err != nil {
			return nil, fmt.Errorf("Failed to fetch columns:\n%w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var c mysqlColumn
			err = rows.Scan(&c.Field, &c.Type, &c.Null, &c.Key, &c.Default, &c.Extra)
			if err != nil {
				return nil, fmt.Errorf("Failed to fetch column row:\n%w", err)
			}
			sCol := schema.Column{
				Name:       c.Field,
				Type:       c.Type,
				Extra:      c.Extra,
				IsNullable: c.Null == "YES",
				IsPrimary:  c.Key == "PRI",
			}
			if c.Default != nil {
				sCol.Default = *c.Default
			}
			tables[i].Columns = append(tables[i].Columns, sCol)
		}

		// TODO: fetch foreign keys
		fkQuery := `
			SELECT 
				TABLE_NAME, COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
			FROM
				INFORMATION_SCHEMA.KEY_COLUMN_USAGE
			WHERE
				REFERENCED_TABLE_SCHEMA = (SELECT DATABASE()) AND
				REFERENCED_TABLE_NAME = ?
		`
		rows, err = d.db.Query(fkQuery, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch foreign keys:\n%w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var fk mysqlFk
			err = rows.Scan(&fk.TableName, &fk.ColumnName, &fk.ConstraintName, &fk.ReferencedTableName, &fk.ReferencedColumnName)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch fk row:\n%w", err)
			}
			fkCol := schema.ForeignKey{
				Name:       fk.ConstraintName,
				From:       fk.TableName,
				FromColumn: fk.ColumnName,
				To:         fk.ReferencedTableName,
				ToColumn:   fk.ReferencedColumnName,
			}
			tables[i].ForeignKeys = append(tables[i].ForeignKeys, fkCol)
		}

		// TODO: fetch indicies
	}
	return
}

func (d *dataSource) TableIterator(table string, columns, orderBy []string) (iterator schema.RecordIterator, err error) {
	colStr := "*"
	if len(columns) > 0 {
		colStr = "`" + strings.Join(columns, "`,`") + "`"
	}
	if len(orderBy) == 0 {
		return d.db.Query(fmt.Sprintf("SELECT %s FROM `%s`", colStr, table))
	}
	return d.db.Query(fmt.Sprintf("SELECT %s FROM `%s` ORDER BY %s", colStr, table, "`"+strings.Join(orderBy, "`,`")+"`"))
}
