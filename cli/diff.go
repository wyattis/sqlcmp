package cli

import (
	"fmt"
	"os"
	"sqlcmp/datasource"
	"sqlcmp/datasource/dsn"

	"github.com/urfave/cli/v2"
	"github.com/wyattis/z/zset/zstringset"
)

var diffCmd = &cli.Command{
	Name:  "diff",
	Usage: "compare the data in two data sources",
	Flags: sharedFlags,
	Action: func(cCtx *cli.Context) (err error) {
		sources := flagsToSources(cCtx)
		if sources.FromDSN == "" || sources.ToDSN == "" {
			return fmt.Errorf("from-dsn and to-dsn are required")
		}
		fromCfg, err := dsn.Parse(sources.FromDSN)
		if err != nil {
			return err
		}
		toCfg, err := dsn.Parse(sources.ToDSN)
		if err != nil {
			return err
		}
		if sources.PromptForPassword {
			if err = ensurePassword(&fromCfg, "Enter 'from-dsn' password: "); err != nil {
				return
			}
			if err = ensurePassword(&toCfg, "Enter 'to-dsn' password: "); err != nil {
				return
			}
		}
		toDb, err := datasource.Open(toCfg)
		if err != nil {
			return err
		}
		defer toDb.Close()
		fromDb, err := datasource.Open(fromCfg)
		if err != nil {
			return err
		}
		defer fromDb.Close()

		fmt.Fprintf(os.Stderr, "comparing data between %s and %s\n", sources.FromDSN, sources.ToDSN)
		toTables, err := toDb.GetTableNames()
		if err != nil {
			return err
		}
		fromTables, err := fromDb.GetTableNames()
		if err != nil {
			return err
		}

		toTableSet, fromTableSet := zstringset.New(toTables...), zstringset.New(fromTables...)
		missingTables := fromTableSet.Clone().Difference(toTableSet)
		for _, table := range missingTables.Items() {
			fmt.Fprintf(os.Stderr, "missing table: %s\n", table)
		}
		sharedTables := fromTableSet.Clone().Intersection(toTableSet)
		for _, table := range sharedTables.Items() {
			fmt.Fprintf(os.Stderr, "comparing table: %s\n", table)
			if err = compareTable(fromDb, toDb, table); err != nil {
				return err
			}
		}
		return
	},
}

func compareTable(fromDb, toDb datasource.DataSource, table string) (err error) {
	from, err := fromDb.GetSchema([]string{table})
	if err != nil {
		return err
	}
	to, err := toDb.GetSchema([]string{table})
	if err != nil {
		return err
	}
	fromTable, toTable := from[0], to[0]
	fromPk, fromCols := []string{}, []string{}
	for _, col := range fromTable.Columns {
		fromCols = append(fromCols, col.Name)
		if col.IsPrimary {
			fromPk = append(fromPk, col.Name)
		}
	}
	if len(fromPk) == 0 {
		return fmt.Errorf("table %s has no primary key", fromTable.Name)
	}
	toPk, toCols := []string{}, []string{}
	for _, col := range toTable.Columns {
		toCols = append(toCols, col.Name)
		if col.IsPrimary {
			toPk = append(toPk, col.Name)
		}
	}
	if len(toPk) == 0 {
		return fmt.Errorf("table %s has no primary key", toTable.Name)
	}
	if len(fromPk) != len(toPk) {
		return fmt.Errorf("table %s has different primary key columns", table)
	}

	sharedCols := zstringset.New(fromCols...).Intersection(zstringset.New(toCols...)).Items()

	fromIter, err := fromDb.TableIterator(table, sharedCols, fromPk)
	if err != nil {
		return err
	}
	defer fromIter.Close()
	toIter, err := toDb.TableIterator(table, sharedCols, fromPk)
	if err != nil {
		return err
	}
	defer toIter.Close()

	// TODO: fix how we represent which columns are missing from each datasource
	if len(fromCols) != len(toCols) {
		missingCols := zstringset.New(fromCols...).Difference(zstringset.New(toCols...)).Items()
		fmt.Fprintln(os.Stderr, "'to' is missing columns: ", missingCols)
	}

	// TODO: Probably we need to compare the actual underlying data types here instead of just the bytes
	fromRow, toRow := make([]interface{}, len(sharedCols)), make([]interface{}, len(sharedCols))
	for i := range fromRow {
		fromRow[i] = new([]byte)
	}
	for i := range toRow {
		toRow[i] = new([]byte)
	}

	// TODO: do the comparison

	return
}
