package cli

import (
	"fmt"
	"os"
	"sqlcmp/datasource"
	"sqlcmp/datasource/dsn"
	"strings"

	"github.com/urfave/cli/v2"
)

var checkFkCmd = &cli.Command{
	Name:  "check-foreign-keys",
	Usage: "check foreign keys on a single data source",
	Flags: sharedFlags,
	Action: func(cCtx *cli.Context) (err error) {
		sources := flagsToSources(cCtx)
		if sources.FromDSN == "" {
			return fmt.Errorf("from-dsn is required")
		}

		cfg, err := dsn.Parse(sources.FromDSN)
		if err != nil {
			return err
		}
		if sources.PromptForPassword {
			if err = ensurePassword(&cfg, ""); err != nil {
				return
			}
		}

		db, err := datasource.Open(cfg)
		if err != nil {
			return err
		}
		defer db.Close()

		tableNames, err := db.GetTableNames()
		if err != nil {
			return err
		}

		tableNames = filterTables(tableNames, sources.Tables, sources.ExcludeTables)
		fmt.Fprintln(os.Stderr, "Tables: ", strings.Join(tableNames, ","))

		tables, err := db.GetSchema(tableNames)
		if err != nil {
			return err
		}

		for _, table := range tables {
			fmt.Fprintln(os.Stderr, "checking table", table.Name)
			for _, fk := range table.ForeignKeys {
				// fmt.Fprintln(os.Stderr, "checking foreign key", fk.Name)
				// select count(distinct geo_id) from respondent_geo where geo_id not in (select id from geo)
				q := fmt.Sprintf("SELECT COUNT(DISTINCT `%s`) FROM `%s` WHERE `%s` NOT IN (SELECT `%s` FROM `%s`)", fk.FromColumn, fk.From, fk.FromColumn, fk.ToColumn, fk.To)
				// fmt.Fprintln(os.Stderr, q)
				row := db.DB().QueryRow(q)
				if row.Err() != nil {
					return fmt.Errorf("failed to query foreign key %s: %w", fk.Name, err)
				}
				var count int
				if err = row.Scan(&count); err != nil {
					return fmt.Errorf("failed to scan foreign key %s: %w", fk.Name, err)
				}
				if count > 0 {
					fmt.Fprintf(os.Stdout, "table `%s`.`%s` references `%s`.`%s`, but `%s` is missing %d values\n", fk.From, fk.FromColumn, fk.To, fk.ToColumn, fk.To, count)
					q := fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` NOT IN (SELECT `%s` FROM `%s`)", fk.From, fk.FromColumn, fk.ToColumn, fk.To)
					fmt.Fprintf(os.Stdout, "use this query to find the missing values:\n  %s\n", q)
				}
			}
		}

		return nil
	},
}
