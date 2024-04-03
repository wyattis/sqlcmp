package cli

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"sqlcmp/datasource"
	"sqlcmp/datasource/dsn"

	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v2"
)

var schemaFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "format",
		Usage: "Output format (json, yaml, table)",
		Value: "json",
	},
}

var schemaCmd = &cli.Command{
	Name:  "schema",
	Usage: "view schema of a data source",
	Flags: append(schemaFlags, sharedFlags...),
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

		sort.Slice(tables, func(i, j int) bool {
			return tables[i].Name < tables[j].Name
		})

		switch cCtx.String("format") {
		case "json":
			e := json.NewEncoder(os.Stdout)
			e.SetIndent("", "  ")
			return e.Encode(tables)
		case "yaml":
			return yaml.NewEncoder(os.Stdout).Encode(tables)
		case "table":
			for _, table := range tables {
				fmt.Fprintln(os.Stdout, table.Name)
			}
		default:
			return fmt.Errorf("invalid format: %s", cCtx.String("format"))
		}
		return
	},
}
