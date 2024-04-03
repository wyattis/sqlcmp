package cli

import (
	"fmt"
	"os"
	"sqlcmp/datasource/dsn"
	"strings"
	"syscall"

	"github.com/urfave/cli/v2"
	"golang.org/x/term"
)

var VERSION string

type SourceConfig struct {
	FromDSN           string
	ToDSN             string
	Tables            []string
	ExcludeTables     []string
	PromptForPassword bool
}

var sharedFlags = []cli.Flag{
	&cli.StringFlag{
		Name:    "from-dsn",
		Aliases: []string{"f"},
		Usage:   "Data Source Name to compare from",
	},
	&cli.StringFlag{
		Name:    "to-dsn",
		Aliases: []string{"t"},
		Usage:   "Data Source Name to compare to",
	},
	&cli.BoolFlag{
		Name:    "password",
		Aliases: []string{"p"},
		Usage:   "Prompt for password",
	},
	&cli.StringSliceFlag{
		Name:  "include-tables",
		Usage: "Tables to compare",
	},
	&cli.StringSliceFlag{
		Name:  "exclude-tables",
		Usage: "Tables to exclude from comparison",
	},
}

var App = &cli.App{
	Name:    "sqlcomp",
	Usage:   "Compare data across multiple SQL databases",
	Version: VERSION,
	Commands: []*cli.Command{
		diffCmd,
		schemaCmd,
		checkFkCmd,
	},
}

func flagsToSources(ctx *cli.Context) (sources SourceConfig) {
	sources.FromDSN = ctx.String("from-dsn")
	sources.ToDSN = ctx.String("to-dsn")
	for _, table := range ctx.StringSlice("include-tables") {
		sources.Tables = append(sources.Tables, strings.Split(table, ",")...)
	}
	for _, table := range ctx.StringSlice("exclude-tables") {
		sources.ExcludeTables = append(sources.ExcludeTables, strings.Split(table, ",")...)
	}
	sources.PromptForPassword = ctx.Bool("password")
	return
}

func ensurePassword(cfg *dsn.DataSourceConfig, prompt string) (err error) {
	if cfg.Password == "" {
		if prompt == "" {
			prompt = "Enter Password: "
		}
		if _, err = fmt.Fprint(os.Stderr, prompt); err != nil {
			return
		}
		bytePassword, err := term.ReadPassword(int(syscall.Stdin))
		if err != nil {
			return err
		}
		if _, err = fmt.Fprintln(os.Stderr); err != nil {
			return err
		}
		cfg.Password = string(bytePassword)
	}
	return
}

func ensurePasswords(toCfg, fromCfg *dsn.DataSourceConfig) (err error) {
	if err = ensurePassword(fromCfg, "Enter Password for 'from DSN': "); err != nil {
		return
	}
	if err = ensurePassword(toCfg, "Enter Password for 'to DSN': "); err != nil {
		return
	}
	return
}

func filterTables(tables []string, include []string, exclude []string) []string {
	res := make([]string, 0, len(tables))
	for _, table := range tables {
		isExcluded := len(include) > 0
		for _, e := range exclude {
			if e == table {
				isExcluded = true
				break
			}
		}
		for _, i := range include {
			if i == table {
				isExcluded = false
				break
			}
		}
		if !isExcluded {
			res = append(res, table)
		}
	}
	return res
}
