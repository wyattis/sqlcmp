package main

import (
	_ "embed"
	"log"
	"os"

	"sqlcmp/cli"
	_ "sqlcmp/datasource"
	_ "sqlcmp/datasource/mysql"
)

//go:embed VERSION
var VERSION string

func main() {
	cli.VERSION = VERSION
	if err := cli.App.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
