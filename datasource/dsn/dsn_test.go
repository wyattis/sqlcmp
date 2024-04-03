package dsn

import (
	"fmt"
	"testing"
)

type dsnCase struct {
	dsn string
	cfg DataSourceConfig
}

var dsnCases = []dsnCase{
	{
		dsn: "postgres://user:password@localhost:5432/postgresdb?param1=value1&paramN=valueN",
		cfg: DataSourceConfig{
			Driver:   "postgres",
			Host:     "localhost",
			Port:     5432,
			Database: "postgresdb",
			User:     "user",
			Password: "password",
			Params: map[string][]string{
				"param1": {"value1"},
				"paramN": {"valueN"},
			},
		},
	},
	{
		dsn: "mysql://username@tcp(localhost:3306)/mysqldb?param1=value1&paramN=valueN",
		cfg: DataSourceConfig{
			Driver:   "mysql",
			Host:     "localhost",
			Protocol: "tcp",
			Port:     3306,
			Database: "mysqldb",
			User:     "username",
			Params: map[string][]string{
				"param1": {"value1"},
				"paramN": {"valueN"},
			},
		},
	},
	{
		dsn: "mysql://127.0.0.1/mysqldb2",
		cfg: DataSourceConfig{
			Driver:   "mysql",
			Host:     "127.0.0.1",
			Database: "mysqldb2",
		},
	},
	{
		dsn: "sqlite3:///absolute/path/to/file.db",
		cfg: DataSourceConfig{
			Driver:   "sqlite3",
			Database: "/absolute/path/to/file.db",
		},
	},
	{
		dsn: "sqlite3://./relative/path/to/file.db",
		cfg: DataSourceConfig{
			Driver:   "sqlite3",
			Database: "./relative/path/to/file.db",
		},
	},
	{
		dsn: "sqlite3:///:memory:",
		cfg: DataSourceConfig{
			Driver:   "sqlite3",
			Database: ":memory:",
		},
	},
}

func TestParse(t *testing.T) {
	for _, c := range dsnCases {
		cfg, err := Parse(c.dsn)
		if err != nil {
			t.Errorf("error parsing dsn %s: %v", c.dsn, err)
		}
		fmt.Printf("input: %s\nres: %+v\n", c.dsn, cfg)
		if cfg.Driver != c.cfg.Driver {
			t.Errorf("expected driver %s, got %s", c.cfg.Driver, cfg.Driver)
		}
		if cfg.Protocol != c.cfg.Protocol {
			t.Errorf("expected protocol %s, got %s", c.cfg.Protocol, cfg.Protocol)
		}
		if cfg.Database != c.cfg.Database {
			t.Errorf("expected database %s, got %s", c.cfg.Database, cfg.Database)
		}
		if cfg.Host != c.cfg.Host {
			t.Errorf("expected host %s, got %s", c.cfg.Host, cfg.Host)
		}
		if cfg.Port != c.cfg.Port {
			t.Errorf("expected port %d, got %d", c.cfg.Port, cfg.Port)
		}
		if cfg.User != c.cfg.User {
			t.Errorf("expected user %s, got %s", c.cfg.User, cfg.User)
		}
		if cfg.Password != c.cfg.Password {
			t.Errorf("expected password %s, got %s", c.cfg.Password, cfg.Password)
		}
		if cfg.Params.Encode() != c.cfg.Params.Encode() {
			t.Errorf("expected params %v, got %v", c.cfg.Params, cfg.Params)
		}
	}
}
