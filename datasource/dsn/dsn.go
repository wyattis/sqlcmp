package dsn

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type DataSourceConfig struct {
	Driver   string
	Protocol string
	Database string
	Host     string
	Port     int
	User     string
	Password string
	Params   url.Values
}

func Parse(dsn string) (cfg DataSourceConfig, err error) {
	// parse dsn
	// driver://[user[:password]@][protocol[(address[:port])]]/dbname[?param1=value1&paramN=valueN]
	// postgres://user:password@localhost:5432/dbname?param1=value1&paramN=valueN
	// mysql://user:password@tcp(localhost:3306)/dbname?param1=value1&paramN=valueN
	// sqlite3:///absolute/path/to/file.db
	// sqlite3://./relative/path/to/file.db
	// sqlite3:///:memory:

	dsn, params, _ := strings.Cut(dsn, "?")
	if params != "" {
		cfg.Params, err = url.ParseQuery(params)
		if err != nil {
			return cfg, fmt.Errorf("invalid params: %w", err)
		}
	}
	// driver
	driver, dsn, ok := strings.Cut(dsn, "://")
	if !ok || driver == "" {
		return cfg, errors.New("must include driver")
	}
	cfg.Driver = driver

	// user:password@
	userPass, dsn, ok := strings.Cut(dsn, "@")
	if ok {
		user, pass, ok := strings.Cut(userPass, ":")
		if ok {
			cfg.User = user
			cfg.Password = pass
		} else {
			cfg.User = user
		}
	} else {
		dsn = userPass
	}

	// protocol
	protocol, dsn, hasProtocol := strings.Cut(dsn, "(")
	if hasProtocol {
		cfg.Protocol = protocol
		host, d, ok := strings.Cut(dsn, ")")
		if !ok {
			return cfg, errors.New("missing protocol address")
		}
		if err = parseHostPort(&cfg, host); err != nil {
			return cfg, err
		}
		dsn = d
	} else {
		dsn = protocol
		// host:port
		hostPort, d, ok := strings.Cut(dsn, "/")
		if ok && hostPort != "" && hostPort != "." {
			if err = parseHostPort(&cfg, hostPort); err != nil {
				return cfg, err
			}
			dsn = "/" + d
		}
	}

	if dsn == "/:memory:" {
		dsn = ":memory:"
	}
	if cfg.Driver != "sqlite3" && dsn[0] == '/' {
		dsn = dsn[1:]
	}
	cfg.Database = dsn

	return cfg, nil
}

func parseHostPort(cfg *DataSourceConfig, hostPort string) (err error) {
	host, port, ok := strings.Cut(hostPort, ":")
	if ok {
		cfg.Port, err = strconv.Atoi(port)
		if err != nil {
			return fmt.Errorf("invalid port %w\n", err)
		}
	}
	cfg.Host = host
	return
}
