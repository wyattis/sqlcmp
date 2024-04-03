package schema

type RecordIterator interface {
	Next() bool
	Columns() ([]string, error)
	Err() error
	Close() error
	Scan(dest ...interface{}) error
}

type Column struct {
	Name            string `json:"name"`
	Type            string `json:"type"`
	Default         string `json:"default"`
	IsPrimary       bool   `json:"is_primary" yaml:"is_primary"`
	IsNullable      bool   `json:"is_nullable" yaml:"is_nullable"`
	IsAutoIncrement bool   `json:"is_auto_increment" yaml:"is_auto_increment"`
	Extra           string `json:"extra"`
}

type Trigger struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

type Index struct {
	Name    string   `json:"name"`
	Type    string   `json:"type"`
	Columns []string `json:"columns"`
}

type ForeignKey struct {
	Name       string `json:"name"`
	From       string `json:"from"`
	FromColumn string `json:"from_column" yaml:"from_column"`
	To         string `json:"to"`
	ToColumn   string `json:"to_column" yaml:"to_column"`
}

type Table struct {
	Name        string       `json:"name"`
	Source      string       `json:"source"`
	Columns     []Column     `json:"columns"`
	Indices     []Index      `json:"indices"`
	Triggers    []Trigger    `json:"triggers"`
	ForeignKeys []ForeignKey `json:"foreign_keys" yaml:"foreign_keys"`
}
