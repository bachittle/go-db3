package schema

import (
	"gopkg.in/yaml.v3"
)

// Database contains table schemas, typically obtained when calling the Scan
// routine on a database connection.
type Database struct {
	Tables []*Table `json:"tables" yaml:"tables"`
}

// Table contains the descriptions for columns and indices within a table.
type Table struct {
	Name         string    `json:"table" yaml:"table"`
	Columns      []*Column `json:"columns,omitempty" yaml:"columns,omitempty"`
	Indices      []*Index  `json:"indices,omitempty" yaml:"indices,omitempty"`
	PK           []string  `json:"pk,omitempty" yaml:"pk,omitempty,flow"`
	WithoutRowID bool      `json:"without_rowid,omitempty" yaml:"without_rowid,omitempty"`
	Strict       bool      `json:"strict,omitempty" yaml:"strict,omitempty"`
}

// Column contains schema scan results for column within a table.
type Column struct {
	Name     string     `json:"name" yaml:"name"`
	Type     ColumnType `json:"type,omitempty" yaml:"type,omitempty"`
	Nullable bool       `json:"nullable,omitempty" yaml:"nullable,omitempty"`
	Default  Literal    `json:"default,omitempty" yaml:"default,omitempty"`
	Comment  string     `json:"comment,omitempty" yaml:"comment,omitempty"`
}

// Index contains schema scan results for table's index.
type Index struct {
	Name    string   `json:"name" yaml:"name"`
	Unique  bool     `json:"unique,omitempty" yaml:"unique,omitempty"`
	Columns []string `json:"columns,omitempty" yaml:"columns,omitempty"`
}

func (db *Database) HasTable(tablename string) bool {
	for _, t := range db.Tables {
		if t.Name == tablename {
			return true
		}
	}
	return false
}

// CheckTables validates existance of the specified tables.
func (db *Database) CheckTables(names ...string) (missing ErrMissingTables) {
	for _, n := range names {
		if !db.HasTable(n) {
			missing = append(missing, n)
		}
	}
	return
}

func (t *Table) FindColumn(columnname string) (*Column, bool) {
	for _, c := range t.Columns {
		if c.Name == columnname {
			return c, true
		}
	}
	return nil, false
}

func (t *Table) ColumnMapping() map[string]*Column {
	m := make(map[string]*Column, len(t.Columns))
	for _, c := range t.Columns {
		m[c.Name] = c
	}
	return m
}

func (t *Table) FindIndex(indexname string) (*Index, bool) {
	for _, i := range t.Indices {
		if i.Name == indexname {
			return i, true
		}
	}
	return nil, false
}

func (t *Table) IndexMapping() map[string]*Index {
	m := make(map[string]*Index, len(t.Indices))
	for _, i := range t.Indices {
		m[i.Name] = i
	}
	return m
}

func (c *Column) MarshalYAML() (any, error) {
	// get Columns to appear with flow style
	type flat Column
	f := (*flat)(c)
	n := yaml.Node{}
	n.Encode(f)
	n.Style = yaml.FlowStyle
	return &n, nil
}

// UnmarshalYAML fixes issue where Literal is not unmarshalled correctly.
func (c *Column) UnmarshalYAML(node *yaml.Node) error {
	// iterate over all fields and unmarshal them
	for i := 0; i < len(node.Content); i += 2 {
		key := node.Content[i].Value
		value := node.Content[i+1]
		switch key {
		case "name":
			c.Name = value.Value
		case "type":
			c.Type = ColumnType(value.Value)
		case "nullable":
			c.Nullable = value.Value == "true"
		case "default":
			// c.Default = Literal(value.Value)
			c.Default = nil // TODO: fix this
		case "comment":
			c.Comment = value.Value
		}
	}
	return nil
}

func (i *Index) MarshalYAML() (any, error) {
	// get Indices to appear with flow style
	type flat Index
	f := (*flat)(i)
	n := yaml.Node{}
	n.Encode(f)
	n.Style = yaml.FlowStyle
	return &n, nil
}
