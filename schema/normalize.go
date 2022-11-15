package schema

import (
	"strings"

	"golang.org/x/exp/slices"
)

func NormalizeType(s ColumnType) ColumnType {
	m := strings.ToLower(string(s))
	switch m {
	case "int", "integer", "tinyint", "smallint", "mediumint":
		return Int
	case "int64", "bigint":
		return Int64
	case "boolean", "bool":
		return Bool
	case "real", "double", "float":
		return Float
	case "blob":
		return Blob
	case "text", "string", "clob":
		return Text
	case "date":
		return Date
	case "time":
		return Time
	case "datetime", "timestamp":
		return Timestamp
	case "uuid", "guid":
		return UUID
	default:
		for _, prefix := range []string{
			"character(",
			"varchar(",
			"nchar(",
			"nvarchar(",
		} {
			if strings.HasPrefix(m, prefix) {
				return Text
			}
		}

	}

	return s
}

func NormalizeDefault(c *Column) {
	if !c.Nullable || c.Default == nil {
		return
	}
	switch d := (c.Default).(type) {
	case NULL:
		c.Default = nil
	case RawLiteral:
		if strings.ToLower(string(d)) == "null" {
			c.Default = nil
		}
	}
}

func NormalizeNames(t *Table, uppercase bool) {
	norm := func(s string) string {
		if uppercase {
			return strings.ToUpper(s)
		} else {
			return strings.ToLower(s)
		}
	}

	t.Name = norm(t.Name)
	for _, c := range t.Columns {
		c.Name = norm(c.Name)
	}

	for _, i := range t.Indices {
		i.Name = norm(i.Name)
	}
}

func SortColumns(t *Table) {
	slices.SortStableFunc(t.Columns, func(a, b *Column) bool {
		return a.Name < b.Name
	})
}

func SortIndices(t *Table) {
	slices.SortStableFunc(t.Indices, func(a, b *Index) bool {
		return a.Name < b.Name
	})
}

func SortTables(tt []*Table) {
	slices.SortStableFunc(tt, func(a, b *Table) bool {
		return a.Name < b.Name
	})
}
