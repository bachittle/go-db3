package schema

import (
	"database/sql"
	"fmt"

	"golang.org/x/exp/slices"
)

// Querier is a generic db query runner, typically should be hooked to sql.Tx or sql.DB.
type Querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// IsEmpty tests if a database has empty schema.
func IsEmpty(src Querier) (bool, error) {
	row := src.QueryRow("select count(*) from sqlite_master;")
	var n int
	err := row.Scan(&n)
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

// Scan obtains all the schema details from the sqlite database.
func Scan(src Querier) (*Database, error) {
	db := &Database{}

	from_master := map[string]struct{}{}
	err := query(src, "select name from sqlite_master where type='table'", nil,
		func(row *sql.Rows) error {
			var n string
			err := row.Scan(&n)
			if err != nil {
				return err
			}
			from_master[n] = struct{}{}
			return nil
		})
	if err != nil {
		return nil, err
	}

	err = query(src, "pragma table_list;", nil,
		func(row *sql.Rows) error {
			var schema string
			var name string
			var typ string
			var ncol int
			var wr int
			var strict int
			err := row.Scan(&schema, &name, &typ, &ncol, &wr, &strict)
			if err != nil {
				return err
			}
			if _, ok := from_master[name]; !ok {
				return nil
			}
			if typ != "table" {
				return nil
			}
			db.Tables = append(db.Tables, &Table{
				Name:         name,
				WithoutRowID: wr == 1,
				Strict:       strict == 1,
			})
			return nil
		})
	if err != nil {
		return nil, err
	}
	for _, table := range db.Tables {
		type pk_info struct {
			i int
			n string
		}
		pk_infos := []*pk_info{}

		q := fmt.Sprintf("pragma table_info([%s])", table.Name)
		err = query(src, q, nil, func(row *sql.Rows) error {
			var cid int
			var name string
			var typeName string
			var notnull int
			var dfltValue sql.NullString
			var pk int
			err := row.Scan(&cid, &name, &typeName, &notnull, &dfltValue, &pk)
			if err != nil {
				return err
			}

			nullable := notnull != 1

			var dflt Literal

			if dfltValue.Valid {
				dflt = parseLiteral(dfltValue.String)
			}

			table.Columns = append(table.Columns, &Column{
				Name:     name,
				Type:     ColumnType(typeName),
				Nullable: nullable,
				Default:  dflt,
			})

			if pk > 0 {
				pk_infos = append(pk_infos, &pk_info{i: pk, n: name})
			}
			return nil
		})
		if err != nil {
			return nil, err
		}

		slices.SortStableFunc(pk_infos, func(a, b *pk_info) bool { return a.i < b.i })
		for _, info := range pk_infos {
			table.PK = append(table.PK, info.n)
		}

		q = fmt.Sprintf("pragma index_list([%s])", table.Name)
		err = query(src, q, nil, func(row *sql.Rows) error {
			var seq int
			var indexName string
			var unique int
			var origin string
			var partial int
			err := row.Scan(&seq, &indexName, &unique, &origin, &partial)
			if err != nil {
				return err
			}
			index := &Index{
				Name:    indexName,
				Unique:  unique == 1,
				Columns: []string{},
			}
			err = query(src, fmt.Sprintf("pragma index_info([%s])", indexName), nil, func(row *sql.Rows) error {
				var seqno int
				var cid int
				var name string
				err := row.Scan(&seqno, &cid, &name)
				if err != nil {
					return err
				}
				index.Columns = append(index.Columns, name)
				return nil
			})
			if err != nil {
				return err
			}
			table.Indices = append(table.Indices, index)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}

// ValidateColumns checks table schema for presense of columns with the
// specified names. Column names prefixed with '?' are considered optional.
// Columns named as 'NULL' are passed through without validation.
func (table *Table) ValidateColumns(names ...string) (validated []string, optmissing []string, missing ErrMissingColumns) {
	validated = make([]string, 0, len(names))
	m := table.ColumnMapping()
	for _, n := range names {
		if n == "" {
			panic("invalid column specification") // library usage error
		}
		if n == "NULL" {
			validated = append(validated, n)
			continue
		}
		optional := n[0] == '?'
		if optional {
			n = n[1:]
		}
		if _, ok := m[n]; ok {
			validated = append(validated, n)
		} else if optional {
			validated = append(validated, "NULL")
			optmissing = append(optmissing, n)
		} else {
			missing = append(missing, n)
		}
	}
	return
}

// CheckColumnTypes checks if the specified database columns match the signatures
// required by the datamodel.
func (table *Table) CheckColumnTypes(required map[string]*Column) *ErrTableColumns {
	err := &ErrTableColumns{}
	m := table.ColumnMapping()
	for n, rc := range required {
		if tc, exists := m[n]; !exists {
			err.Missing = append(err.Missing, n)
		} else if !tc.CompatibleTo(rc) {
			err.Incompatible = append(err.Incompatible, n)
		}
	}
	if len(err.Missing) > 0 || len(err.Incompatible) > 0 {
		return err
	} else {
		return nil
	}
}

func (table *Table) ColumnNames() map[string]struct{} {
	r := make(map[string]struct{}, len(table.Columns))
	for _, c := range table.Columns {
		r[c.Name] = struct{}{}
	}
	return r
}

// CheckIndices validates if database signatures of specified indices match
// the indices specified in the data model.
func (table *Table) CheckIndices(required map[string]*Index) *ErrIndices {
	err := &ErrIndices{}
	for n, want := range required {
		if have, exists := table.FindIndex(n); !exists {
			err.Missing = append(err.Missing, n)
		} else if !have.CompatibleTo(want) {
			err.Incompatible = append(err.Incompatible, n)
		}
	}
	if len(err.Missing) > 0 || len(err.Incompatible) > 0 {
		return err
	} else {
		return nil
	}
}

// CompatibleTo returns true if both column signatures are compatible.
func (column *Column) CompatibleTo(info *Column) bool {
	return column.Nullable == info.Nullable
}

func (idx *Index) CompatibleTo(other *Index) bool {
	return idx.Unique == other.Unique &&
		slices.Equal(idx.Columns, other.Columns)
}

func query(src Querier, q string, args []any, on_row func(row *sql.Rows) error) error {
	rows, err := src.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		err = on_row(rows)
		if err != nil {
			return err
		}
	}
	return nil
}
