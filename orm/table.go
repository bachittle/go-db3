package orm

import (
	"database/sql"
	"errors"
	"reflect"
	"strings"
)

type namelist = []string
type nameset = map[string]struct{}

// Querier is a generic db query runner, typically should be hooked to sql.Tx or sql.DB.
type Querier interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type Table struct {
	Name    string
	columns nameset
}

// GetTable queries table columns from src.
func GetTable(src Querier, table_name string) (*Table, error) {
	table := Table{Name: table_name, columns: nameset{}}

	q := "pragma table_info([" + table_name + "])"
	err := query(src, q, nil,
		func(row *sql.Rows) error {
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
			table.columns[name] = struct{}{}
			return nil
		})
	if err != nil {
		return nil, err
	}

	if len(table.columns) == 0 {
		var exists bool
		err := query(src, "select exists(select 1 from sqlite_master where type='table' and name=?)",
			[]any{table_name}, func(row *sql.Rows) error {
				return row.Scan(&exists)
			})
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrTableDoesNotExist
		} else {
			return nil, ErrEmptyTableSchema
		}
	}

	return &table, nil
}

func all_table_names(src Querier) (map[string]struct{}, error) {
	m := map[string]struct{}{}
	err := query(src, "select name from sqlite_master where type='table'", nil, func(row *sql.Rows) error {
		var n string
		err := row.Scan(&n)
		if err == nil {
			m[n] = struct{}{}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return m, nil
}

// GetTables queries the set of tables from src.
//
// Prefix table name with ? to make it optional.
func GetTables(src Querier, table_names ...string) (tt map[string]*Table, err error) {
	existing, err := all_table_names(src)
	if err != nil {
		return
	}
	matching := map[string]struct{}{}
	missing := []string{}
	for _, n := range table_names {
		if len(n) == 0 {
			continue
		}
		optional := n[0] == '?'
		if optional {
			n = n[1:]
			if len(n) == 0 {
				continue
			}
		}
		if _, ok := existing[n]; ok {
			matching[n] = struct{}{}
		} else if !optional {
			missing = append(missing, n)
		}
	}
	if len(missing) > 0 {
		return nil, ErrMissingTables(missing)
	}
	if len(matching) == 0 {
		return nil, nil
	}

	tt = map[string]*Table{}
	for n := range matching {
		t, err := GetTable(src, n)
		if err != nil {
			return nil, err
		}
		tt[n] = t
	}

	return tt, nil
}

func (t *Table) HasColumn(column_name string) bool {
	_, ok := t.columns[column_name]
	return ok
}

type bindings struct {
	receivers []interface{}
	selectors namelist
	missing   namelist
}

// bind_receivers creates a list of receivers for filds in dst that match the orm description.
func (t *Table) bind_receivers(dst any) (*bindings, error) {
	dst_v := reflect.ValueOf(dst)
	if dst_v.Kind() != reflect.Ptr {
		panic("invalid binding")
	}

	struct_value := dst_v.Elem()
	if struct_value.Kind() != reflect.Struct {
		panic("target must be a struct")
	}

	bb := &bindings{}
	bind_struct_fields(t.columns, bb, false, &struct_value)

	if len(bb.missing) > 0 {
		return nil, ErrMissingColumns(bb.missing)
	} else if len(bb.selectors) == 0 {
		return nil, ErrNoBindingsProduced
	}

	return bb, nil
}

func bind_struct_fields(columns nameset, bb *bindings, all_optional bool, struct_v *reflect.Value) {
	for i := 0; i < struct_v.NumField(); i++ {
		field_v := struct_v.Field(i)
		field_t := struct_v.Type().Field(i)

		tag := field_t.Tag
		orm_content := tag.Get("orm")

		if field_t.Type.Kind() == reflect.Struct && (orm_content == "!" || orm_content == "?") {
			optional := all_optional || orm_content == "?"
			bind_struct_fields(columns, bb, optional, &field_v)
			continue
		}

		if orm_content == "" {
			continue
		}

		optional := orm_content[0] == '?'
		if optional {
			orm_content = orm_content[1:]
			if orm_content == "" {
				panic("invalid orm tag syntax " + orm_content + " in field " + field_t.Name)
			}
		}
		if all_optional {
			optional = true
		}

		orm := ""
		for _, orm_term := range strings.Split(orm_content, "|") {
			if _, exists := columns[orm_term]; exists {
				orm = orm_term
				break
			}
		}

		if orm == "" {
			if !optional {
				bb.missing = append(bb.missing, orm_content)
			}
			continue
		}

		dst := field_v.Addr().Interface()
		bb.receivers = append(bb.receivers, dst)
		bb.selectors = append(bb.selectors, orm)
	}
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

var ErrTableDoesNotExist = errors.New("table does not exist")
var ErrEmptyTableSchema = errors.New("empty table schema")
var ErrNoBindingsProduced = errors.New("failed to produce any field bindings")

// ErrMissingColumns is a list of column names that can be used as the 'missing
// columns' error.
type ErrMissingColumns []string

func (e ErrMissingColumns) Error() string {
	return "missing columns: " + strings.Join([]string(e), ", ")
}

// ErrMissingTables is a list of table names that can be used as the 'missing
// tables' error.
type ErrMissingTables []string

func (e ErrMissingTables) Error() string {
	return "missing tables: " + strings.Join([]string(e), ", ")
}
