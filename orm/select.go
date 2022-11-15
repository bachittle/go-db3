package orm

import "fmt"

// Select enumerates table rows mapping its columns to fields in struct T.
//
// T needs to have its fields tagged with orm attributes:
//
//   - use `orm:"fieldname"“ for mandatory fields
//   - use `orm:"?fieldname"` for optional fields
//   - use `orm:"fieldname|alternative"` to match a field to any of the specified columns
//   - use `orm:"!"` for structural child filds to link to their fields
//   - use `orm:"?"` for structural child filds to optionally link to their fields
//
func Select[T any](src Querier, table *Table, opts Options, on_row func(t *T) error) error {
	// internal temporary that gets populated with results from row.Scan
	var internal_v T

	// receivers references to bound fields in T
	bb, err := table.bind_receivers(&internal_v)
	if err != nil {
		return fmt.Errorf("binding table %s: %w", table.Name, err)
	}

	// statement sql
	select_sql := opts.Sql(table.Name, bb.selectors)
	rows, err := src.Query(select_sql, opts.Args()...)
	if err != nil {
		return fmt.Errorf("querying table %s: %w", table.Name, err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(bb.receivers...)
		if err != nil {
			return fmt.Errorf("scanning table %s: %w", table.Name, err)
		}
		err = on_row(&internal_v)
		if err != nil {
			return fmt.Errorf("scanning table %s: %w", table.Name, err)
		}
	}
	return nil
}

// SelectToSlice appends results of Select enumeration to dst
func SelectToSlice[S ~[]*T, T any](src Querier, table *Table, opts Options, dst *S) error {
	return Select(src, table, opts, func(v *T) error {
		copy := *v
		*dst = append(*dst, &copy)
		return nil
	})
}

// Selector produces an enumerating callable for struct T.
func Selector[T any](src Querier, table *Table) (func(opts Options, callback func(t *T) error) error, error) {
	// internal temporary that gets populated with results from row.Scan
	var internal_v T

	// receivers references to bound fields in T
	bb, err := table.bind_receivers(&internal_v)
	if err != nil {
		return nil, fmt.Errorf("binding table %s: %w", table.Name, err)
	}

	f := func(opts Options, callback func(t *T) error) error {
		rows, err := src.Query(opts.Sql(table.Name, bb.selectors), opts.Args()...)
		if err != nil {
			return fmt.Errorf("querying table %s: %w", table.Name, err)
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(bb.receivers...)
			if err != nil {
				return fmt.Errorf("scanning table %s: %w", table.Name, err)
			}
			err = callback(&internal_v)
			if err != nil {
				return fmt.Errorf("scanning table %s: %w", table.Name, err)
			}
		}
		return nil
	}
	return f, nil
}
