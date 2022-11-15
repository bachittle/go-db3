package schema

import (
	"bytes"
	"strings"
)

// ErrMissingTables is a list of column names that can be used as the 'missing
// tables' error.
type ErrMissingTables []string

// ErrMissingColumns is a list of column names that can be used as the 'missing
// columns' error.
type ErrMissingColumns []string

// ErrIncompatibleColumns is a list of column names that can be used as the
// 'incimpatible columns' error.
type ErrIncompatibleColumns []string

// ErrTableColumns enlists missing and incompatible columns in a table.
type ErrTableColumns struct {
	Missing      ErrMissingColumns
	Incompatible ErrIncompatibleColumns
}

// ErrMissingIndices is a list of index names that can be used as the 'missing
// columns' error.
type ErrMissingIndices []string

// ErrIncompatibleIndices is a list of index names that can be used as the
// 'incimpatible columns' error.
type ErrIncompatibleIndices []string

// ErrIndices enlists missing and incompatible indices in a table.
type ErrIndices struct {
	Missing      ErrMissingIndices
	Incompatible ErrIncompatibleIndices
}

// Error implements support for the standard error interface.
func (e ErrMissingTables) Error() string { return msg("missing tables", e) }

// Error implements support for the standard error interface.
func (e ErrMissingColumns) Error() string { return msg("missing columns", e) }

// Error implements support for the standard error interface.
func (e ErrIncompatibleColumns) Error() string { return msg("incompatible columns", e) }

// Error implements support for the standard error interface.
func (e ErrMissingIndices) Error() string { return msg("missing indices", e) }

// Error implements support for the standard error interface.
func (e ErrIncompatibleIndices) Error() string { return msg("incompatible indices", e) }

// Error implements support for the standard error interface.
func (e *ErrTableColumns) Error() string {
	b := bytes.Buffer{}
	b.WriteString("incompatible table schema")
	if len(e.Missing) > 0 {
		b.WriteString(", ")
		b.WriteString(e.Missing.Error())
	}
	if len(e.Incompatible) > 0 {
		b.WriteString(", ")
		b.WriteString(e.Incompatible.Error())
	}
	return b.String()
}

// Error implements support for the standard error interface.
func (e *ErrIndices) Error() string {
	b := bytes.Buffer{}
	b.WriteString("incompatible table indices")
	if len(e.Missing) > 0 {
		b.WriteString(", ")
		b.WriteString(e.Missing.Error())
	}
	if len(e.Incompatible) > 0 {
		b.WriteString(", ")
		b.WriteString(e.Incompatible.Error())
	}
	return b.String()
}

func joined[T ~[]string](names T) string           { return strings.Join([]string(names), ", ") }
func msg[T ~[]string](subj string, names T) string { return subj + ": " + joined(names) }
