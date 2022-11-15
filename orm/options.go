package orm

import "bytes"

type Options interface {
	Sql(tablename string, selectors []string) string
	Args() []any
}

type Condition interface {
	Sql() (sql string, args []any)
}

func Enumerate(conditions ...Condition) Options {
	enm := &enumerate_opts{}
	for _, c := range conditions {
		s, args := c.Sql()
		if s != "" {
			enm.sql_tail += " " + s
		}
		enm.args = append(enm.args, args...)
	}
	return enm
}

type where struct {
	expr string
	args []any
}

func (w *where) Sql() (sql string, args []any) {
	return "where " + w.expr, w.args
}

func Where(expr string, args ...any) Condition {
	return &where{expr: expr, args: args}
}

type enumerate_opts struct {
	sql_tail string
	args     []any
}

func (enm *enumerate_opts) Sql(tablename string, selectors []string) string {
	b := bytes.Buffer{}
	b.WriteString("select ")
	for i := range selectors {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(selectors[i])
	}
	b.WriteString(" from ")
	b.WriteString(tablename)

	if enm.sql_tail != "" {
		b.WriteByte(' ')
		b.WriteString(enm.sql_tail)
	}
	return b.String()
}

func (e *enumerate_opts) Args() []any { return e.args }
