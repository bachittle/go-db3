package schema

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

type CreateFlag int

const TemporaryTable = CreateFlag(1)

func (t *Table) CreateStatements(w io.Writer, flags ...CreateFlag) {
	out := &bytes.Buffer{}

	temporary := ""
	for _, f := range flags {
		if f == TemporaryTable {
			temporary = "temporary"
		}
	}

	fmt.Fprintf(out, "create %stable %s (",
		temporary, t.Name)

	first := true

	indented := func() {
		if first {
			first = false
		} else {
			out.WriteByte(',')
		}
		out.WriteString("\n    ")
	}

	grid := table_grid{}
	col_widths := []int{}
	for _, f := range t.Columns {
		row := []string{f.Name}

		if s := string(f.Type); s != "" {
			row = append(row, s)
		}

		attrs := []string{}
		if !f.Nullable {
			attrs = append(attrs, "not null")
		}
		if f.Default != nil {
			attrs = append(attrs, "default "+f.Default.SQLLiteral())
		}
		if len(attrs) > 0 {
			if len(row) < 2 {
				row = append(row, "")
			}
			row = append(row, strings.Join(attrs, " "))
		}
		measure_cells(row, &col_widths)
		grid = append(grid, row)
	}

	for _, row := range grid {
		indented()
		for col_idx := range row {
			if col_idx > 0 {
				out.WriteString("  ")
			}
			col := 0
			if col_idx < len(col_widths) {
				col = col_widths[col_idx]
			}
			adv := measure_cell(row[col_idx])
			if col < adv {
				col = adv
			}
			out.WriteString(row[col_idx])
			if col > adv && col_idx+1 < len(row) {
				n := col - adv
				for n >= 8 {
					out.WriteString("        ")
					n -= 8
				}
				out.WriteString("        "[:n])
			}
		}
	}

	if len(t.PK) > 0 {
		indented()
		out.WriteString("primary key (" + strings.Join(t.PK, ",") + ")")
	}

	out.WriteString("\n)")
	options := []string{}
	if t.WithoutRowID {
		options = append(options, "without rowid")
	}
	if t.Strict {
		options = append(options, "strict")
	}
	if len(options) > 0 {
		out.WriteString(" " + strings.Join(options, ", "))
	}
	out.WriteString(";\n")

	for _, idx := range t.Indices {
		out.WriteString(t.CreateIndexStatement(idx))
		out.WriteByte('\n')
	}

	w.Write(out.Bytes())
}

func (t *Table) CreateIndexStatement(idx *Index) string {
	n, u := idx.Name, ""
	if len(n) == 0 {
		// auto-generate
		n = t.Name + "_" + strings.Join(idx.Columns, "_") + "_index"
	}
	if idx.Unique {
		u = "unique "
	}
	return fmt.Sprintf("create %sindex %s on %s(%s);",
		u, n, t.Name, strings.Join(idx.Columns, ","))
}

type table_grid [][]string

func measure_cell(s string) int {
	return len([]rune(s))
}

func measure_cells(row []string, col_widths *[]int) {
	for i := range row {
		w := measure_cell(row[i])
		if i < len(*col_widths) {
			if w > (*col_widths)[i] {
				(*col_widths)[i] = w
			}
		} else {
			*col_widths = append(*col_widths, w)
		}
	}
}
