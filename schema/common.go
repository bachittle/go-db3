package schema

import (
	"encoding/json"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type ColumnType string

const (
	Untyped   = ColumnType("")
	Bool      = ColumnType("bool")
	Int       = ColumnType("int")
	Int64     = ColumnType("int64")
	Float     = ColumnType("float")
	Text      = ColumnType("text")
	Blob      = ColumnType("blob")
	Time      = ColumnType("time")
	Date      = ColumnType("date")
	Timestamp = ColumnType("timestamp")
	UUID      = ColumnType("uuid")
)

type Literal interface {
	json.Marshaler
	yaml.Marshaler
	SQLLiteral() string
}

type LiteralInt int64
type RawLiteral string
type LiteralBoolean bool
type CurrentTime struct{}
type CurrentDate struct{}
type CurrentTimestamp struct{}
type NULL struct{}

func (v LiteralInt) SQLLiteral() string           { return strconv.FormatInt(int64(v), 10) }
func (v LiteralInt) MarshalJSON() ([]byte, error) { return []byte(v.SQLLiteral()), nil }
func (v LiteralInt) MarshalYAML() (any, error)    { return int64(v), nil }

func (v RawLiteral) SQLLiteral() string           { return string(v) }
func (v RawLiteral) MarshalJSON() ([]byte, error) { return []byte(v), nil }
func (v RawLiteral) MarshalYAML() (any, error)    { return string(v), nil }

func (v LiteralBoolean) SQLLiteral() string           { return strconv.FormatBool(bool(v)) }
func (v LiteralBoolean) MarshalJSON() ([]byte, error) { return []byte(v.SQLLiteral()), nil }
func (v LiteralBoolean) MarshalYAML() (any, error)    { return bool(v), nil }

func (v CurrentTime) SQLLiteral() string           { return "CURRENT_TIME" }
func (v CurrentTime) MarshalJSON() ([]byte, error) { return []byte(`"CURRENT_TIME"`), nil }
func (v CurrentTime) MarshalYAML() (any, error)    { return v.SQLLiteral(), nil }

func (v CurrentDate) SQLLiteral() string           { return "CURRENT_DATE" }
func (v CurrentDate) MarshalJSON() ([]byte, error) { return []byte(`"CURRENT_DATE"`), nil }
func (v CurrentDate) MarshalYAML() (any, error)    { return v.SQLLiteral(), nil }

func (v CurrentTimestamp) SQLLiteral() string           { return "CURRENT_TIMESTAMP" }
func (v CurrentTimestamp) MarshalJSON() ([]byte, error) { return []byte(`"CURRENT_TIMESTAMP"`), nil }
func (v CurrentTimestamp) MarshalYAML() (any, error)    { return v.SQLLiteral(), nil }

func (v NULL) SQLLiteral() string           { return "null" }
func (v NULL) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (v NULL) MarshalYAML() (any, error)    { return nil, nil }

func parseLiteral(s string) Literal {
	switch strings.ToLower(s) {
	case "null":
		return NULL{}
	case "current_time":
		return CurrentTime{}
	case "current_date":
		return CurrentDate{}
	case "current_timestamp":
		return CurrentTimestamp{}
	case "true":
		return LiteralBoolean(true)
	case "false":
		return LiteralBoolean(false)
	default:
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return LiteralInt(v)
		} else {
			return RawLiteral(requote(s))
		}
	}
}

func requote(s string) string {
	n := len(s)
	if n >= 2 && s[0] == '\'' && s[n-1] == '\'' {
		return `"` + s[1:n-1] + `"`
	} else {
		return s
	}
}
