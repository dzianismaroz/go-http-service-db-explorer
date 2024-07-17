package main

import "strings"

// Non-exported struct to collect required information about database column.
type ColumnMetadata struct {
	fieldName       string // Name of column
	isNumericType   bool   // Is it a numeric type
	isNullable      bool
	isAutoIncrement bool
}

type TableMetadata struct {
	columnsInfo []ColumnMetadata
	columnNames []string                  // Easier way to iterate column names.
	hash        map[string]ColumnMetadata // Faster way to obtain column info by it's name.
}

// Internal function to build Column Info based on attributes from database.
func newColumnInfo(fieldName, fType, extra, null string) ColumnMetadata {
	return ColumnMetadata{
		fieldName:       fieldName,                            // Name of column.
		isNumericType:   strings.Contains(fType, "int"),       // Column type [ numeric / text].
		isNullable:      null == "YES",                        // Nullability of column.
		isAutoIncrement: strings.Contains(extra, "increment")} // Is column auto-incremental.
}

// Obtain column's info by it's name.
func (t TableMetadata) getColumn(name string) ColumnMetadata {
	return t.hash[name]
}
