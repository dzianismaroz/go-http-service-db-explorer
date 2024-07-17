package main

import (
	"database/sql"
	"encoding/json"
	"errors"
)

// To handle raw results from database.
type RowResult struct {
	metadata TableMetadata
	entries  DBEntries
}

func newRowResult(metadata TableMetadata) *RowResult {
	return &RowResult{metadata: metadata, entries: make(DBEntries, 0, 20)}
}

// Extract raw data from single row.
func (r *RowResult) handleSingleRowResult(row *sql.Row) (interface{}, error) {
	colsCount := len(r.metadata.columnsInfo)
	columnVals := make([]interface{}, colsCount) // Content holder.
	for i := 0; i < colsCount; i++ {
		columnVals[i] = &columnVals[i]
	}
	err := row.Scan(columnVals...)
	if err != nil {
		return nil, errors.New("record not found")
	}
	entry := make(DBEntry, 1)
	for i := 0; i < colsCount; i++ {
		switch {
		case columnVals[i] == nil:
			entry[r.metadata.columnNames[i]] = nil
		default:
			if r.metadata.columnsInfo[i].isNumericType {
				intVal, _ := columnVals[i].(int64)
				entry[r.metadata.columnNames[i]] = intVal
			} else {
				entry[r.metadata.columnNames[i]] = string(columnVals[i].([]byte))
			}
		}
	}
	result := map[string]DBEntry{"record": entry}
	return result, nil
}

func (r *RowResult) handleMultiRowResult(rows *sql.Rows) error {
	colsCount := len(r.metadata.columnNames)
	columnVals := make([]interface{}, colsCount)

	for rows.Next() {
		for i := 0; i < colsCount; i++ {
			columnVals[i] = &columnVals[i]
		}
		err := rows.Scan(columnVals...)
		if err != nil {
			return err
		}
		entry := make(DBEntry, 1)
		for i := 0; i < colsCount; i++ {
			switch {
			case columnVals[i] == nil:
				entry[r.metadata.columnNames[i]] = nil
			default:
				if !r.metadata.columnsInfo[i].isNumericType {
					entry[r.metadata.columnNames[i]] = string(columnVals[i].([]byte))
				} else {
					intVal, _ := columnVals[i].(int64)
					entry[r.metadata.columnNames[i]] = intVal
				}
			}
		}
		r.entries = append(r.entries, entry)
	}
	_, err := json.Marshal(r.entries)
	return err
}
