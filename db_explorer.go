package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// !!!!! Here you write the code.
// !!!!! Please note that global variables are not allowed in this task.

const (
	UnknownTableErr         = "unknown table"              // If unknown table was requested.
	RecordNotFoungErr       = "record not found"           // No entry in table at all or by specified id found.
	InvalidIDTypeErrParrern = "field %s have invalid type" // Client submitted invalid field or type
)

type Response struct {
	Err  string      `json:"error,omitempty"`
	Resp interface{} `json:"response,omitempty"`
}

type Column struct {
	Field     string // name of column
	Type      string
	Collation string
	Null      string
	Key       string
}

type TablesList struct {
	Tables []string `json:"tables"`
}

type TableMetadata struct {
	columnNames []string
	columnTypes []*sql.ColumnType
}

type DBExplorer struct {
	db         *sql.DB  // database handler
	tableNames []string //
	metadata   map[string]TableMetadata
}

func Resp(content interface{}, e error) Response {
	errContent := ""
	if e != nil {
		errContent = e.Error()
	}
	return Response{Err: errContent, Resp: content}
}

// Reply with valid json to request.
func Reply(w http.ResponseWriter, status int, response Response) {
	respBytes, err := json.Marshal(response)
	if err != nil {
		safeWrite(w, status, []byte(fmt.Sprintf("{\"error\":\"%s\"}", err)))
		return
	}
	safeWrite(w, status, respBytes)
}

func safeWrite(w http.ResponseWriter, statusCode int, content []byte) {
	w.WriteHeader(statusCode)
	_, err := w.Write(content)
	if err != nil {
		http.Error(w, "unexpected error", http.StatusInternalServerError)
	}
}

// Collect table metadata: column names and column types to aggign it to DBExplorer.
func (d *DBExplorer) getColumnMetadata(tableName string) error {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", tableName)

	rows, err := d.db.Query(query)
	if err != nil {
		return err
	}
	for rows.Next() {
		colTypes, err := rows.ColumnTypes()
		if err != nil {
			return err
		}
		colNames, err := rows.Columns()
		if err != nil {
			return err
		}
		tableMetadata := TableMetadata{columnTypes: colTypes, columnNames: colNames}
		d.metadata[tableName] = tableMetadata
	}
	return nil
}

func NewDbExplorer(db *sql.DB) (*DBExplorer, error) {
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(2 * time.Second)
	// Just verify db connection once again. `who knows....`
	if connErr := db.Ping(); connErr != nil {
		return nil, connErr
	}
	result := &DBExplorer{db: db, metadata: make(map[string]TableMetadata, 10)}
	tablesNames := result.findTableNames()
	if len(tablesNames) < 1 {
		return nil, errors.New("no tables in database")
	}
	result.tableNames = tablesNames
	for _, tableName := range tablesNames {
		// collect each table metadata and persist in parent struct
		err := result.getColumnMetadata(tableName)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Return table names, resolved on initialization step.
// We can suspect it to be static.
func (d *DBExplorer) ListTables() []string {
	return d.tableNames
}

func (d *DBExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Printf(":::: %s [%s] : %s {%#v}\n", time.Now().Format(time.DateTime), r.Method, r.URL.Path, r.URL.Query()) // simple request tracking to std::out
	d.route(w, r)
}

// -------------------------------- Router   --------------------------------------
func (d *DBExplorer) route(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		d.handleGet(w, r)
	case http.MethodPost:
		d.handlePost(w, r)
	case http.MethodPut:
		d.handlePut(w, r)
	case http.MethodDelete:
		d.handleDelete(w, r)
	default:
		http.Error(w, "invalid request", http.StatusNotFound)
	}
}

// -------------------------------- Handlers --------------------------------------
func (d *DBExplorer) handleGet(w http.ResponseWriter, r *http.Request) {
	pathTokens := strings.Fields(strings.ReplaceAll(r.URL.Path, "/", ""))
	fmt.Printf("path [%d] : %s\n", len(pathTokens), pathTokens)
	switch {
	case len(pathTokens) < 1:
		Reply(w, http.StatusOK, Resp(map[string][]string{"tables": d.ListTables()}, nil))
	case len(pathTokens) == 1:
		switch {
		case strings.Contains(pathTokens[0], "?"):
			//d.GetEntriesFrom(w, pathTokens)
		default:
			d.FindFrom(pathTokens)
			Reply(w, http.StatusNotFound, Resp(nil, errors.New(UnknownTableErr)))
		}
	}
}

func (d *DBExplorer) handlePost(w http.ResponseWriter, r *http.Request) {

}

func (d *DBExplorer) handlePut(w http.ResponseWriter, r *http.Request) {

}

func (d *DBExplorer) handleDelete(w http.ResponseWriter, r *http.Request) {

}

// ---------------------------- DB executors ------------------

func (e *DBExplorer) findTableNames() []string {
	if rows, err := e.db.Query("SHOW TABLES;"); err != nil {
		return nil
	} else {
		tablesNames := make([]string, 0, 10)
		var tableName string
		for rows.Next() {

			if err := rows.Scan(&tableName); err != nil {
				return nil
			} else {
				tablesNames = append(tablesNames, tableName)
			}
		}
		return tablesNames
	}
}

func (d DBExplorer) FindFrom(params []string) ([]interface{}, error) {
	switch {
	case len(params) < 1:
		return nil, errors.New("invalid")
	}
	return nil, nil
}
