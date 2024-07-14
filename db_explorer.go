package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"
)

// !!!!! Here you write the code.
// !!!!! Please note that global variables are not allowed in this task.

const (
	UnknownTableErr         = "unknown table"              // If unknown table was requested.
	RecordNotFoungErr       = "record not found"           // No entries in table at all or by specified id found.
	InvalidIDTypeErrParrern = "field %s have invalid type" // Client submited invalid field or type
)

type Response struct {
	Err  string      `json:"error,omitempty"`    // any error representation
	Resp interface{} `json:"response,omitempty"` // any content as response
}

// !!delete candidate
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
	ColumnNames []string
	ColumnTypes []*sql.ColumnType
}

type DBExplorer struct {
	db         *sql.DB // database handler
	TableNames TablesList
	metadata   map[string]TableMetadata
}

type Req struct {
	table  string
	id     int64
	params url.Values
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
		tableMetadata := TableMetadata{ColumnTypes: colTypes, ColumnNames: colNames}
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
	result.TableNames = TablesList{Tables: tablesNames}
	for _, tableName := range tablesNames {
		// collect each table metadata and persist in parent struct
		err := result.getColumnMetadata(tableName)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// Return table names, resolved on initialization. We can suspect it to be static.
func (d *DBExplorer) ListTables() []string {
	return d.TableNames.Tables
}

// Simple request tracking to StdOut.
func (d *DBExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fmt.Printf(":::: %s [%s] : %s {%#v}\n", time.Now().Format(time.DateTime), r.Method, r.URL.Path, r.URL.Query())
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

// ------------------ parse resuqest params ---------------------

func parse(r *http.Request) (presult *Req, err error) {
	path := r.URL.Path
	tokens := strings.Split(path, "/")
	var tableName string
	var id int64
	if len(tokens) > 0 {
		tableName = tokens[0]
	}
	if len(tokens) > 1 {
		candiate, err := strconv.ParseInt(tokens[1], 10, 64)
		if err != nil {
			return nil, err
		}
		id = candiate
	}

	return &Req{table: tableName, id: id, params: r.URL.Query()}, nil
}

func (r *Req) isTableRequest() bool {
	return r.table == ""
}

func getId(p string) (int64, bool) {
	str := path.Base(p)
	if str == "" {
		return -1, false
	}
	id, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return -1, false
	}
	return id, true
}

// -------------------------------- Handlers --------------------------------------
func (d *DBExplorer) handleGet(w http.ResponseWriter, r *http.Request) {
	req, err := parse(r)
	if err != nil {
		Reply(w, http.StatusInternalServerError, Resp(nil, err))
		return
	}
	// fmt.Printf("path [%s] : %s\n", req., params)
	// fmt.Println("path", path_, "base", path.Base(path_), "clear", path.Clean(path_))
	switch {
	case req.isTableRequest():
		Reply(w, http.StatusOK, Resp(map[string][]string{"tables": d.ListTables()}, nil))
	default:
		status := http.StatusOK
		result, err := d.query(req)
		if err != nil {
			status = http.StatusInternalServerError
		}
		Reply(w, status, Resp(result, err))
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

func (d *DBExplorer) query(r *Req) (result interface{}, err error) {
	// resolve query type
	// 1. all ftom table ?
	// if p == path.Base(p) {

	// }
	// 2. single row by id ?
	// 3. query by criteria ( limits)
	return nil, nil
}
