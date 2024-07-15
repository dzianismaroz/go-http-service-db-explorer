package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// !!!!! Here you write the code.
// !!!!! Please note that global variables are not allowed in this task.

const (
	UnknownTableErr         = "unknown table"              // If requested table does not exist in database.
	RecordNotFoungErr       = "record not found"           // No entries found in table by criteria.
	InvalidIDTypeErrParrern = "field %s have invalid type" // Cleint submited invalid field for persitence in database.
)

type Response struct {
	status int
	Err    string      `json:"error,omitempty"`    // any error representation
	Resp   interface{} `json:"response,omitempty"` // any content as response
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

func Resp(content interface{}, status int, e error) Response {
	errContent := ""
	if e != nil {
		errContent = e.Error()
	}
	return Response{Err: errContent, status: status, Resp: content}
}

// Reply on request with valid json.
func Reply(w http.ResponseWriter, response Response) {
	respBytes, err := json.Marshal(response)
	if err != nil {
		safeWrite(w, response.status, []byte(fmt.Sprintf("{\"error\":\"%s\"}", err)))
		return
	}
	safeWrite(w, response.status, respBytes)
}

func safeWrite(w http.ResponseWriter, statusCode int, content []byte) {
	w.WriteHeader(statusCode)
	_, err := w.Write(content)
	if err != nil {
		http.Error(w, "unexpected error", http.StatusInternalServerError)
	}
}

// Collect table metadata: column names and column types.
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
		// Collect each table metadata and persist in parent struct.
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

func (d *DBExplorer) listColumns(tableName string) string {
	columns := d.metadata[tableName].ColumnNames
	return strings.Join(columns, ",")
}

func (d *DBExplorer) countColumns(tableName string) int {
	return len(d.metadata[tableName].ColumnNames)
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
	// table, idParam := path.Split(r.URL.Path)
	p := r.URL.Path
	tokens := strings.Split(p, "/")[1:]
	var tableName string
	var id int64 = -1
	fmt.Println("parsing request: tokens len:", len(tokens), "tokens content:", tokens)
	if len(tokens) > 0 {
		tableName = tokens[0]
	}
	if len(tokens) > 1 && tokens[1] != "" {
		fmt.Printf("\"%s\"\n", tokens[1])
		candiate, err := strconv.ParseInt(tokens[1], 10, 64)
		if err != nil {
			return nil, err
		}
		id = candiate
	}

	return &Req{table: tableName, id: id, params: r.URL.Query()}, nil
}

func (r *Req) isTableListRequest() bool {
	return r.table == ""
}

func (r *Req) isFromTable() bool {
	return len(r.table) > 1 && r.id < 0
}

func (r *Req) isById() bool {
	return r.isFromTable() && r.id > 0
}

// func getId(p string) (int64, bool) {
// 	str := path.Base(p)
// 	if str == "" {
// 		return -1, false
// 	}
// 	id, err := strconv.ParseInt(str, 10, 64)
// 	if err != nil {
// 		return -1, false
// 	}
// 	return id, true
// }

// -------------------------------- Handlers --------------------------------------
func (d *DBExplorer) handleGet(w http.ResponseWriter, r *http.Request) {
	req, err := parse(r)
	if err != nil {
		fmt.Println("error detected while parsing request:", err)
		Reply(w, Resp(nil, http.StatusInternalServerError, err))
		return
	}
	fmt.Printf("%+v\n", req)
	// fmt.Printf("path [%s] : %s\n", req., params)
	// fmt.Println("path", path_, "base", path.Base(path_), "clear", path.Clean(path_))
	switch {
	case req.isTableListRequest():
		Reply(w, Resp(map[string][]string{"tables": d.ListTables()}, http.StatusOK, nil))
	case req.isFromTable():
		fmt.Println("requesting from table !, ", req.table)
		res, err := d.query(req)
		if err != nil {
			Reply(w, Resp(nil, http.StatusNotFound, err))
			return
		}
		Reply(w, Resp(res, http.StatusOK, nil))
	case req.isById():
		_, err := d.queryBy(req.id)
		if err != nil {
			Reply(w, Resp(nil, http.StatusNotFound, err))
			return
		}
	default:
		Reply(w, Resp(nil, http.StatusNotFound, errors.New("no such endpoint")))
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

func (d DBExplorer) FindFrom(params []url.Values) ([]interface{}, error) {
	switch {
	case len(params) < 1:
		return nil, errors.New("invalid")
	}
	return nil, nil
}

func (d *DBExplorer) queryBy(id int64) (interface{}, error) {
	return nil, nil
}

func (d *DBExplorer) query(r *Req) (result interface{}, err error) {
	if r.table == "" {
		return nil, errors.New("bad request")
	}

	_, ok := d.metadata[r.table]
	if !ok {
		return nil, errors.New(UnknownTableErr)
	}

	if len(r.params) < 1 {
		columns := d.listColumns(r.table)
		colsCount := d.countColumns(r.table)
		sql := fmt.Sprintf("select %s from %s", columns, r.table)
		fmt.Println("equering:", sql)
		rows, err := d.db.Query(sql)

		if err != nil {
			return nil, err
		}

		for rows.Next() {
			columns := make([]string, colsCount)
			columnPtrs := make([]interface{}, colsCount)
			for i := 0; i < colsCount; i++ {
				columnPtrs[i] = &columns[i]
			}
			// for i := 0; i < len(result); i++ {
			// rows.Scan()
			// }
			err := rows.Scan(columnPtrs...)
			if err != nil {
				fmt.Println("error while scanning results:", err)
			}
			fmt.Printf("scanned: %+v\n", columns)
		}
	}
	// resolve query type
	// 1. all ftom table ?
	// if p == path.Base(p) {

	// }
	// 2. single row by id ?
	// 3. query by criteria ( limits)
	return nil, nil
}
