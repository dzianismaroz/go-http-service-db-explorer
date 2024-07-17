package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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
	BadRequest              = "BAD_REQUEST"
	defaultLimit            = 5
	defaultOffest           = 0
)

type DBEntry = map[string]interface{}

type DBEntries = []DBEntry

type Response struct {
	status int
	Err    string      `json:"error,omitempty"`    // any error representation
	Resp   interface{} `json:"response,omitempty"` // any content as response
}

type TablesList struct {
	Tables []string `json:"tables"`
}

type ColumnMetadata struct {
	FieldName       string // Name of column
	IsNumericType   bool   // Is it a numeric type
	IsNullable      bool
	IsAutoIncrement bool
}

type TableMetadata struct {
	ColumnsInfo []ColumnMetadata
	ColumnNames []string
	hash        map[string]ColumnMetadata
}

type RowResult struct {
	metadata TableMetadata
	entries  DBEntries
}

type DBExplorer struct {
	db         *sql.DB // database handler
	TableNames TablesList
	metadata   map[string]TableMetadata
}

type Req struct {
	table  string
	id     int
	params url.Values
}

// Internal function to build Column Info based on attributes from database.
func newColumnInfo(fieldName, fType, extra, null string) ColumnMetadata {
	return ColumnMetadata{
		FieldName:       fieldName,
		IsNumericType:   strings.Contains(fType, "int"),
		IsNullable:      null == "YES",
		IsAutoIncrement: strings.Contains(extra, "increment")}
}

func (t TableMetadata) getColumn(name string) ColumnMetadata {
	return t.hash[name]
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

func NewRowResult(metadata TableMetadata) *RowResult {
	return &RowResult{metadata: metadata, entries: make(DBEntries, 0, 20)}
}

func (r *RowResult) handleSingle(row *sql.Row) (interface{}, error) {
	colsCount := len(r.metadata.ColumnsInfo)
	columnVals := make([]interface{}, colsCount)
	for i := 0; i < colsCount; i++ {
		columnVals[i] = &columnVals[i]
	}
	err := row.Scan(columnVals...)
	if err != nil {
		fmt.Println("hahahahahahah", err)
		return nil, errors.New("record not found")
	}
	fmt.Printf("scanned: %+s\n", columnVals)
	entry := make(DBEntry, 1)
	for i := 0; i < colsCount; i++ {
		fmt.Printf("col val %T\n", columnVals[i])
		switch {
		case columnVals[i] == nil:
			entry[r.metadata.ColumnNames[i]] = nil
		default:
			if r.metadata.ColumnsInfo[i].IsNumericType {
				intVal, _ := columnVals[i].(int64)
				entry[r.metadata.ColumnNames[i]] = intVal
			} else {
				entry[r.metadata.ColumnNames[i]] = string(columnVals[i].([]byte))
			}
		}
	}
	result := map[string]DBEntry{"record": entry}
	return result, nil
}

func (r *RowResult) handleResult(rows *sql.Rows) {
	colsCount := len(r.metadata.ColumnNames)
	columnVals := make([]interface{}, colsCount)

	for rows.Next() {
		for i := 0; i < colsCount; i++ {
			columnVals[i] = &columnVals[i]
		}
		err := rows.Scan(columnVals...)
		if err != nil {
			fmt.Println("error while scanning results:", err)
		}
		fmt.Printf("scanned: %+s\n", columnVals)
		entry := make(DBEntry, 1)
		for i := 0; i < colsCount; i++ {
			switch {
			case columnVals[i] == nil:
				entry[r.metadata.ColumnNames[i]] = nil
			default:
				if r.metadata.ColumnsInfo[i].IsNumericType {
					intVal, _ := columnVals[i].(int64)
					entry[r.metadata.ColumnNames[i]] = intVal
				} else {
					entry[r.metadata.ColumnNames[i]] = string(columnVals[i].([]byte))
				}
			}
		}
		jsonString, _ := json.Marshal(entry)
		fmt.Printf("entries extracted: %s\n", entry)
		for k, v := range entry {
			fmt.Printf("for %s we have value %s of type %+T\n", k, v, v)
		}
		fmt.Printf("entries extracted: %s\n", jsonString)
		r.entries = append(r.entries, entry)
	}
	jsonString, err := json.Marshal(r.entries)
	fmt.Println(err)
	fmt.Printf("entries extracted: %s\n", jsonString)
}

// Collect table metadata: column names and column types.
func (d *DBExplorer) getColumnMetadata(tableName string) error {
	query := fmt.Sprintf("SHOW FULL COLUMNS FROM %s", tableName)
	rows, err := d.db.Query(query)
	defer closeResources(rows)

	if err != nil {
		return err
	}
	columnsInfo := []ColumnMetadata{}
	for rows.Next() {
		var (
			field, tType, tNull, key, tExtra, privileges, comment string
			collation, tDefault                                   interface{}
		)
		err := rows.Scan(&field, &tType, &collation, &tNull, &key, &tDefault, &tExtra, &privileges, &comment)
		if err != nil {
			fmt.Println("error while scanning row to value-holders:", err)
		}
		columnsInfo = append(columnsInfo, newColumnInfo(field, tType, tExtra, tNull))
	}
	fmt.Printf("^^^^ WE GOT: %#v\n", &columnsInfo)
	columnNames := make([]string, len(columnsInfo))
	for i := 0; i < len(columnsInfo); i++ {
		columnNames[i] = columnsInfo[i].FieldName
	}
	hash := make(map[string]ColumnMetadata, len(columnsInfo))
	for i := 0; i < len(columnsInfo); i++ {
		hash[columnsInfo[i].FieldName] = columnsInfo[i]
	}
	tableMetadata := TableMetadata{ColumnsInfo: columnsInfo, ColumnNames: columnNames, hash: hash}
	d.metadata[tableName] = tableMetadata
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

func (d *DBExplorer) getIdColumn(tableName string) string {
	columnsInfo := d.metadata[tableName].ColumnsInfo
	for i := 0; i < len(columnsInfo); i++ {
		if columnsInfo[i].IsAutoIncrement {
			return columnsInfo[i].FieldName
		}
	}
	return "" // No candidate for id found among columns ?
}
func (d *DBExplorer) getUpdatePlaceholders(req *Req, entity DBEntry) (placeholders string, values []interface{}) {
	columnNames := d.collectInsertColumns(req.table)
	values = make([]interface{}, 0, len(columnNames))
	filtered := make([]string, 0, len(columnNames))
	fmt.Printf("req body: %+v\n", entity)
	for i := 0; i < len(columnNames); i++ {
		if _, presented := entity[columnNames[i]]; !presented {
			continue
		}
		values = append(values, entity[columnNames[i]])
		filtered = append(filtered, columnNames[i])
	}
	if len(filtered) < 1 {
		return BadRequest, nil
	}
	placeholders = "`" + strings.Join(filtered, "` = ?, `") + "` = ?"
	fmt.Println("placeholders:", placeholders, "values", values)
	return
}

func (d *DBExplorer) collectInsertColumns(tableName string) []string {
	columns := d.metadata[tableName].ColumnsInfo
	filtered := make([]string, 0, len(columns)-1)
	for i := 0; i < len(columns); i++ {
		if columns[i].IsAutoIncrement {
			continue
		}
		filtered = append(filtered, columns[i].FieldName)
	}
	return filtered
}

func (d *DBExplorer) listInsertColumns(tableName string) string {
	return strings.Join(d.collectInsertColumns(tableName), ",")
}

func (d *DBExplorer) listUpdateColumns(tableName string) string {
	return d.listInsertColumns(tableName)
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
	var id int = -1
	fmt.Println("parsing request: tokens len:", len(tokens), "tokens content:", tokens)
	if len(tokens) > 0 {
		tableName = tokens[0]
	}
	if len(tokens) > 1 && tokens[1] != "" {
		fmt.Printf("\"%s\"\n", tokens[1])
		candiate, err := strconv.Atoi(tokens[1])
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
	return len(r.table) > 1 && r.id > 0
}

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
		result, err := d.queryBy(req)
		if err != nil {
			Reply(w, Resp(nil, http.StatusNotFound, err))
			return
		}
		Reply(w, Resp(result, http.StatusOK, nil))
	default:
		Reply(w, Resp(nil, http.StatusNotFound, errors.New("no such endpoint")))
	}
}

func (d *DBExplorer) handlePost(w http.ResponseWriter, r *http.Request) {
	req, err := parse(r)
	if err != nil {
		fmt.Println("error detected while parsing request:", err)
		Reply(w, Resp(nil, http.StatusInternalServerError, err))
		return
	}
	tableMetadata, ok := d.metadata[req.table]
	if !ok {
		http.Error(w, UnknownTableErr, http.StatusInternalServerError)
		return
	}
	requestBody, err := extractRequestBody(r, tableMetadata.ColumnsInfo)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result, err := d.update(req, requestBody)
	if err != nil {
		fmt.Println("&&& WE got problem on persist:", err)
		Reply(w, Resp(nil, http.StatusBadRequest, err))
		return
	}
	Reply(w, Resp(result, http.StatusOK, nil))
}

func extractRequestBody(r *http.Request, columnsInfo []ColumnMetadata) (map[string]interface{}, error) {
	defer closeResources(r.Body)
	bytedata, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Println("error reading request body:", err)
		return nil, err
	}
	temp := make(map[string]interface{})

	if err := json.Unmarshal(bytedata, &temp); err != nil {
		fmt.Println("error unmarshalling request body:", err)
		return nil, err
	}
	fmt.Printf("we got raw request body %s\n", bytedata)
	result := make(map[string]interface{})
	var columnInfo ColumnMetadata
	for i := 0; i < len(columnsInfo); i++ {
		columnInfo = columnsInfo[i]
		val, presented := temp[columnInfo.FieldName]
		if !presented {
			continue
		}

		if columnInfo.IsNumericType {
			result[columnInfo.FieldName] = int(val.(float64))
			continue
		}
		result[columnInfo.FieldName] = val
	}
	return result, nil
}

func (d *DBExplorer) handlePut(w http.ResponseWriter, r *http.Request) {
	req, err := parse(r)
	if err != nil {
		fmt.Println("error detected while parsing request:", err)
		Reply(w, Resp(nil, http.StatusInternalServerError, err))
		return
	}
	tableMetadata, ok := d.metadata[req.table]
	if !ok {
		http.Error(w, UnknownTableErr, http.StatusInternalServerError)
		return
	}
	fmt.Printf("%+v\n", req)

	requestBody, err := extractRequestBody(r, tableMetadata.ColumnsInfo)
	if err != nil {
		http.Error(w, "unable to parse json", http.StatusInternalServerError)
		return
	}
	result, err := d.insert(req, requestBody)
	if err != nil {
		fmt.Println("&&& WE got problem on persist:", err)
		Reply(w, Resp(nil, http.StatusNotFound, err))
		return
	}
	Reply(w, Resp(result, http.StatusOK, nil))
	// 1 . extract request body and validate
	// 2. try to insert
	// 3. handle errors
	// 4. response
}

func (d *DBExplorer) handleDelete(w http.ResponseWriter, r *http.Request) {
	req, err := parse(r)
	if err != nil {
		fmt.Println("error detected while parsing request:", err)
		Reply(w, Resp(nil, http.StatusInternalServerError, err))
		return
	}
	result, err := d.delete(req)
	if err != nil {
		fmt.Println("&&& WE got problem on persist:", err)
		Reply(w, Resp(nil, http.StatusNotFound, err))
		return
	}
	Reply(w, Resp(result, http.StatusOK, nil))
}

// ---------------------------- DB executors ------------------

func closeResources(closer io.Closer) {
	err := closer.Close()
	if err != nil {
		log.Fatal("unable to close rows")
	}
}

func (e *DBExplorer) findTableNames() []string {

	rows, err := e.db.Query("SHOW TABLES;")
	defer closeResources(rows)
	if err != nil {
		return nil
	}

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

func (d DBExplorer) FindFrom(params []url.Values) ([]interface{}, error) {
	switch {
	case len(params) < 1:
		return nil, errors.New("invalid")
	}
	return nil, nil
}

func (d *DBExplorer) delete(req *Req) (interface{}, error) {
	if _, ok := d.metadata[req.table]; !ok {
		return nil, errors.New(UnknownTableErr)
	}
	idColumn := d.getIdColumn(req.table)
	sql := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = ?", req.table, idColumn)

	result, err := d.db.Exec(sql, req.id)
	if err != nil {
		return nil, err
	}
	lastID, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"deleted": lastID}, nil
}

func (d *DBExplorer) insert(req *Req, entity DBEntry) (interface{}, error) {
	tableMetadata, ok := d.metadata[req.table]
	if !ok {
		return nil, errors.New(UnknownTableErr)
	}
	idColumn := d.getIdColumn(req.table)
	columns := d.collectInsertColumns(req.table)
	columnsCount := len(columns)
	placeholders := strings.Join(strings.Split(strings.Repeat("?", columnsCount), ""), ",")
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", req.table, d.listInsertColumns(req.table), placeholders)
	fmt.Println("raw sql on insert:", sql)
	values := make([]interface{}, columnsCount)
	for i := 0; i < columnsCount; i++ {
		values[i] = entity[columns[i]]
		if values[i] == nil {
			colInfo := tableMetadata.getColumn(columns[i])
			if !colInfo.IsNullable {
				if colInfo.IsNumericType {
					values[i] = 0
				} else {
					values[i] = ""
				}
			}
		}
	}
	result, err := d.db.Exec(sql, values...)
	if err != nil {
		return nil, err
	}
	lastID, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{idColumn: lastID}, nil
}

func validate(entity DBEntry, columnsInfo []ColumnMetadata) error {
	for i := 0; i < len(columnsInfo); i++ {
		if val, presented := entity[columnsInfo[i].FieldName]; !presented {
			continue
		} else {
			var failed bool
			if columnsInfo[i].IsAutoIncrement {
				failed = true
			}
			if !columnsInfo[i].IsNullable && val == nil {
				failed = true
			}

			isNumeric := columnsInfo[i].IsNumericType
			if val != nil {
				switch val.(type) {
				case string:
					if isNumeric {
						failed = true
					}
				default:
					if !isNumeric {
						failed = true
					}
				}
			}

			if failed {
				return fmt.Errorf("field %s have invalid type", columnsInfo[i].FieldName)
			}
		}
	}
	return nil
}

func (d *DBExplorer) update(req *Req, entity DBEntry) (interface{}, error) {
	tableMetadata, ok := d.metadata[req.table]
	if !ok {
		return nil, errors.New(UnknownTableErr)
	}
	if hasError := validate(entity, tableMetadata.ColumnsInfo); hasError != nil {
		return nil, hasError
	}
	idValue := req.id
	idColumn := d.getIdColumn(req.table)
	columns := d.collectInsertColumns(req.table)
	columnsCount := len(columns)
	updatePlaceholders, updateValues := d.getUpdatePlaceholders(req, entity)
	if updatePlaceholders == BadRequest {
		return nil, errors.New("bad request")
	}
	sql := fmt.Sprintf("UPDATE  `%s` SET %s WHERE `%s` = ?", req.table, updatePlaceholders, idColumn)
	fmt.Println("POST YUPDATE : ", sql)
	values := make([]interface{}, columnsCount)
	for i := 0; i < columnsCount; i++ {
		values[i] = entity[columns[i]]
	}
	updateValues = append(updateValues, idValue)
	result, err := d.db.Exec(sql, updateValues...)
	if err != nil {
		return nil, err
	}
	lastID, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"updated": lastID}, nil
}

// /update
func (d *DBExplorer) queryBy(r *Req) (interface{}, error) {
	fmt.Println("##### query by ID !~")
	if r.table == "" {
		return nil, errors.New("bad request")
	}

	tableMetadata, ok := d.metadata[r.table]
	if !ok {
		return nil, errors.New(UnknownTableErr)
	}
	columns := d.listColumns(r.table)
	sql := fmt.Sprintf("select %s from %s WHERE %s = ?", columns, r.table, tableMetadata.ColumnNames[0])
	row := d.db.QueryRow(sql, r.id)
	rowResult := NewRowResult(tableMetadata)
	return rowResult.handleSingle(row)
}

func (d *DBExplorer) query(r *Req) (result interface{}, err error) {
	if r.table == "" {
		return nil, errors.New("bad request")
	}
	tableMetadata, ok := d.metadata[r.table]
	if !ok {
		return nil, errors.New(UnknownTableErr)
	}
	columns := d.listColumns(r.table)
	sql := fmt.Sprintf("select %s from %s LIMIT ? OFFSET ?", columns, r.table)
	limit := defaultLimit
	offset := defaultOffest
	if len(r.params) > 0 {
		if tempLimit, err := strconv.Atoi(r.params.Get("limit")); err == nil {
			limit = tempLimit
		}
		if tempOffset, err := strconv.Atoi(r.params.Get("offset")); err == nil {
			offset = tempOffset
		}
	}

	rows, err := d.db.Query(sql, limit, offset)
	defer closeResources(rows)

	if err != nil {
		return nil, err
	}
	rowResult := NewRowResult(tableMetadata)
	rowResult.handleResult(rows)
	return map[string]interface{}{"records": rowResult.entries}, nil
}
