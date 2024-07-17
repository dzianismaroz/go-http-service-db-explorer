package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// !!!!! Here you write the code.
// !!!!! Please note that global variables are not allowed in this task.

const (
	// ------------------ DB QUERIES ------------------------------
	showTablesQuery = "SHOW TABLES;"
	selectByIdQuery = "select %s from %s WHERE %s = ?"
	selectQuery     = "select %s from %s LIMIT ? OFFSET ?"
	insertQuery     = "INSERT INTO %s(%s) VALUES(%s)"
	updateQuery     = "UPDATE  `%s` SET %s WHERE `%s` = ?"
	deleteQuery     = "DELETE FROM `%s` WHERE `%s` = ?"
	// ------------------ errors ----------------------------------
	UnknownTableErr         = "unknown table"              // If requested table does not exist in database.
	RecordNotFoungErr       = "record not found"           // No entries found in table by criteria.
	InvalidIDTypeErrParrern = "field %s have invalid type" // Cleint submited invalid field for persitence in database.
	BadRequest              = "BAD_REQUEST"
	//-------------------------------------------------------------
	defaultLimit  = 5
	defaultOffest = 0
)

type (
	DBEntry     = map[string]interface{}
	HTTPStatus  = int
	DBEntries   = []DBEntry
	RequestBody = map[string]interface{}
)

type DBExplorer struct {
	db         *sql.DB                  // database handler
	TableNames TablesList               // Keep table names after instantiating.
	metadata   map[string]TableMetadata // Keep metadate per table after instantiating.
}

func Resp(content interface{}, status int, e error) Response {
	errContent := ""
	if e != nil {
		errContent = e.Error()
	}
	return Response{Err: errContent, status: status, Resp: content}
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
			return err
		}
		columnsInfo = append(columnsInfo, newColumnInfo(field, tType, tExtra, tNull))
	}
	columnNames := make([]string, len(columnsInfo))
	for i := 0; i < len(columnsInfo); i++ {
		columnNames[i] = columnsInfo[i].fieldName
	}
	hash := make(map[string]ColumnMetadata, len(columnsInfo))
	for i := 0; i < len(columnsInfo); i++ {
		hash[columnsInfo[i].fieldName] = columnsInfo[i]
	}
	tableMetadata := TableMetadata{columnsInfo: columnsInfo, columnNames: columnNames, hash: hash}
	d.metadata[tableName] = tableMetadata
	return nil
}

// Create new DB Explorer instance to handle DB-queries and http-requests
func NewDbExplorer(db *sql.DB) (*DBExplorer, error) {
	// Adjust default settings of DB-connection
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(1 * time.Second)
	// Just verify db connection once again. `who knows....`
	if connErr := db.Ping(); connErr != nil {
		return nil, connErr
	}

	dbExplorer := &DBExplorer{db: db, metadata: make(map[string]TableMetadata, 10)}
	return dbExplorer.collectMetaInfo()
}

func (d *DBExplorer) collectMetaInfo() (*DBExplorer, error) {
	tablesNames := d.findTableNames()
	if len(tablesNames) < 1 {
		return nil, errors.New("no tables in database")
	}
	d.TableNames = TablesList{Tables: tablesNames}
	for _, tableName := range tablesNames {
		// Collect each table metadata and persist in parent struct.
		err := d.getColumnMetadata(tableName)
		if err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Return table names, resolved on initialization. We can suspect it to be static.
func (d *DBExplorer) ListTables() []string {
	return d.TableNames.Tables
}

func (d *DBExplorer) listColumns(tableName string) string {
	columns := d.metadata[tableName].columnNames
	return strings.Join(columns, ",")
}

func (d *DBExplorer) getIdColumn(tableName string) string {
	columnsInfo := d.metadata[tableName].columnsInfo
	for i := 0; i < len(columnsInfo); i++ {
		if columnsInfo[i].isAutoIncrement {
			return columnsInfo[i].fieldName
		}
	}
	return "" // No candidate for id found among columns ?
}
func (d *DBExplorer) getUpdatePlaceholders(req *Req, entity DBEntry) (placeholders string, values []interface{}) {
	columnNames := d.collectInsertColumns(req.table)
	values = make([]interface{}, 0, len(columnNames))
	filtered := make([]string, 0, len(columnNames))
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
	return
}

func (d *DBExplorer) collectInsertColumns(tableName string) []string {
	columns := d.metadata[tableName].columnsInfo
	filtered := make([]string, 0, len(columns)-1)
	for i := 0; i < len(columns); i++ {
		if columns[i].isAutoIncrement {
			continue
		}
		filtered = append(filtered, columns[i].fieldName)
	}
	return filtered
}

func (d *DBExplorer) listInsertColumns(tableName string) string {
	return strings.Join(d.collectInsertColumns(tableName), ",")
}

// Simple request tracking to StdOut.
func (d *DBExplorer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Usefull http requests tracking [-> 2024-07-17 17:58:29 [GET] : /users/1 {url.Values{}}]
	fmt.Printf("-> %s [%s] : %s {%#v}\n", time.Now().Format(time.DateTime), r.Method, r.URL.Path, r.URL.Query())
	d.route(w, r)
}

// -------------------------------- Router   --------------------------------------
func (d *DBExplorer) route(w http.ResponseWriter, r *http.Request) {
	requestedData, err := parse(r, d.metadata)
	if err != nil {
		reply(w, Resp(nil, http.StatusInternalServerError, err))
		return
	}
	switch r.Method {
	case http.MethodGet: // Query results by predicate from database.
		d.handleGet(w, requestedData)
	case http.MethodPost: // Upate existing entry in database.
		d.handlePost(w, requestedData)
	case http.MethodPut: // Create new entry in database.
		d.handlePut(w, requestedData)
	case http.MethodDelete: // Delete entry by id provided in url path.
		d.handleDelete(w, requestedData)
	default: // Invalid API usage by client.
		http.Error(w, "invalid request", http.StatusNotFound)
	}
}

// ------------------ parse request params ---------------------

func parse(r *http.Request, tableMetadataMap map[string]TableMetadata) (presult *Req, err error) {
	p := r.URL.Path
	tokens := strings.Split(p, "/")[1:]
	var tableName string
	var id int = -1
	if len(tokens) > 0 {
		tableName = tokens[0]
	}
	if len(tokens) > 1 && tokens[1] != "" {
		candiate, err := strconv.Atoi(tokens[1])
		if err != nil {
			return nil, err
		}
		id = candiate
	}

	return &Req{
		table:  tableName,
		id:     id,
		params: r.URL.Query(),
		body:   extractRequestBody(r, tableMetadataMap[tableName].columnsInfo),
	}, nil
}

// -------------------------------- Handlers --------------------------------------
// Only query for some data from database: list of table names / content of specified table.
func (d *DBExplorer) handleGet(w http.ResponseWriter, requestedData *Req) {
	var resp Response = Resp(nil, http.StatusNotFound, errors.New("no such endpoint"))
	switch {
	// We need to provide only list of table names.
	case requestedData.isTableNamesQuery():
		resp = Resp(map[string][]string{"tables": d.ListTables()}, http.StatusOK, nil)
	// we need to reply on multi-row query to database.
	case requestedData.isTableEntriesQuery():
		res, err := d.query(requestedData)
		if err != nil {
			resp = Resp(nil, http.StatusNotFound, err)
			break
		}
		resp = Resp(res, http.StatusOK, nil)
	// Only single row was requested by id.
	case requestedData.isByIdQuery():
		result, err := d.queryBy(requestedData)
		if err != nil {
			resp = Resp(nil, http.StatusNotFound, err)
			break
		}
		resp = Resp(result, http.StatusOK, nil)
	}
	reply(w, resp)
}

func (d *DBExplorer) handlePost(w http.ResponseWriter, requestedData *Req) {
	result, err := d.update(requestedData)
	if err != nil {
		reply(w, Resp(nil, http.StatusBadRequest, err))
		return // Failed to query DB.
	}
	reply(w, Resp(result, http.StatusOK, nil)) // Success on DB query.
}

func extractRequestBody(r *http.Request, columnsInfo []ColumnMetadata) RequestBody {
	if r.Method == http.MethodGet || r.Method == http.MethodDelete {
		return nil
	}
	rawBodyBytes, err := io.ReadAll(r.Body)
	defer closeResources(r.Body)
	if err != nil {
		return nil
	}
	temp := make(RequestBody) // Preliminary uhnmarshall of request body for further filtering and casting.

	if err := json.Unmarshal(rawBodyBytes, &temp); err != nil {
		return nil
	}
	var colName string
	// Filter unknown attributes and make explicit casting. Use known columns metadata.
	for i := 0; i < len(columnsInfo); i++ {
		colName = columnsInfo[i].fieldName
		if val, presented := temp[colName]; !presented {
			delete(temp, colName)
			continue
		} else {
			// Explicit cast to numeric.
			if columnsInfo[i].isNumericType {
				temp[colName] = int(val.(float64))
				continue
			}
		}
	}
	return temp
}

func (d *DBExplorer) handlePut(w http.ResponseWriter, requestedData *Req) {
	result, err := d.insert(requestedData)
	if err != nil {
		reply(w, Resp(nil, http.StatusNotFound, err))
		return
	}
	reply(w, Resp(result, http.StatusOK, nil))
}

func (d *DBExplorer) handleDelete(w http.ResponseWriter, requestedData *Req) {
	if result, err := d.delete(requestedData); err != nil {
		reply(w, Resp(nil, http.StatusNotFound, err))
	} else {
		reply(w, Resp(result, http.StatusOK, nil))
	}
}

// ---------------------------- ------------------

func closeResources(closer io.Closer) {

	err := closer.Close()
	if err != nil {
		log.Fatal("unable to close rows")
	}
}

func validate(entity DBEntry, columnsInfo []ColumnMetadata) error {
	for i := 0; i < len(columnsInfo); i++ {
		if val, presented := entity[columnsInfo[i].fieldName]; !presented {
			continue
		} else {
			var failed bool
			if columnsInfo[i].isAutoIncrement {
				failed = true
			}
			if !columnsInfo[i].isNullable && val == nil {
				failed = true
			}

			isNumeric := columnsInfo[i].isNumericType
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
				return fmt.Errorf("field %s have invalid type", columnsInfo[i].fieldName)
			}
		}
	}
	return nil
}

// ---------------------- Database operations ---------------------------

func (e *DBExplorer) findTableNames() []string {
	rows, err := e.db.Query(showTablesQuery)
	defer closeResources(rows)
	if err != nil {
		return nil
	}

	tablesNames := make([]string, 0, 10)
	var tableName string
	for rows.Next() {
		if err := rows.Scan(&tableName); err != nil {
			return nil
		}
		tablesNames = append(tablesNames, tableName)
	}
	return tablesNames

}

// Perform delete from database by ID specified in http path.
func (d *DBExplorer) delete(req *Req) (interface{}, error) {
	if _, ok := d.metadata[req.table]; !ok {
		return nil, errors.New(UnknownTableErr)
	}
	idColumn := d.getIdColumn(req.table)
	sql := fmt.Sprintf(deleteQuery, req.table, idColumn)

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

func (d *DBExplorer) insert(req *Req) (interface{}, error) {
	entity := req.body
	tableMetadata, ok := d.metadata[req.table]
	if !ok {
		return nil, errors.New(UnknownTableErr)
	}
	idColumn := d.getIdColumn(req.table)
	columns := d.collectInsertColumns(req.table)
	columnsCount := len(columns)
	placeholders := strings.Join(strings.Split(strings.Repeat("?", columnsCount), ""), ",")
	sql := fmt.Sprintf(insertQuery, req.table, d.listInsertColumns(req.table), placeholders)
	values := make([]interface{}, columnsCount)
	for i := 0; i < columnsCount; i++ {
		values[i] = entity[columns[i]]
		if values[i] == nil {
			colInfo := tableMetadata.getColumn(columns[i])
			if !colInfo.isNullable {
				if colInfo.isNumericType {
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

func (d *DBExplorer) update(req *Req) (interface{}, error) {
	entity := req.body
	tableMetadata, ok := d.metadata[req.table]
	if !ok || entity == nil {
		return nil, errors.New(UnknownTableErr)
	}
	if hasError := validate(entity, tableMetadata.columnsInfo); hasError != nil {
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
	sql := fmt.Sprintf(updateQuery, req.table, updatePlaceholders, idColumn)
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

func (d *DBExplorer) queryBy(r *Req) (interface{}, error) {
	if r.table == "" {
		return nil, errors.New("bad request")
	}

	tableMetadata, ok := d.metadata[r.table]
	if !ok {
		return nil, errors.New(UnknownTableErr)
	}
	columns := d.listColumns(r.table)
	sql := fmt.Sprintf(selectByIdQuery, columns, r.table, tableMetadata.columnNames[0])
	row := d.db.QueryRow(sql, r.id)
	rowResult := newRowResult(tableMetadata)
	return rowResult.handleSingleRowResult(row)
}

func (d *DBExplorer) query(r *Req) (result interface{}, err error) {

	tableMetadata, known := d.metadata[r.table] // Should be ok, cause table is known.
	if !known {
		return nil, errors.New(UnknownTableErr)
	}
	sql := fmt.Sprintf(selectQuery, d.listColumns(r.table), r.table)
	limit, offset := defaultLimit, defaultOffest
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
	rowResult := newRowResult(tableMetadata)
	err = rowResult.handleMultiRowResult(rows)
	return map[string]interface{}{"records": rowResult.entries}, err
}
