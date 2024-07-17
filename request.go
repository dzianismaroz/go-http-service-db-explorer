package main

import "net/url"

// Representation of requested table / id / params
type Req struct {
	table  string
	id     int
	params url.Values
	body   RequestBody
}

// Representation of reply to http client.
// Provide any reasonable result /  error.
type Response struct {
	status HTTPStatus  // Transient attribute for http status handling.
	Err    string      `json:"error,omitempty"`    // Error if occurred.
	Resp   interface{} `json:"response,omitempty"` // Any reasonable content.
}

// Wrapper for rerplying list of tables in database.
type TablesList struct {
	Tables []string `json:"tables"`
}

func (r *Req) isTableNamesQuery() bool {
	return r.table == ""
}

func (r *Req) isTableEntriesQuery() bool {
	return len(r.table) > 1 && r.id < 0
}

func (r *Req) isByIdQuery() bool {
	return len(r.table) > 1 && r.id > 0
}
