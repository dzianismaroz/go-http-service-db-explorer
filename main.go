// here is the test code
// you may only need to change the connection to the database
package main

import (
	"database/sql"
	"fmt"
	"net/http"

	_ "github.com/go-sql-driver/mysql"
)

var (
	// DSN is the connection to the database
	// you can change this to the one you need
	// docker run -p 3306:3306 -v $(PWD):/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql
	// DSN = "root@tcp(localhost:3306)/golang2017?charset=utf8"
	DSN = "root:love@tcp(localhost:3306)/photolist?charset=utf8"
)

func main() {
	db, err := sql.Open("mysql", DSN)
	err = db.Ping() // here will be the first connection to the database
	if err != nil {
		panic(err)
	}

	handler, err := NewDbExplorer(db)
	if err != nil {
		panic(err)
	}

	fmt.Println("starting server at :8082")
	http.ListenAndServe(":8082", handler)
}
