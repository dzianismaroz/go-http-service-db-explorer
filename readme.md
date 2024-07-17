db_explorer program

This simple web service will be a MySQL database manager that allows CRUD queries (create, read, update, delete) to it via HTTP

In this assignment, we continue to develop skills in working with HTTP and interacting with the database.

*You cannot use global variables in this task; store what you need in the fields of the structure that lives in the closure*

For the user it looks like this:
* GET / - returns a list of all tables (which we can use in further queries)
* GET /$table?limit=5&offset=7 - returns a list of 5 records (limit) starting from the 7th (offset) from table $table. limit by default 5, offset 0
* GET /$table/$id - returns information about the entry itself or 404
* PUT /$table - creates a new entry given by entry in the request body (POST parameters)
* POST /$table/$id - updates the record, the data comes in the body of the request (POST parameters)
* DELETE /$table/$id - deletes an entry
* GET, PUT, POST, DELETE - this is the http method by which the request was sent

Features of the program:
* Request routing is done manually, no external libraries can be used.
* Full dynamics. during initialization in NewDbExplorer, we read a list of tables and fields from the database (queries below), then we work with them during validation. No headcode in the form of a bunch of conditions and written code for validation and completion. If you add a third table, everything should work for it.
* We assume that the list of tables does not change while the program is running
* Queries will have to be constructed dynamically, data from there will also have to be retrieved dynamically - you do not have a fixed list of parameters - you load it during initialization.
* Validation at the "string - int - float - null" level, without any problems. Remember that json in an empty interface is unpacked as float, unless special parameters are specified. options.
* All work takes place through database/sql; a working connection to the database is sent to you as input. No orms or anything else.
* All field names are as they are in the database.
* If an error occurs, simply return 500 in the http status
* Don't forget about SQL injections
We ignore unknown fields
* The use of global variables is prohibited in this task. Whatever you want to store, store it in the fields of the structure that lives in the closure

Queries to help you get a list of tables and their structure:
``
SHOW TABLES;
SHOW FULL COLUMNS FROM `$table_name`;
``

Tips:
* Inside the row that you receive from the database there are not only the field values ​​themselves, but also metadata - https://golang.org/pkg/database/sql/#Rows.ColumnTypes
* Empty interfaces will be actively used here
* Pay attention to the processing of null values ​​(this is when you do not receive the value of a variable for which there is no default in the database - there will be such a test case)
* You will have to pull out an unknown number of fields from row, think about how you can use empty interfaces here
* The easiest way to raise a mysql database locally is through docker:
```
docker run -p 3306:3306 -v $(PWD):/docker-entrypoint-initdb.d -e MYSQL_ROOT_PASSWORD=1234 -e MYSQL_DATABASE=golang -d mysql
```
