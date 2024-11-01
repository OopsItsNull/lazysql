package drivers

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	// import postgresql driver
	_ "github.com/microsoft/go-mssqldb"
	"github.com/xo/dburl"

	"github.com/jorgerojas26/lazysql/helpers/logger"
	"github.com/jorgerojas26/lazysql/models"
)

type SqlServer struct {
	Connection       *sql.DB
	Provider         string
	CurrentDatabase  string
	PreviousDatabase string
	Urlstr           string
}

const (
	defaultSqlServerPort = "1433"
)

func (db *SqlServer) TestConnection(urlstr string) error {
	return db.Connect(urlstr)
}

func (db *SqlServer) Connect(urlstr string) (err error) {
	db.SetProvider(DriverSqlServer)

	db.Connection, err = sql.Open("sqlserver", urlstr)
	if err != nil {
		return err
	}

	err = db.Connection.Ping()
	if err != nil {
		return err
	}

	db.Urlstr = urlstr

	return nil
}

func (db *SqlServer) GetDatabases() (databases []string, err error) {
	rows, err := db.Connection.Query("SELECT [name] FROM sys.databases WHERE [name] NOT IN('master', 'tempdb', 'model', 'msdb') ORDER BY [name];")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var database string
		err := rows.Scan(&database)
		if err != nil {
			return nil, err
		}
		databases = append(databases, database)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return databases, nil
}

func (db *SqlServer) GetTables(database string) (tables map[string][]string, err error) {
	tables = make(map[string][]string)

	logger.Info("GetTables", map[string]any{"database": database})

	if database == "" {
		return nil, errors.New("database name is required")
	}

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			_ = db.SwitchDatabase(db.PreviousDatabase)
		}
	}()

	query := "SELECT [TABLE_NAME], [TABLE_SCHEMA] FROM INFORMATION_SCHEMA.TABLES ORDER BY [TABLE_SCHEMA], [TABLE_NAME];"
	rows, err := db.Connection.Query(query, database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			tableName   string
			tableSchema string
		)
		if err := rows.Scan(&tableName, &tableSchema); err != nil {
			return nil, err
		}

		tables[tableSchema] = append(tables[tableSchema], tableName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func (db *SqlServer) GetTableColumns(database, table string) (results [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return nil, errors.New("table must be in the format schema.table")
	}

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			_ = db.SwitchDatabase(db.PreviousDatabase)
		}
	}()

	tableSchema := splitTableString[0]
	tableName := splitTableString[1]

	query := "SELECT [COLUMN_NAME] FROM INFORMATION_SCHEMA.COLUMNS WHERE [TABLE_SCHEMA] = ? AND [TABLE_NAME] = ? ORDER BY [ORDINAL_POSITION];"
	rows, err := db.Connection.Query(query, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, columnsError := rows.Columns()
	if columnsError != nil {
		err = columnsError
	}

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))

		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		if err := rows.Scan(rowValues...); err != nil {
			return nil, err
		}

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return
}

func (db *SqlServer) GetConstraints(database, table string) (constraints [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return nil, errors.New("table must be in the format schema.table")
	}

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			_ = db.SwitchDatabase(db.PreviousDatabase)
		}
	}()

	tableSchema := splitTableString[0]
	tableName := splitTableString[1]

	rows, err := db.Connection.Query(`
        SELECT
            tc.CONSTRAINT_NAME,
            kcu.COLUMN_NAME,
            tc.CONSTRAINT_TYPE
        FROM
            INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS ccu ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            AND ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
        WHERE
            tc.CONSTRAINT_TYPE != 'FOREIGN KEY'
			AND tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?;
            `, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, columnsError := rows.Columns()
	if columnsError != nil {
		err = columnsError
	}

	constraints = append(constraints, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		if err := rows.Scan(rowValues...); err != nil {
			return nil, err
		}

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		constraints = append(constraints, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return
}

func (db *SqlServer) GetForeignKeys(database, table string) (foreignKeys [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return nil, errors.New("table must be in the format schema.table")
	}

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			_ = db.SwitchDatabase(db.PreviousDatabase)
		}
	}()

	tableSchema := splitTableString[0]
	tableName := splitTableString[1]

	rows, err := db.Connection.Query(`
        SELECT
            tc.CONSTRAINT_NAME,
            kcu.COLUMN_NAME,
            ccu.TABLE_NAME AS foreign_table_name,
            ccu.COLUMN_NAME AS foreign_column_name
        FROM
            INFORMATION_SCHEMA.table_constraints tc
            INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
            	ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            		AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
            INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
                ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                    AND ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
        WHERE
            tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
          	AND tc.TABLE_SCHEMA = ?
            AND tc.TABLE_NAME = ?;
  `, tableSchema, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, columnsError := rows.Columns()
	if columnsError != nil {
		err = columnsError
	}

	foreignKeys = append(foreignKeys, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		if err := rows.Scan(rowValues...); err != nil {
			return nil, err
		}

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		foreignKeys = append(foreignKeys, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return
}

func (db *SqlServer) GetIndexes(database, table string) (indexes [][]string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return nil, errors.New("table must be in the format schema.table")
	}

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			_ = db.SwitchDatabase(db.PreviousDatabase)
		}
	}()

	tableSchema := splitTableString[0]
	tableName := splitTableString[1]

	rows, err := db.Connection.Query(fmt.Sprintf(`
        SELECT
            ind.name AS [index_name],
            col.name AS [column_name],
        	ind.type_desc AS [type]
        FROM
            sys.indexes ind
        	INNER JOIN sys.index_columns ind_col
        		ON ind.object_id = ind_col.object_id
        			AND ind.index_id = ind_col.index_id
        	INNER JOIN sys.columns col
        		ON ind_col.object_id = col.object_id
        			AND ind_col.column_id = col.column_id
        	INNER JOIN sys.tables tab
        		ON ind.object_id = tab.object_id
        	INNER JOIN sys.schemas schem
        		ON schem.schema_id = tab.schema_id
        WHERE
        	tab.name = ?
        	AND schem.name = ?
            AND ind.is_primary_key = 0
            AND ind.is_unique = 0
            AND ind.is_unique_constraint = 0
            AND tab.is_ms_shipped = 0
        ORDER BY
             tab.name,
        	 ind.name,
        	 ind.index_id,
        	 ind_col.is_included_column,
        	 ind_col.key_ordinal;
  `, tableSchema, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, columnsError := rows.Columns()
	if columnsError != nil {
		err = columnsError
	}

	indexes = append(indexes, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		if err := rows.Scan(rowValues...); err != nil {
			return nil, err
		}

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		indexes = append(indexes, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return
}

func (db *SqlServer) GetRecords(database, table, where, sort string, offset, limit int) (records [][]string, totalRecords int, err error) {
	if database == "" {
		return nil, 0, errors.New("database name is required")
	}

	if table == "" {
		return nil, 0, errors.New("table name is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return nil, 0, errors.New("table must be in the format schema.table")
	}

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return nil, 0, err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			if database != db.PreviousDatabase {
				_ = db.SwitchDatabase(db.PreviousDatabase)
			}
		}
	}()

	tableSchema := splitTableString[0]
	tableName := splitTableString[1]

	formattedTableName := db.formatTableName(tableSchema, tableName)

	isPaginationEnabled := offset > 0 || limit > 0

	if limit == 0 {
		limit = DefaultRowLimit
	}

	query := "SELECT * FROM "
	query += formattedTableName

	if where != "" {
		query += fmt.Sprintf(" %s", where)
	}

	if sort != "" {
		query += fmt.Sprintf(" ORDER BY %s", sort)
	} else if isPaginationEnabled {
		query += " ORDER BY (SELECT NULL)"
	}
	query += fmt.Sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit)

	paginatedRows, err := db.Connection.Query(query)
	if err != nil {
		return nil, 0, err
	}
	defer paginatedRows.Close()

	columns, columnsError := paginatedRows.Columns()
	if columnsError != nil {
		return nil, 0, columnsError
	}

	records = append(records, columns)

	for paginatedRows.Next() {
		nullStringSlice := make([]sql.NullString, len(columns))

		rowValues := make([]interface{}, len(columns))
		for i := range nullStringSlice {
			rowValues[i] = &nullStringSlice[i]
		}

		if err := paginatedRows.Scan(rowValues...); err != nil {
			return nil, 0, err
		}

		var row []string
		for _, col := range nullStringSlice {
			if col.Valid {
				if col.String == "" {
					row = append(row, "EMPTY&")
				} else {
					row = append(row, col.String)
				}
			} else {
				row = append(row, "NULL&")
			}
		}

		records = append(records, row)

	}

	if err := paginatedRows.Err(); err != nil {
		return nil, 0, err
	}
	// close to release the connection
	if err := paginatedRows.Close(); err != nil {
		return nil, 0, err
	}

	countQuery := "SELECT COUNT(*) FROM "
	countQuery += formattedTableName
	row := db.Connection.QueryRow(countQuery)
	if err := row.Scan(&totalRecords); err != nil {
		return nil, 0, err
	}

	return
}

func (db *SqlServer) UpdateRecord(database, table, column, value, primaryKeyColumnName, primaryKeyValue string) (err error) {
	if database == "" {
		return errors.New("database name is required")
	}

	if table == "" {
		return errors.New("table name is required")
	}

	if column == "" {
		return errors.New("column name is required")
	}

	if value == "" {
		return errors.New("value is required")
	}

	if primaryKeyColumnName == "" {
		return errors.New("primary key column name is required")
	}

	if primaryKeyValue == "" {
		return errors.New("primary key value is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return errors.New("table must be in the format schema.table")
	}

	switchDatabaseOnError := false

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return err
		}
		switchDatabaseOnError = true
	}

	tableSchema := splitTableString[0]
	tableName := splitTableString[1]

	formattedTableName := db.formatTableName(tableSchema, tableName)

	query := "UPDATE "
	query += formattedTableName
	query += fmt.Sprintf(" SET [%s] = ? WHERE [%s] = ?", column, primaryKeyColumnName)

	_, err = db.Connection.Exec(query, value, primaryKeyValue)

	if err != nil && switchDatabaseOnError {
		err = db.SwitchDatabase(db.PreviousDatabase)
	}

	return err
}

func (db *SqlServer) DeleteRecord(database, table, primaryKeyColumnName, primaryKeyValue string) (err error) {
	if database == "" {
		return errors.New("database name is required")
	}

	if table == "" {
		return errors.New("table name is required")
	}

	if primaryKeyColumnName == "" {
		return errors.New("primary key column name is required")
	}

	if primaryKeyValue == "" {
		return errors.New("primary key value is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return errors.New("table must be in the format schema.table")
	}

	switchDatabaseOnError := false

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return err
		}
		switchDatabaseOnError = true
	}

	tableSchema := splitTableString[0]
	tableName := splitTableString[1]

	formattedTableName := db.formatTableName(tableSchema, tableName)

	query := "DELETE FROM "
	query += formattedTableName
	query += fmt.Sprintf(" WHERE [%s] = ?", primaryKeyColumnName)

	_, err = db.Connection.Exec(query, primaryKeyValue)

	if err != nil && switchDatabaseOnError {
		err = db.SwitchDatabase(db.PreviousDatabase)
	}

	return err
}

func (db *SqlServer) ExecuteDMLStatement(query string) (result string, err error) {
	res, err := db.Connection.Exec(query)
	if err != nil {
		return result, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return result, err
	}

	return fmt.Sprintf("%d rows affected", rowsAffected), nil
}

func (db *SqlServer) ExecuteQuery(query string) (results [][]string, err error) {
	rows, err := db.Connection.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	results = append(results, columns)

	for rows.Next() {
		rowValues := make([]interface{}, len(columns))
		for i := range columns {
			rowValues[i] = new(sql.RawBytes)
		}

		err = rows.Scan(rowValues...)
		if err != nil {
			return nil, err
		}

		var row []string
		for _, col := range rowValues {
			row = append(row, string(*col.(*sql.RawBytes)))
		}

		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return
}

func (db *SqlServer) ExecutePendingChanges(changes []models.DbDmlChange) (err error) {
	var queries []models.Query

	for _, change := range changes {
		columnNames := []string{}
		values := []interface{}{}
		valuesPlaceholder := []string{}
		placeholderIndex := 1

		for _, cell := range change.Values {
			columnNames = append(columnNames, cell.Column)

			switch cell.Type {
			case models.Default:
				valuesPlaceholder = append(valuesPlaceholder, "DEFAULT")
			case models.Null:
				valuesPlaceholder = append(valuesPlaceholder, "NULL")
			default:
				valuesPlaceholder = append(valuesPlaceholder, fmt.Sprintf("$%d", placeholderIndex))
				placeholderIndex++
			}
		}

		for _, cell := range change.Values {
			switch cell.Type {
			case models.Empty:
				values = append(values, "")
			case models.String:
				values = append(values, cell.Value)
			}
		}

		splitTableString := strings.Split(change.Table, ".")

		tableSchema := splitTableString[0]
		tableName := splitTableString[1]

		formattedTableName := db.formatTableName(tableSchema, tableName)

		switch change.Type {

		case models.DmlInsertType:

			queryStr := "INSERT INTO " + formattedTableName
			queryStr += fmt.Sprintf(" (%s) VALUES (%s)", strings.Join(columnNames, ", "), strings.Join(valuesPlaceholder, ", "))

			newQuery := models.Query{
				Query: queryStr,
				Args:  values,
			}

			queries = append(queries, newQuery)
		case models.DmlUpdateType:
			queryStr := "UPDATE " + formattedTableName

			for i, column := range columnNames {
				if i == 0 {
					queryStr += fmt.Sprintf(" SET [%s] = %s", column, valuesPlaceholder[i])
				} else {
					queryStr += fmt.Sprintf(", [%s] = %s", column, valuesPlaceholder[i])
				}
			}

			args := make([]interface{}, len(values))

			copy(args, values)

			wherePlaceholder := 0

			for _, placeholder := range valuesPlaceholder {
				if strings.Contains(placeholder, "$") {
					wherePlaceholder++
				}
			}

			for i, pki := range change.PrimaryKeyInfo {
				wherePlaceholder++
				if i == 0 {
					queryStr += fmt.Sprintf(" WHERE [%s] = $%d", pki.Name, wherePlaceholder)
				} else {
					queryStr += fmt.Sprintf(" AND [%s] = $%d", pki.Name, wherePlaceholder)
				}
				args = append(args, pki.Value)
			}

			newQuery := models.Query{
				Query: queryStr,
				Args:  args,
			}

			queries = append(queries, newQuery)
		case models.DmlDeleteType:
			queryStr := "DELETE FROM " + formattedTableName
			args := make([]interface{}, len(change.PrimaryKeyInfo))

			for i, pki := range change.PrimaryKeyInfo {
				if i == 0 {
					queryStr += fmt.Sprintf(" WHERE [%s] = $%d", pki.Name, i+1)
				} else {
					queryStr += fmt.Sprintf(" AND [%s] = $%d", pki.Name, i+1)
				}
				args[i] = pki.Value
			}

			newQuery := models.Query{
				Query: queryStr,
				Args:  args,
			}

			queries = append(queries, newQuery)
		}
	}
	return queriesInTransaction(db.Connection, queries)
}

func (db *SqlServer) GetPrimaryKeyColumnNames(database, table string) (primaryKeyColumnName []string, err error) {
	if database == "" {
		return nil, errors.New("database name is required")
	}

	if table == "" {
		return nil, errors.New("table name is required")
	}

	splitTableString := strings.Split(table, ".")

	if len(splitTableString) == 1 {
		return nil, errors.New("table must be in the format schema.table")
	}

	if database != db.CurrentDatabase {
		err = db.SwitchDatabase(database)
		if err != nil {
			return nil, err
		}
	}

	defer func() {
		if r := recover(); r != nil {
			_ = db.SwitchDatabase(db.PreviousDatabase)
		}
	}()

	schemaName := splitTableString[0]
	tableName := splitTableString[1]

	row, err := db.Connection.Query(`
	SELECT ccu.COLUMN_NAME
	FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
		INNER JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
			ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
				AND ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
	WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		AND tc.TABLE_SCHEMA = ?
		AND tc.TABLE_NAME = ?
	ORDER BY ccu.COLUMN_NAME
	`, schemaName, tableName)
	if err != nil {
		return nil, err
	}

	defer row.Close()

	for row.Next() {
		var colName string
		err = row.Scan(&colName)
		if err != nil {
			return nil, err
		}

		if row.Err() != nil {
			return nil, row.Err()
		}

		primaryKeyColumnName = append(primaryKeyColumnName, colName)
	}

	if row.Err() != nil {
		return nil, row.Err()
	}

	return primaryKeyColumnName, nil
}

func (db *SqlServer) SetProvider(provider string) {
	db.Provider = provider
}

func (db *SqlServer) GetProvider() string {
	return db.Provider
}

func (db *SqlServer) SwitchDatabase(database string) error {
	parsedConn, err := dburl.Parse(db.Urlstr)
	if err != nil {
		return err
	}

	user := parsedConn.User.Username()
	password, _ := parsedConn.User.Password()
	host := parsedConn.Host
	port := parsedConn.Port()
	parsedQuery := parsedConn.Query()
	dbName := parsedQuery.Get("database")

	if dbName == "" {
		dbName = database
	}

	if port == "" {
		port = defaultSqlServerPort
	}

	connection, err := sql.Open(
		"sqlserver", fmt.Sprintf(
			"sqlserver://%s:%s@%s:%s/?database=%s",
			user, password,
			host, port,
			dbName))
	if err != nil {
		return err
	}

	err = db.Connection.Close()
	if err != nil {
		return err
	}

	db.Connection = connection
	db.PreviousDatabase = db.CurrentDatabase
	db.CurrentDatabase = database

	return nil
}

func (db *SqlServer) formatTableName(database, table string) string {
	return fmt.Sprintf("[%s].[%s]", database, table)
}
