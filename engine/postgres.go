/*
    TODO: Consider column name security
		TODO: Obtain lock on a per-row basis when promoting fields
*/
package engine

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"errors"
	"encoding/json"
	"strings"
	"strconv"
	"github.com/lib/pq"
)


type PostgresDB struct {
	connection *sql.DB
	version string
}

func (postgresDB *PostgresDB) Connect(config *Config) error {
	if config == nil {
		return errors.New("Config is nil")
	}
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		config.DB_USER, config.DB_PASSWORD, config.DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	if err != nil {
		return err
	}
	postgresDB.connection = db
	err = postgresDB.setup()
	if err != nil {
		return err
	}
	return err
}

func (postgresDB *PostgresDB) CurrentSchema() (map[string]Table, error) {
	tables := make(map[string]Table, 0)
	//Get schema information from information_schema.columns
	rows, err := postgresDB.connection.Query("SELECT table_name, column_name, data_type, character_maximum_length, numeric_precision, numeric_precision_radix, numeric_scale from information_schema.columns WHERE table_schema = 'public'")
	if err != nil {
		return tables, err
	}
	defer rows.Close()
	//For every column (we have a row for each), create a ColumnInfo
	// and use it to populate the data in `tables`
	for rows.Next() {
		var tableName string
		var ci ColumnInfo
		var charMaxLen, numericPrecision, numericPrecisionRadix, numericScale sql.NullInt64
		err = rows.Scan(&tableName, &ci.Name, &ci.DataType, &charMaxLen, &numericPrecision, &numericPrecisionRadix, &numericScale)
		if err != nil {
			return tables, err
		}

		//Populate possibly null values
		if charMaxLen.Valid { ci.CharMaxLength = charMaxLen.Int64	}
		if numericPrecision.Valid { ci.NumericPrecision = numericPrecision.Int64	}
		if numericPrecisionRadix.Valid { ci.NumericPrecisionRadix = numericPrecisionRadix.Int64	}
		if numericScale.Valid { ci.NumericScale = numericScale.Int64	}

		// Create the table in memory if necessary
		if _, ok := tables[tableName]; !ok {
			tables[tableName] = Table{
				Name: tableName,
				Columns: make(map[string]string, 0),
				Status: "created",
			}
		}

		//Set the column information to the string version of ColumnInfo
		tables[tableName].Columns[ci.Name] = ci.ToString()
	}
	return tables, nil
}

func (postgresDB *PostgresDB) PerformMigration(steps []MigrationStep) error {
	for _, step := range steps {
		switch val := step.(type){
		case MigrationStepCreateTable:
			err := postgresDB.MigrationCreateTable(val)
			if err != nil { return err }
		case MigrationStepPromoteField:
			err := postgresDB.MigrationPromoteField(val)
			if err != nil { return err }
		case MigrationStepIndexColumn:
			err := postgresDB.MigrationIndexColumn(val)
			if err != nil { return err }
		default:
			return errors.New("Error. Unknown migration step type")
		}
	}
	return nil
}

//Return the postgres column type to be used for a given
// autoscope column
func postgresColumnType(table Table, column string) string{
	tyMap := map[string]string{
		"json": "jsonb",
	}
	ty := table.Columns[column]
	if postgresTy, ok := tyMap[ty]; ok {
		return postgresTy
	}
	return ty
}

//Return any postgres restraints to be used for a given column
// e.g. UNIQUE, NOT NULL, PRIMARY KEY
func postgresConstraints(table Table, column string) string{
	if column == "id" {
		return "PRIMARY KEY"
	}
	return ""
}

//Create a table in postgres
// TODO: We must also copy over any data present in autoscope_unassigned
func (postgresDB *PostgresDB) MigrationCreateTable(ct MigrationStepCreateTable) error {
	queryStr := "CREATE TABLE " + ct.tableName + "(\n"
	for column, _ := range ct.table.Columns {
		queryStr += column
		queryStr += " " + postgresColumnType(ct.table, column)
		queryStr += " " + postgresConstraints(ct.table, column) + ",\n"
	}
	//Remove trailing comma
	queryStr = queryStr[0: len(queryStr) - 2]
	queryStr += ");"
	_, err := postgresDB.connection.Exec(queryStr)
	return err
}

func (postgresDB *PostgresDB) MigrationPromoteField(pf MigrationStepPromoteField) error {
	//First, create column in table
	queryStr := "ALTER TABLE " + pf.tableName + " CREATE COLUMN " + pf.column + " " + postgresColumnType(pf.table, pf.column) + " " + postgresConstraints(pf.table, pf.column)
	log.Println(queryStr)
	_, err := postgresDB.connection.Exec(queryStr)
	if err != nil {
		return err
	}

	//Second, perform the per-row promotion from object field to column
	// For now, we do this inline. Eventually, we can perform this as a background
	// batch job.
	rows, err := postgresDB.connection.Query("SELECT id, autoscope_objectfields FROM " + pf.tableName + " WHERE autoscope_objectfields ->> "+jsonProp(pf.column)+" != '')")
	defer rows.Close()
	for rows.Next(){
		var jsonStr string
		var id int64
		err = rows.Scan(&id, &jsonStr)
		if err != nil {
			return err
		}
		var jsonObj map[string]interface{}
		err := json.Unmarshal([]byte(jsonStr), &jsonObj)
		if err != nil {
			return err
		}
		//Extract the value to place into the column
		val := jsonObj[pf.column]

		//Delete the value from the json store
		delete(jsonObj, pf.column)
		jsonStrb, err := json.Marshal(jsonObj)
		jsonStr = string(jsonStrb)
		if err != nil {
			return err
		}

		queryStr = "UPDATE " + pf.tableName + " SET"
		queryStr += " \"" + pf.column + "\" = ?, "
		queryStr += " autoscope_objectfields = ? "
		queryStr += " WHERE id = ?"

		_, err = postgresDB.connection.Exec(queryStr, val, jsonStr, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (postgresDB *PostgresDB) MigrationIndexColumn(pf MigrationStepIndexColumn) error {
	return nil
}


type PostgresRetrievalResult struct {
	Table Table
	Rows *sql.Rows
}

func (res PostgresRetrievalResult) Next() bool {
	return res.Rows.Next()
}

func (res PostgresRetrievalResult) Get() (map[string]interface{}, error) {
	row := make(map[string]interface{}, 0)
	cols, err := res.Rows.Columns()
	if err != nil {
		return row, err
	}

	types := typeArrs()

	//If the table doesn't yet exist, we're retrieving a result from
	// autoscope_unassigned
	tableCols := res.Table.Columns
	if res.Table.Status == "" {
		tableCols = map[string]string {
			"id": "bigint",
			"autoscope_uid": "bigint",
			"autoscope_gid": "bigint",
			"table_name": "string",
			"data": "jsonb",
		}
	}

	//Lookup each column and its type in res.Table, using this
	// to populate our return array types
	vals := make([]interface{}, len(cols))
	for idx, col := range cols {
		if ty, ok := tableCols[col]; !ok {
			log.Println(res.Table)
			return row, errors.New("Column returned and not found in schema: "+col)
		} else {
			ty = strings.Split(ty, "(")[0]
			if listContains(types["int"], ty) {
				var x sql.NullInt64
				vals[idx] = &x
			} else if listContains(types["float"], ty) {
				var x sql.NullFloat64
				vals[idx] = &x
			} else if listContains(types["str"], ty) {
				var x sql.NullString
				vals[idx] = &x
			} else if listContains(types["json"], ty) {
				var x sql.NullString
				vals[idx] = &x
			} else {
				return row, errors.New("Unknown postgres type returned: "+ty)
			}
		}
	}

	err = res.Rows.Scan(vals...)
	if err != nil {
		return row, err
	}

	//Pull our values out of the array by casting them appropriately
	for idx, col := range cols {
		if ty, ok := tableCols[col]; ok {
			ty = strings.Split(ty, "(")[0]
			if listContains(types["int"], ty) {
				v := vals[idx].(*sql.NullInt64)
				if v.Valid{
					row[col] = v.Int64
				}
			} else if listContains(types["str"], ty) {
				v := vals[idx].(*sql.NullString)
				if v.Valid{
					row[col] = v.String
				}
			} else if listContains(types["float"], ty) {
				v := vals[idx].(*sql.NullFloat64)
				if v.Valid{
					row[col] = v.Float64
				}
			} else if listContains(types["json"], ty) {
				v := vals[idx].(*sql.NullString)
				if !v.Valid { continue }

				//If col == autoscope_objectfields, populate its values
				if col == "autoscope_objectfields" {
					var res map[string]interface{}
					err := json.Unmarshal([]byte(v.String), &res)
					if err != nil {	return row, err	}
					for k, v := range res {
						//If there is a column and objectfield with same name,
						// throw an error
						if _, ok := row[k]; ok {
							return row, errors.New("Autoscope objectfield already exists as column in row")
						}
						//Otherwise just set the value
						row[k] = v
					}
				} else {
					row[col] = v.String
				}
			} else {
				return row, errors.New("Unknown postgres type returned: "+ty)
			}
		}
	}

	return row, err
}

type PostgresModificationResult struct {
	id int64
	rowsAffected int64
}

func (res PostgresModificationResult) LastInsertId() (int64, error){
	return res.id, nil
}

func (res PostgresModificationResult) RowsAffected() (int64, error){
	return res.rowsAffected, nil
}

//Simple means of securing SQL identities. Double quotes are not allowed.
func escapeSQLIdent(ident string) string {
	return strings.Replace(ident, "\"", "", -1)
	//return pq.QuoteIdentifier(ident)
}

//Helper function to convert ? placeholders to $1, $2 etc
func questionToPositional(query string, start int) string{
	res := ""
	i := start
	for _, c := range query {
		if c == '?' {
			res += "$" + strconv.Itoa(i)
			i += 1
		} else {
			res += string(c)
		}
	}
	return res
}

//Helper function to replace placeholder %s with their actual identifiers,
// as escaped by the pq library
func replaceIdentifiers(query string, idents []string) string{
	sanitized := make([]string, 0)
	for _, val := range idents {
		sanitized = append(sanitized, pq.QuoteIdentifier(val))
	}

	//Manually copy strings into interface array due to golang's casting policy
	args := make([]interface{}, 0)
	for _, val := range idents {
		args = append(args, val)
	}

	return fmt.Sprintf(query, args...)
}

//Returns the escaped version of a json property accessor
// ie) turns name into 'name' for use in autoscope_objectfields->>'name'
func jsonProp(field string) string {
	return "'" + strings.Replace(field, "'", "", -1) + "'"
}


//Transform relational field names (e.g. event__venue__owner) given our schema
// and a precomputed prefix map
// possible return values include: __event__venue.owner
//                      __event__venue.data->>owner [if column DNE]
//                      __event__venue.autoscope_objectfields->>owner [if table DNE]
//           For input of event__venue:
//                      __root.data->>venue [if original table DNE]
func relationalFieldTransform(schema map[string]Table, prefixes map[string]RelationPath, fieldName string, tableName string) string{
	if !strings.Contains(fieldName, "__"){
		_, tableExists := schema[tableName]
		_, colExists := schema[tableName].Columns[fieldName]
		if !tableExists || !colExists {
			//If the table doesn't exist, or the col doesn't exist,
			// access via autoscope_unassigned
			return "__root.autoscope_objectfields->>" + jsonProp(fieldName)
		} else if _, ok := schema[tableName].Columns[fieldName]; ok {
			//Otherwise, just do a normal query
			return "__root." + fieldName
		}
	}

	parts := strings.Split(fieldName, "__")
	prefix := "__" + strings.Join(parts[0:len(parts) - 1], "__")
	relPath := prefixes[prefix]
	field := parts[len(parts) - 1]

	if sch, ok := schema[relPath.Table]; !ok  {
		//If the table has not yet been created, we will query autoscope_unassigned
		return prefix + ".autoscope_objectfields->>" + jsonProp(field)
	} else if _, ok := sch.Columns[field]; !ok {
		//If the column has not yet been created, we use autoscope_objectfields->column
		return prefix + ".autoscope_objectfields->>" + jsonProp(field)
	} else {
		//Replace the last __ of relational identifiers with '.' for our SQL
		return prefix + "." + field
	}

	return fieldName
}

func relationalFormulaTransform(schema map[string]Table, prefixes map[string]RelationPath, formula Formula, tableName string) Formula{
	switch formula.(type){
	case AttrSelection:
		return AttrSelection{
			AttrA: relationalFieldTransform(schema, prefixes, formula.(AttrSelection).AttrA, tableName),
			AttrB: relationalFieldTransform(schema, prefixes, formula.(AttrSelection).AttrB, tableName),
			Op: formula.(AttrSelection).Op,
			CastA: "int",
			CastB: "int",
		}
	case ValueSelection:
		return ValueSelection{
			Attr: relationalFieldTransform(schema, prefixes, formula.(ValueSelection).Attr, tableName),
			Value: formula.(ValueSelection).Value,
			Op: formula.(ValueSelection).Op,
			/*Cast: "int",*/
		}
	}
	return formula
}

//Given a query, schema and a map of prefixes (given the query) to their relational
// paths, transform the query to represent the current DB structure
func RelationalQueryTransform(schema map[string]Table, prefixes map[string]RelationPath, query SelectQuery) SelectQuery {
	fn := func(f Formula) Formula {
		return relationalFormulaTransform(schema, prefixes, f, query.Table)
	}
	query.Selection = ModifyLeaves(fn, query.Selection)
	return query
}

//Perform a select query on the postgres database using relational filtering
// (e.g. event__venue__owner = "Jim")
func (postgresDB *PostgresDB) Select(schema map[string]Table, prefixes map[string]RelationPath, query SelectQuery) (RetrievalResult, error) {
	query.Table = strings.ToLower(query.Table)

	//If there are no query criteria, we assume all rows are being requested
	if query.Selection == nil {
		queryStr := "SELECT * FROM " + escapeSQLIdent(query.Table)
		rows, err := postgresDB.connection.Query(queryStr)
		if err != nil {
			log.Println(err.Error())
			return nil, err
		}
		return PostgresRetrievalResult{ Rows: rows, Table: schema[query.Table] }, nil
	}

	//If the table given by `query` doesn't exist, we need to
	// query autoscope_unassigned instead and modify the WHERE clause
	// appropriately
	if _, ok := schema[query.Table]; !ok {
		query.Selection = And{
			A: query.Selection,
			B: ValueSelection{ Attr: "table_name", Value: query.Table, Op: "=" },
		}
		query.Table = "autoscope_unassigned"
	}

	//Start our query
	queryStr := "SELECT __root.* FROM " + query.Table + " __root\n"

	//Transform our attribute names appropriately where necessary
	transformed := RelationalQueryTransform(schema, prefixes, query)

	//Generate the WHERE clause
	whereClause, err := transformed.Selection.toSQL()
	if err != nil {
		return nil, err
	}

	//Replace identifiers
	whereClauseSQL := replaceIdentifiers(whereClause.SQL, whereClause.Idents)

	//Replace ?s with $1s
	whereClauseSQL = questionToPositional(whereClauseSQL, 1)

	//Record which prefixes refer to tables that haven't been created
	// so we can create queries accordingly
	unassigned := make(map[string]bool, 0)

	//If the starting table hasn't yet been created, mark it as unassigned
	if _, ok := schema[query.Table]; !ok {
		unassigned["__root"] = true
	}

	// Sort prefixes by prefix length - this implicitly captures
	// all dependencies, since every prefix contains all its dependencies
	// in its string
	sortedPrefixes := make([]string, 0)
	for k, _ := range prefixes {
		sortedPrefixes = append(sortedPrefixes, k)
	}
	sort.Sort(ByLength(sortedPrefixes))

	//Add relational joins
	for _, prefix := range sortedPrefixes {
		path := prefixes[prefix]
		joinTable := path.Table
		additionalRestrictions := ""

		//If table doesn't exist, we need to use autoscope_unassigned table instead
		if _, ok := schema[path.Table]; !ok {
			joinTable = "autoscope_unassigned"
			//Record that this prefix refers to an unassigned table
			unassigned[prefix] = true
		}

		//If the table we're coming from doesn't exist, we need to use
		// (fromTablePrefix.data->>col)::int instead of fromTablePrefix.col
		// since fromTablePrefix refers to autoscope_unassigned
		fromTableSelection := path.FromTablePrefix + "." + path.FromField
		if _, ok := unassigned[path.FromTablePrefix]; ok {
			fromTableSelection = path.FromTablePrefix + ".autoscope_objectfields->>" + jsonProp(path.FromField)
			fromTableSelection = "(" + fromTableSelection + ")::int"

		} else if _, ok := schema[path.FromTable].Columns[path.FromField]; !ok {
			//If the table exists but the column doesn't, we need to access
			// the autoscope_objectfields column
			fromTableSelection = path.FromTablePrefix + ".autoscope_objectfields->>" + jsonProp(path.FromField)
			fromTableSelection = "(" + fromTableSelection + ")::int"
		}

		queryStr += "LEFT JOIN " + joinTable + " " + prefix
		queryStr += " on " + fromTableSelection + " = " + prefix + ".id"
		queryStr += " " + additionalRestrictions + "\n"
	}

	//Add WHERE clause
	queryStr +=" WHERE " + whereClauseSQL

	//Perform query
	rows, err := postgresDB.connection.Query(queryStr, whereClause.Args...)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}

	return PostgresRetrievalResult{ Rows: rows, Table: schema[query.Table] }, nil
}

//Perform an insert query on the postgres database
func (postgresDB *PostgresDB) Insert(schema map[string]Table, query InsertQuery) (ModificationResult, error) {
	var r PostgresModificationResult
	query.Table = strings.ToLower(query.Table)

	//If the table given by `query` doesn't exist, we need to
	// insert into autoscope_unassigned and create a new table_name value
	if _, ok := schema[query.Table]; !ok {
		query.Data["table_name"] = query.Table
		query = InsertQuery{
			Table: "autoscope_unassigned",
			Data: query.Data,
		}
	}

	queryStr := "INSERT INTO " + escapeSQLIdent(query.Table) + " ("
	valueStr := ""
	values := make([]interface{}, 0)
	jsonValues := make(map[string]interface{})
	i := 1
	for key, val := range query.Data {
		if _, ok := schema[query.Table].Columns[key]; ok {
			//Insert into normal column if exists
			queryStr += escapeSQLIdent(key) + ", "
			valueStr += "$" + strconv.Itoa(i) + ", "
			values = append(values, val)
			i += 1
		} else {
			//Add to jsonValues map
			jsonValues[key] = val
		}
	}

	//Generate the JSON strings
	jsonCol := ""
	jsonStr := ""
	if len(jsonValues) > 0 {
		jsonCol = "autoscope_objectfields"
		s, err := json.Marshal(jsonValues)
		if err != nil {
			return nil, err
		}
		jsonStr += "'" + string(s) + "'"
	}

	//Remove trailing commas from these lists if necessary
	if i > 0 && len(jsonValues) == 0 {
		queryStr = queryStr[0:len(queryStr) - 2]
		valueStr = valueStr[0: len(valueStr) - 2]
	}

	queryStr +=  jsonCol + ")"
	queryStr += " VALUES ("+ valueStr + jsonStr + ")"
	queryStr += " RETURNING id"

	var id int64
	err := postgresDB.connection.QueryRow(queryStr, values...).Scan(&id)
	r.id = id
	r.rowsAffected = 1
	return r, err
}


func (ps *PostgresDB) setup() error{
	//Set the postgres version
	rows, err := ps.connection.Query("select version()")
	if err != nil {
		return err
	}
	for rows.Next(){
		err := rows.Scan(&ps.version)
		if err != nil {
			return err
		}
	}
  return err
}

//Perform an update query on the postgres database
func (postgresDB *PostgresDB) Update(schema map[string]Table, prefixes map[string]RelationPath, query UpdateQuery) (ModificationResult, error) {
	/*
	UPDATE catalog_category
	SET    leaf_category = TRUE
	FROM   catalog_category c1
	LEFT   join catalog_category c2 ON c1.id = c2.parent_id
	WHERE  catalog_category.id = c1.id
	AND    c2.parent_id IS NULL;
	*/
	var r PostgresModificationResult
	query.Table = strings.ToLower(query.Table)
	//table := schema[query.Table]

	//If the table given by `query` doesn't exist, we need to
	// query autoscope_unassigned instead and modify the WHERE clause
	// appropriately
	if _, ok := schema[query.Table]; !ok {
		query.Selection = And{
			A: query.Selection,
			B: ValueSelection{ Attr: "table_name", Value: query.Table, Op: "=" },
		}
		query.Table = "autoscope_unassigned"
	}

	queryStr := "UPDATE " + escapeSQLIdent(query.Table) + " __root SET "
	i := 1
	colCount := 0

	values := make([]interface{}, 0)
	jsonValues := make(map[string]interface{})


	for key, val := range query.Data {
		if _, ok := schema[query.Table].Columns[key]; ok {
			//Query normal column if it exists
			queryStr += escapeSQLIdent(key) + "="
			queryStr += "$" + strconv.Itoa(i) + ", "
			values = append(values, val)
			colCount += 1
		} else {
			//Otherwise, build include the key/value pair in jsonValues
			jsonValues[key] = val
		}
		i += 1
	}

	//Remove trailing commas if necessary
	if i > 0 && len(jsonValues) == 0 {
		queryStr = queryStr[0:len(queryStr) - 2]
	}

	if len(jsonValues) > 0 {
		// If we need to update the autoscope_objectfields portion of the row,
		// we must first retrieve the whole row if we're using postgres < 9.5
		// TODO: Implement postgres 9.5 efficient update
		if postgresDB.version != "9.5" || true {
			selectQuery := SelectQuery{
				Selection: query.Selection,
				Table: query.Table,
			}
			res, err := postgresDB.Select(schema, prefixes, selectQuery)
			if err != nil { return nil, err }
			b := res.Next()
			if !b { return nil, errors.New("Failed to retrieve row we're attempting to update") }
			resMap, err := res.Get()
			if err != nil { return nil, err }

			//Insert any old values that we're not overwriting into our
			// map of object field names to values
			for k, v := range resMap {
				if _, ok := schema[query.Table].Columns[k]; !ok {
					if _, ok := jsonValues[k]; !ok {
						jsonValues[k] = v
					}
				}
			}

			s, err := json.Marshal(jsonValues)
			if err != nil { return nil, err	}
			queryStr += "autoscope_objectfields = "
			queryStr += "'" + string(s) + "'"
		}
	}

	//Transform our attribute names appropriately where necessary
	fn := func(f Formula) Formula {
		return relationalFormulaTransform(schema, prefixes, f, query.Table)
	}
	query.Selection = ModifyLeaves(fn, query.Selection)

	whereClause, err := query.Selection.toSQL()
	if err != nil { return r, err	}

	//Replace identifiers
	whereClauseSQL := replaceIdentifiers(whereClause.SQL, whereClause.Idents)

	//Replace ?s with $1s, starting at $(len(values) + 1)
	whereClauseSQL = questionToPositional(whereClauseSQL, len(values) + 1)
	queryStr += " WHERE " + whereClauseSQL

	log.Println("-------------------------------")
	log.Println(queryStr)
	res, err := postgresDB.connection.Exec(queryStr, append(values, whereClause.Args...)...)
	if err != nil { return r, err }
	rowsAffected, err := res.RowsAffected()
	r.rowsAffected = rowsAffected
	return r, err
}
