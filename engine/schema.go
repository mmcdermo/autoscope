package engine

import (
	"database/sql"
)

//Struct representing the state of a table
type Table struct {
	Name string
	Columns map[string]string      //Column name -> column type
	ObjectFields map[string]string //Top N used non-column object fields
	Indices []string
}

//Struct representing the in-memory accumulation
// of table statistics for this node. These will be regularly aggregated
// into a shared postgres table.
type TableQueryStats struct {
	TotalQueries int

	//Map from cols/object-fields -> number of queries using those fields as restrictions
	// TODO: Use combinations of object fields as keys (for composite indices)
	Restrictions map[string]int

	// Map from cols/object-fields -> number of rows using those fields
	ObjectFieldCount map[string]int
}

type MigrationStep struct {
	Table string

	//MigrationStep Types include:
	// CreateTable  - Create a new table
	// PromoteField - Promote an ObjectField to Column, migrating the data
	//                (during migration, the field will count as both column & OF,
	//                 and nodes will use the column for INSERT but col+OF for WHERE)
	// IndexColumn  - Create a new index on a column
	Type string

	// Arguments necessary to perform migration step (table name, field name, etc)
	Args map[string]string
}

//Get the current table schema from postgres
func SchemaFromPostgres(conn *sql.DB) (map[string]Table, error){
	tables := make(map[string]Table)
	return tables, nil
}

//Create a sequence of migration steps
func CreateMigration(config *Config, tables map[string]Table, tableStats map[string]TableQueryStats) ([]MigrationStep, error){
	steps := make([]MigrationStep, 0)
	return steps, nil
}
