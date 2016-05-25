package engine

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"strconv"
	"os"
)
/*
  TODO: Insert/Update - consider case where ObjectField column DNE

*/

type ColumnInfo struct {
	//The name of the column
	Name string `yaml:"name,omitempty"`
	//The data type of the column
	DataType string `yaml:"data_type,omitempty"`
	//For character types, the maximum length for this field
	CharMaxLength int64 `yaml:"max_length,omitempty"`
	//For numeric types, the number of significant digits for this field
	NumericPrecision int64 `yaml:"numeric_precision,omitempty"`
	//For numeric types, the radix of the numeric precision (base 2 or 10)
	NumericPrecisionRadix int64 `yaml:"numeric_precision_radix,omitempty"`
	//For decimal types, the number of significant digits to the right of the decimal
	NumericScale int64 `yaml:"numeric_scale,omitempty"`
}

//Struct representing the state of a table
type Table struct {
	//Name of the table, as stored in the database
	Name string `yaml:"name,omitempty"`
	//Column name -> column info string
	Columns map[string]string `yaml:"columns,omitempty"`
	//Top N used non-column object fields
	ObjectFields map[string]string `yaml:"object_fields,omitempty"`
	//Names of indexed columns
	Indices []string `yaml:"indices,omitempty"`
	//Table name aliases. Permits legacy code to reference other table names
	Aliases []string `yaml:"aliases,omitempty"`
	//Status of the table: live, migrating or blank (doesn't yet exist)
	Status string `yaml:"status,omitempty"`
}


//MigrationStep Types include:
// CreateTable  - Create a new table
// PromoteField - Promote an ObjectField to Column, migrating the data
//                (during migration, the field will count as both column & OF,
//                 and nodes will use the column for INSERT but col+OF for WHERE)
// IndexColumn  - Create a new index on a column

type MigrationStep interface {
	TableName() string
	ToString() string
}

//Migration step to create a new table
type MigrationStepCreateTable struct {
	tableName string
	table Table
}
func (ct MigrationStepCreateTable) TableName() string {
	return ct.tableName
}
func (ct MigrationStepCreateTable) ToString() string {
	return "CreateTable: "+ct.tableName
}

//Migration step to create a new index on a column
type MigrationStepIndexColumn struct {
	tableName string
	column string
}
func (ic MigrationStepIndexColumn) TableName() string {
	return ic.tableName
}
func (ic MigrationStepIndexColumn) ToString() string {
	return "Create index '" + ic.column + "' for table " + ic.tableName
}

//Migration step to promote an object field to a column
type MigrationStepPromoteField struct {
	tableName string
	table Table
	column string
	columnType string
}
func (pf MigrationStepPromoteField) TableName() string {
	return pf.tableName
}
func (pf MigrationStepPromoteField) ToString() string {
	return "Promote object field '" + pf.column + "' for table " + pf.tableName
}


//Returns a map of type associations
// Internally, autoscope only stores several basic types:
//  string, int64, float64, decimal, and json
func typeArrs() map[string][]string {
	return map[string][]string{
		"str" : []string{"varchar", "text", "char", "string"},
		"int" : []string{"int", "smallint", "integer", "bigint", "serial"},
		"float" : []string{"float", "double"},
		"decimal" : []string{"decimal"},
		"json" : []string{"json", "jsonb"},
	}
}

//Convert a ColumnInfo object to its string representation (e.g. varchar(20))
func (ci ColumnInfo) ToString() string {
	typeMap := map[string]string{
		"character varying": "varchar",
		"real": "float",
		"double precision": "double",
		"numeric": "decimal",
	}
	ty := ci.DataType
	if val, ok := typeMap[ci.DataType]; ok {
		ty = val
	}
	types := typeArrs()
	if listContains(types["str"], ty) {
		if ci.CharMaxLength == 0 {
			return ty
		} else {
			return ty+"("+strconv.FormatInt(ci.CharMaxLength, 10)+")"
		}
	} else if listContains(types["decimal"], ty) {
		return ty+"("+strconv.FormatInt(ci.NumericPrecision, 10)+","+strconv.FormatInt(ci.NumericPrecisionRadix, 10)+")"
	}
	return ty
}

//Generate a new schema, based on available configs and statistics
func GenerateNewSchema(config *Config, currentSchema map[string]Table, globalTableStats map[string]TableQueryStats) (map[string]Table, error) {
	newSchema := make(map[string]Table)
	for k, v := range currentSchema {
		newSchema[k] = v
	}

	// Include autoscope's internal tables
	autoscopeTables, err := AutoscopeTableSchemas()
	if err != nil {
		return nil, err
	}
	for _, table := range autoscopeTables {
		newSchema[table.Name] = table
	}

	// Create any new tables from currently Unassigned data
	newTables := NewTableSchemas(config, currentSchema, globalTableStats)
	for _, table := range newTables {
		newSchema[table.Name] = table
	}

	//TODO: include any user-defined table settings

	//TODO: ensure tables in currentSchema have ids and autoscope_objectfield cols

	//Make any changes necessary to individual field values
	newSchema = FieldSchemaChanges(config, newSchema, globalTableStats)

	return newSchema, nil
}


//Update the new schema with changes to any fields
// For now, this will only be field promotions (object-field to column)
func FieldSchemaChanges(config *Config, newSchema map[string]Table, globalTableStats map[string]TableQueryStats) map[string]Table {
	//For every table
	for tableName, table := range newSchema {
		//Get its stats
		if stats, ok := globalTableStats[tableName]; ok {
			//For every field
			for field, countMap := range stats.ObjectFieldCount {
				//Check whether this field should be promoted

				//Determine what type to make the new field
				//Here, we're simply choosing the type most commonly used in this column.
				//Although practical, we may desire more formal semantics for
				// the promotion of an object field to a full column
				ty := maxKey(countMap)
				maxCount := countMap[ty]

				if maxCount > config.NewFieldThreshhold {
					if _, ok := table.Columns[field]; ok {
						//Field already exists
						if table.Columns[field] != ty {
							log.Println("Field schema update error for table '"+tableName+"'. Incompatible types. Column should be "+ty+" but is "+table.Columns[field])
						}
					} else {
						//Create the field
						table.Columns[field] = ty
					}
				}
			}
		}
	}
	return newSchema
}

//Generate the current needed migration steps, if any, to migrate
// from currentSchema to newSchema
func CreateMigration(config *Config, currentSchema map[string]Table, newSchema map[string]Table) ([]MigrationStep, error){
	steps := make([]MigrationStep, 0)

	// Create any missing tables from newSchema
	for _, table := range newSchema {
		if _, ok := currentSchema[table.Name]; !ok {
			steps = append(steps, MigrationStepCreateTable{
				tableName: table.Name,
				table: table,
			})
		}
	}

	// Update tables as necessary
	for _, table := range currentSchema {
		if newTable, ok := newSchema[table.Name]; ok {
			steps = append(steps, TableDiffMigrationSteps(table, newTable)...)
		}
	}

	return steps, nil
}

// Returns a list of any tables that need to be created, given
// current usage statistics
func NewTableSchemas(config *Config, currentSchema map[string]Table, globalTableStats map[string]TableQueryStats) ([]Table){
	newTables := make([]Table, 0)
	//Any table with stats is a possible candidate
	for tableName, stats := range globalTableStats {
		//Ensure table doesn't already exist in schema
		if _, ok := currentSchema[tableName]; !ok {
			//Determine whether or not to create the table given the
			// available statistics
			if stats.InsertQueries > config.NewTableRowsThreshhold {
				//Create the table with the required autoscope columns: id and autoscope_objectfields
				table := Table{Name: tableName,
					Columns: map[string]string{
						"id": "bigint",
						"autoscope_objectfields": "json",
					},
				}
				newTables = append(newTables, table)
			}
		}
	}
	return newTables
}

// Take two table schemas and produce migration steps to migrate between them
// For now this will only create new columns, indices
func TableDiffMigrationSteps(oldSchema Table, newSchema Table) []MigrationStep {
	steps := make([]MigrationStep, 0)

	// Create new indices as necessary
	for _, newIndex := range newSchema.Indices {
		found := false
		for _, oldIndex := range oldSchema.Indices {
			if newIndex == oldIndex {
				found = true
			}
		}
		if !found {
			steps = append(steps, MigrationStepIndexColumn{
				tableName: newSchema.Name,
				column: newIndex,
			})
		}
	}

	// Create new columns as necessary
	for newColumn, _ := range newSchema.Columns {
		if _, ok := oldSchema.Columns[newColumn]; !ok {
			steps = append(steps, MigrationStepPromoteField{
				tableName: newSchema.Name,
				table: newSchema,
				column: newColumn,
			})
		}
	}

	return steps
}

//Adds default fields of id, autoscope_objectfields, autoscope_uid, and
// autoscope_gid to a table definition
func AddDefaultFields(t Table) Table {
	t.Columns["id"] = "serial"
	t.Columns["autoscope_uid"] = "bigint"
	t.Columns["autoscope_gid"] = "bigint"
	return t
}

//Extract autoscope table schemas from autoscope_tables.yml
func AutoscopeTableSchemas() ([]Table, error){
	contents, err := ioutil.ReadFile(os.Getenv("AUTOSCOPE_CONFIG_DIR") + "/autoscope_tables.yml")
	if err != nil {
		log.Fatal("Failed to read autoscope_tables.yml")
	}

	var tables map[string]map[string]Table
	err = yaml.Unmarshal([]byte(contents), &tables)
	if err != nil {
		log.Fatal("Failed to load yaml from config file: "+err.Error())
	}
	tableStructs := make([]Table, 0)

	for k, table := range tables["tables"] {
		table.Name = k
		tableStructs = append(tableStructs, AddDefaultFields(table))
	}
	return tableStructs, nil
}
