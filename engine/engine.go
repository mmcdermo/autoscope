package engine

import (
	"errors"
	_ "strconv"
	"log"
	"strings"
	"sync"
)

/* TODO
     - Update stats after performing queries
     - Periodically perform migrations (given config)

     - Test migration generation [schema]
     - Test engine [using mem db type?]
       + Basic queries
       + Stat additions
       + Generation of migrations

     - Add distributed engine functionality

*/

//Autoscope config struct
type Config struct {
	Port string `yaml:"port"`
	DB_USER string `yaml:"db_user"`
	DB_HOST string `yaml:"db_host"`
	DB_NAME string `yaml:"db_name"`
	DB_PASSWORD string `yaml:"db_password"`
	DB_PREFIX string `yaml:"db_prefix"`
	DatabaseType string `yaml:"database_type"`
	NewTableRowsThreshhold int64 `yaml:"new_table_rows_threshhold"`
	NewFieldThreshhold int64 `yaml:"new_field_threshhold"`
}

//Main data structure for an instance of the Autoscope Engine
type Engine struct {
	DB AutoscopeDB
	Config *Config
	Schema map[string]Table
	SchemaLock sync.RWMutex
	GlobalStats map[string]TableQueryStats
	GlobalStatsLock sync.RWMutex
	LocalStats map[string]TableQueryStats
	LocalStatsLock sync.RWMutex
}

// In order to accurately aggregate our local stats into the database
// but also be able to quickly reference global stats, we store both independently.
//
//                +---------------------+
//	              | Global Stats Obj    |
//	+--------+--> |                     + <--+-------+
//	| Query  |    +---------------------+    |       |
//  |        |                               |  DB   |
//  +--------+--> +---------------------+    |       |
//               	| Local Stats Obj     +--> +-------+
//	              |                     |
//	              +---------------------+
//
// General strategy for stat updates:
// - Update both as we process queries
// - Reload GlobalStats whenever we flush to DB or get updates from other nodes
// - Fully flush LocalStats whenever we push to DB
// - Only use GlobalStats as our reference

// TableQueryStats is the struct representing either Global or Local stats for
//  a given query
type TableQueryStats struct {
	//Number of insert queries observed on this table
	InsertQueries int64
	//Number of select queries observed on this table
	SelectQueries int64
	//Number of update queries observed on this table
	UpdateQueries int64
	//Map from cols/object-fields -> number of queries with field as restriction
	// TODO: Use combinations of object fields as keys (for composite indices)
	Restrictions map[string]int64
	// Map from cols/object-fields -> types used in this column ->
	// number of rows using those fields as that type
	ObjectFieldCount map[string]map[string]int64
	// Map from cols/object-fields -> tables this foreign key references ->
	// number of rows with that foreign key
	ForeignKeyCount map[string]map[string]int64
}

//Initialize the engine with a given config
func (e *Engine) Init(config *Config) (error){
	e.Config = config
	switch config.DatabaseType {
	case "postgres":
		e.DB = AutoscopeDB(&PostgresDB{})
		err := e.DB.Connect(config)
		if err != nil { return err }
		break
	case "memdb":
		e.DB = AutoscopeDB(&MemDB{})
		err := e.DB.Connect(config)
		if err != nil { return err }
		break
	default:
		return errors.New("Please specify a known database type (postgres, mysql, etc). Found: '"+config.DatabaseType+"'")
	}

	//Load default schema
	defTables, err := AutoscopeTableSchemas()
	if err != nil { return err }
	defSchema := make(map[string]Table, 0)
	for _, table := range defTables {
		defSchema[table.Name] = table
	}

	//Create migration to new schema
	migration, err := CreateMigration(config, nil, defSchema)

	//Perform migration
	err = e.DB.PerformMigration(migration)
	if err != nil { return err }

	return nil
}

//Data structure to hold information about a prefix of a relational path
type RelationPath struct {
	Table string
	FromTable string
	FromTablePrefix string
	FromField string
}

//Perform a Select query using the engine
func (e *Engine) Select(query SelectQuery) (RetrievalResult, error){
	e.SchemaLock.RLock()
	defer e.SchemaLock.RUnlock()

	e.GlobalStatsLock.RLock()
	prefixes, err := genPrefixes(e.Schema, e.GlobalStats, query.Table, query.Selection)
	e.GlobalStatsLock.RUnlock()

	if err != nil { return nil, err }
	r, err := e.DB.Select(e.Schema, prefixes, query)

	e.GlobalStatsLock.Lock()

	//Update global stats
	e.GlobalStatsLock.Lock()
	stats := e.GlobalStats[query.Table]
	//Update UpdateQueries stats
	stats.SelectQueries += 1
	//Update restriction stats
	for _, prefix := range prefixes {
		tstats, ok := e.GlobalStats[prefix.FromTable]
		if !ok {
			tstats = defStats()
		}

		if _, ok := tstats.Restrictions[prefix.FromField]; !ok {
			tstats.Restrictions[prefix.FromField] = 0
		}
		tstats.Restrictions[prefix.FromField] += 1
		e.GlobalStats[prefix.FromTable] = tstats
	}
	e.GlobalStats[query.Table] = stats
	e.GlobalStatsLock.Unlock()

	return r, err
}

//Perform an Update query using the engine
func (e *Engine) Update(query UpdateQuery) (ModificationResult, error){
	e.SchemaLock.RLock()
	defer e.SchemaLock.RUnlock()

	e.GlobalStatsLock.RLock()
	prefixes, err := genPrefixes(e.Schema, e.GlobalStats, query.Table, query.Selection)
	e.GlobalStatsLock.RUnlock()

	if err != nil { return nil, err }
	r, err := e.DB.Update(e.Schema, prefixes, query)

	//Update global stats
	e.GlobalStatsLock.Lock()
	stats := e.GlobalStats[query.Table]
	//Update UpdateQueries stats
	stats.InsertQueries += 1
	//Update foreign key stats
	for field, table := range query.ForeignKeys {
		stats.ForeignKeyCount = incrementCountMap(stats.ForeignKeyCount, field, table)

	}
	//Update object field stats
	for k, v := range query.Data {
		ty := TypeFromValue(v)
		stats.ObjectFieldCount = incrementCountMap(stats.ObjectFieldCount, k, ty)
	}
	//Update restriction stats
	for _, prefix := range prefixes {
		tstats, ok := e.GlobalStats[prefix.FromTable]
		if !ok {
			tstats = defStats()
		}

		if _, ok := tstats.Restrictions[prefix.FromField]; !ok {
			tstats.Restrictions[prefix.FromField] = 0
		}
		tstats.Restrictions[prefix.FromField] += 1
		e.GlobalStats[prefix.FromTable] = tstats
	}
	e.GlobalStats[query.Table] = stats
	e.GlobalStatsLock.Unlock()
	return r, err
}

//Perform an Insert query using the engine
func (e *Engine) Insert(query InsertQuery) (ModificationResult, error){
	e.SchemaLock.RLock()
	defer e.SchemaLock.RUnlock()

	r, err := e.DB.Insert(e.Schema, query)

	e.GlobalStatsLock.Lock()
	stats := e.GlobalStats[query.Table]
	//Update InsertQueries stats
	stats.InsertQueries += 1
	//Update foreign key stats
	for field, table := range query.ForeignKeys {
		stats.ForeignKeyCount = incrementCountMap(stats.ForeignKeyCount, field, table)

	}
	//Update object field stats
	for k, v := range query.Data {
		ty := TypeFromValue(v)
		stats.ObjectFieldCount = incrementCountMap(stats.ObjectFieldCount, k, ty)
	}
	e.GlobalStats[query.Table] = stats
	e.GlobalStatsLock.Unlock()

	return r, err
}

//Helper function to return an empty table stats struct
func defStats() TableQueryStats {
	return TableQueryStats{
		InsertQueries: 0,
		SelectQueries: 0,
		UpdateQueries: 0,
		Restrictions: make(map[string]int64, 0),
		ObjectFieldCount: make(map[string]map[string]int64, 0),
		ForeignKeyCount: make(map[string]map[string]int64, 0),
	}
}

func (e *Engine) loadGlobalStats() error {
	e.GlobalStatsLock.Lock()
	defer e.GlobalStatsLock.Unlock()

	log.Println("Loading stats")

	//Load basic table stats
	query := SelectQuery{ Table: "autoscope_table_stats" }
	res, err := e.DB.Select(e.Schema, nil, query)
	if err != nil { return err }
	for res.Next() {
		row, err := res.Get()
		if err != nil { return err }
		name := row["table_name"].(string)
		stats, ok := e.GlobalStats[name]
		if !ok { stats = defStats() }
		stats.InsertQueries += row["insert_queries"].(int64)
		stats.SelectQueries += row["select_queries"].(int64)
		stats.UpdateQueries += row["update_queries"].(int64)
		e.GlobalStats[name] = stats
	}

	//Load restriction stats
	query = SelectQuery{ Table: "autoscope_restriction_stats" }
	res, err = e.DB.Select(e.Schema, nil, query)
	if err != nil { return err }
	for res.Next() {
		row, err := res.Get()
		if err != nil { return err }
		name := row["table_name"].(string)
		stats, ok := e.GlobalStats[name]
		if !ok { stats = defStats() }
		stats.Restrictions["restriction"] += row["queries"].(int64)
		e.GlobalStats[name] = stats
	}

	//Load object field stats
	query = SelectQuery{ Table: "autoscope_object_field_stats" }
	res, err = e.DB.Select(e.Schema, nil, query)
	if err != nil { return err }
	for res.Next() {
		row, err := res.Get()
		if err != nil { return err }
		name := row["table_name"].(string)
		stats, ok := e.GlobalStats[name]
		if !ok { stats = defStats() }

		ofMap, ok := stats.ObjectFieldCount[row["object_field_name"].(string)]
		if !ok { ofMap = make(map[string]int64, 0) }
		ofMap[row["type"].(string)] = row["occurrences"].(int64)
		stats.ObjectFieldCount[row["object_field_name"].(string)] = ofMap
		e.GlobalStats[name] = stats
	}

	//Load foreign key stats
	query = SelectQuery{ Table: "autoscope_foreignkey_stats" }
	res, err = e.DB.Select(e.Schema, nil, query)
	if err != nil { return err }
	for res.Next() {
		row, err := res.Get()
		if err != nil { return err }
		name := row["table_name"].(string)
		stats, ok := e.GlobalStats[name]
		if !ok { stats = defStats() }

		ofMap, ok := stats.ObjectFieldCount[row["object_field_name"].(string)]
		if !ok { ofMap = make(map[string]int64, 0) }
		ofMap[row["foreign_table_name"].(string)] = row["occurrences"].(int64)
		stats.ObjectFieldCount[row["object_field_name"].(string)] = ofMap
		e.GlobalStats[name] = stats
	}

	return nil
}

//Flushes our local stats to the database, zeroing them as it goes
func (e *Engine) flushStatsToDB() error {
	// TODO: Obtain inter-node lock on query stat updates

	e.LocalStatsLock.Lock()
	defer e.LocalStatsLock.Unlock()
	for table, stats := range e.LocalStats {

		//Update the basic table stats
		restrictions := map[string]interface{}{
			"table_name": interface{}(table),
		}
		updates := map[string]int64{
			"insert_queries": stats.InsertQueries,
			"update_queries": stats.UpdateQueries,
			"select_queries": stats.SelectQueries,
		}
		err := e.IncrementColumns("autoscope_table_stats", restrictions, updates)
		if err != nil { return err }
		stats.InsertQueries = 0
		stats.UpdateQueries = 0
		stats.SelectQueries = 0
		e.LocalStats[table] = stats

		//Update restriction stats
		for k, v := range stats.Restrictions {
			restrictions := map[string]interface{}{
				"table_name": interface{}(table),
				"restriction": interface{}(k),
			}
			updates := map[string]int64{
				"queries": v,
			}
			err := e.IncrementColumns("autoscope_table_stats", restrictions, updates)
			if err != nil { return err }
			stats.Restrictions[k] = 0
			e.LocalStats[table] = stats
		}

		//Update ObjectFieldCount stats
		for col, m := range stats.ObjectFieldCount {
			for ty, v := range m {
				restrictions := map[string]interface{}{
					"table_name": interface{}(table),
					"object_field_name": interface{}(col),
					"type": ty,
				}
				updates := map[string]int64{
					"occurrences": v,
				}
				err := e.IncrementColumns("autoscope_table_stats", restrictions, updates)
				if err != nil { return err }
				stats.ObjectFieldCount[col][ty] = 0
				e.LocalStats[table] = stats
			}
		}

		//Update ForeignKeyCount stats
		for col, m := range stats.ForeignKeyCount {
			for foreignTable, v := range m {
				restrictions := map[string]interface{}{
					"table_name": interface{}(table),
					"object_field_name": interface{}(col),
					"foreign_table_name": interface{}(foreignTable),
				}
				updates := map[string]int64{
					"occurrences": v,
				}
				err := e.IncrementColumns("autoscope_table_stats", restrictions, updates)
				if err != nil { return err }
				stats.ForeignKeyCount[col][foreignTable] = 0
				e.LocalStats[table] = stats
			}
		}
	}
	return nil
}



//Helper function to select a row matching `restrictions` and increment
// the value present in each column in `columns`.
// If no row is present, incrementColumns will insert the appropriate row with 1 values
// in counter columns.
func (e *Engine) IncrementColumns(tableName string, restrictions map[string]interface{}, columns map[string]int64) error {
	//TODO: Obtain inter-node lock on this table + restriction hash
	// For databases offering transactions, those could be used instead

	//Convert restriction map type
	selection := MapToAnds(restrictions)
	query := SelectQuery{
		Table: tableName,
		Selection: selection,
	}
	//Pull out the appropriate row
	res, err := e.DB.Select(e.Schema, nil, query)
	if err != nil {	return err }

	if res.Next() {
		//Row exists: Increment relevant columns and update
		row, err := res.Get()
		if err != nil { return err }
		for k, v := range row {
			if quantity, ok := columns[k]; ok {
				//Ensure that the internal type of this column is int64
				switch v.(type) {
				case int64:
					break
				default:
					return errors.New("Cannot increment column unless it's an integer ("+tableName+"."+k+")")
				}
				//Increment the value
				row[k] = v.(int64) + quantity
			}
		}
		query := UpdateQuery{
			Table: tableName,
			Selection: selection,
			Data: row,
		}
		_, err = e.DB.Update(e.Schema, nil, query)
		if err != nil { return err }

	} else {
		//Insert a new row with the given restrictions, initializing all values to 1
		for col, quantity := range columns {
			restrictions[col] = quantity
		}
		query := InsertQuery {
			Table: tableName,
			Data: restrictions,
		}
		_, err = e.DB.Insert(e.Schema, query)
		if err != nil { return err }
	}
	return nil
}

// Update the stats regarding how often certain fields are used as foriegn keys
// This data is used to determine where foreign keys lead for SELECT queries
func updateForeignKeyCount(stats TableQueryStats, foreignKeys map[string]string) TableQueryStats {
	for field, tableRef := range foreignKeys {
		stats.ForeignKeyCount = incrementCountMap(stats.ForeignKeyCount, field, tableRef)
	}
	return stats
}

//Function to derive an autoscope type string from a given JSON-deserialized value
func TypeFromValue(val interface{}) string {
	switch val.(type){
	case int:
		return "int"
	case int64:
		return "int"
	case float32:
		return "float"
	case float64:
		return "float"
	case string:
		return "string"
	default:
		return "unknown"
	}
}

// Update the stats regarding how often certain columns are contained in the data
// This data is used to decide when to create new columns
func updateObjectFieldCount(stats TableQueryStats, query InsertQuery) TableQueryStats{
	for field, val := range query.Data {
		ty := TypeFromValue(val)
		stats.ObjectFieldCount = incrementCountMap(stats.ObjectFieldCount, field, ty)
	}
	return stats
}

// Update the stats regarding how often certain columns are use as query restrictions
// This data is used to decide when to create indices
func updateRestrictions(schema map[string]Table, stats map[string]TableQueryStats, prefixes map[string]RelationPath, tableName string, sqlParts SQLPart) map[string]TableQueryStats {
	for _, ident := range sqlParts.Idents {
		identParts := strings.Split(ident, "__")
		targetTable := tableName
		targetIdent := identParts[len(identParts) - 1]

		//If the restriction is relational, we need to use the prefixes
		// to determine which table the restriction applies to
		if len(identParts) > 1 {
			prefix := strings.Join(identParts[0:len(identParts) - 1], "__")
			relPath := prefixes[prefix]
			targetTable = relPath.Table
		}

		if _, ok := stats[targetTable].Restrictions[targetIdent]; ok {
			stats[targetTable].Restrictions[targetIdent] += 1
		} else {
			stats[targetTable].Restrictions[targetIdent] = 1
		}
	}

	return stats
}

//Generates information about the prefixes of relational paths (venue__owner__name)
// For example, venue__owner -> {Table: "people", FromTable: "venues", ...}
func genPrefixes(schema map[string]Table, stats map[string]TableQueryStats, tableName string, selection Formula) (map[string]RelationPath, error) {
	parts, err := selection.toSQL()

	if err != nil {
		return nil, err
	}

	//Map of prefixes to the relation that they represent
	prefixes := make(map[string]RelationPath)

	//For every identifier, identify all prefixes and determine
	// which relations they represent
	// e.g. venue__owner__name represents event.venue -> venue.owner -> person.name
	for _, ident := range parts.Idents {
		if strings.Contains(ident, "__") {
			fields := strings.Split(ident, "__")
			for i, prefix := range fields {
				//Don't generate a prefix for the last value, since it
				// is not a reference to another table, but a field
				if i == len(fields) - 1 { break }
				startTable := tableName + ""
				startPrefix := ""
				startField := prefix + ""

				for j, field := range fields {
					if i == j {	break	}
					startPrefix += "__" + field
				}

				//If the prefix has already been iterated over,
				// use the table from RelationPath
				if _, ok := prefixes[startPrefix]; ok {
					startTable = prefixes[startPrefix].Table
				}

				//Determine which table is next using tableStats
				nextTable := maxKey(stats[startTable].ForeignKeyCount[startField])

				//Determine the prefix of the table we're referenced from
				fromTablePrefix := startPrefix
				if fromTablePrefix == "" {
					fromTablePrefix = "__root"
				}

				prefixes[startPrefix + "__" + prefix] = RelationPath{
					Table: nextTable,
					FromTable: startTable,
					FromTablePrefix: fromTablePrefix,
					FromField: startField,
				}
			}
		}
	}
	return prefixes, nil
}