package engine

import (
	"testing"
	"os"
	"io/ioutil"
	"gopkg.in/yaml.v2"
	"log"
	"strconv"
)

var (
	config *Config
	configured = false
)


/* Setup the database by
   - Loading the test config
   - Clearing out the database
   - Recreating default autoscope tables
*/

func TestMain(m *testing.M) {
	//Load the test config
	config_dir := os.Getenv("AUTOSCOPE_CONFIG_DIR")
	contents, err := ioutil.ReadFile(config_dir+"/test_postgres.yml")
	if err != nil {
		configured = false
		log.Println("Failed to read config file.")
		os.Exit(m.Run())
		return
	}
	err = yaml.Unmarshal([]byte(contents), &config)
	if err != nil {
		configured = false
		log.Println("Failed to load yaml from config file.")
		os.Exit(m.Run())
		return
	}

	//Connect to the database
	var ps PostgresDB
	err = ps.Connect(config)
	if err != nil {
		configured = false
		log.Println(err.Error())
		os.Exit(m.Run())
		return
	}

	//Load the current schema
	currentSchema, err := ps.CurrentSchema()
	if err != nil {
		configured = false
		log.Println(err.Error())
	}

	//Clear out any existing tables
	for n, _ := range currentSchema {
		log.Println("Dropping table: "+n)
		_, err := ps.connection.Exec("drop table "+n+"")
		if err != nil {
			configured = false
			log.Println(err.Error())
		}
	}

	//Load the current schema again
	currentSchema, err = ps.CurrentSchema()

	//Generate new schema
	newSchema, err := GenerateNewSchema(config, currentSchema, nil)
	if err != nil {
		configured = false
		log.Println(err.Error())
	}

	//Create migration to new schema
	migration, err := CreateMigration(config, currentSchema, newSchema)
	for _, step := range migration {
		log.Println(step.ToString())
	}

	//Perform migration
	err = ps.PerformMigration(migration)
	if err != nil {
		configured = false
		log.Println(err)
	}

	if !configured {
		config = nil
	}
	os.Exit(m.Run())
}

//With this test, we ensure that the default autoscope tables are
// created correctly
func TestInitialPostgresMigration(t *testing.T){
	var ps PostgresDB

	//Connect to the database
	err := ps.Connect(config)
	if err != nil {
		t.Fatal(err.Error())
	}

	//Load the current schema
	currentSchema, err := ps.CurrentSchema()
	if err != nil {
		t.Fatal(err.Error())
	}

	//Generate new schema
	newSchema, err := GenerateNewSchema(config, currentSchema, nil)
	if err != nil {
		t.Fatal(err.Error())
	}

	//Ensure schemas match
	migratedSchema, err := ps.CurrentSchema()
	steps, err := CreateMigration(config, newSchema, migratedSchema)
	if len(steps) != 0 {
		t.Fatal("Migration incomplete")
	}
}


func TestBasicSQL(t *testing.T){
	var ps PostgresDB

	//Connect to the database
	err := ps.Connect(config)
	if err != nil {
		t.Fatal(err.Error())
	}

	//Create a test table
	testTable := Table{
		Name: "testTable",
		Columns: map[string]string{
			"id": "serial",
			"strcol": "text",
			"intcol": "bigint",
			"floatcol": "float",
			"jsoncol": "json",
			"autoscope_objectfields": "json",
		},
		Status: "created",
	}
	currentSchema, err := ps.CurrentSchema()
	if err != nil { t.Fatal(err.Error()) }
	steps, err := CreateMigration(config, currentSchema, map[string]Table{
		"testTable": testTable,
	})
	if err != nil { t.Fatal(err.Error()) }
	err = ps.PerformMigration(steps)
	if err != nil { t.Fatal(err.Error()) }
	newSchema, err := ps.CurrentSchema()

	//Insert a row
	insertQuery := InsertQuery{
		Table: "testTable",
		Data: map[string]interface{}{
			"strcol": "myStr",
			"intcol": int64(42),
			"floatcol": 42.99,
			"jsoncol": "{\"x\": 99}",
		},
	}

	ires, err := ps.Insert(newSchema, insertQuery)
	if err != nil { t.Fatal(err.Error()) }

	//Retrieve that row
	selectQuery := SelectQuery{
		Table: "testTable",
		Selection: Or{
			A: AttrSelection{ AttrA: "intcol", AttrB: "jsoncol ->> 'x'", Op: "=" },
			B: ValueSelection{ Attr: "strcol", Value: "myStr", Op: "="},
		},
	}

	res, err := ps.Select(newSchema, nil, selectQuery)
	if err != nil { t.Fatal(err.Error()) }

	if !res.Next(){
		t.Fatal("No rows retrieved")
	}
	resDict, err := res.Get()
	if err != nil { t.Fatal(err.Error()) }

	//Ensure insert id matches
	id, err := ires.LastInsertId()
	if id != resDict["id"].(int64) {
		t.Fatal("Incorrect primary key")
	}

	//Ensure data matches
	for k, _ := range insertQuery.Data {
		if resDict[k] != insertQuery.Data[k] {
			t.Log(resDict[k])
			t.Log(insertQuery.Data[k])
			t.Fatal("Retrieved data does not match inserted data")
		}
	}

	//Make an update
	updateSel := ValueSelection{ Attr: "id", Value: id, Op: "=" }
	updateQuery := UpdateQuery{
		Table: "testTable",
		Data: map[string]interface{}{
			"floatcol": 44.2,
		},
		Selection: updateSel,
	}

	updRes, err := ps.Update(newSchema, make(map[string]RelationPath, 0), updateQuery)
	if err != nil { t.Fatal(err.Error()) }

	//Ensure one row was affected
	updN, err := updRes.RowsAffected()
	if err != nil { t.Fatal(err.Error()) }
	if updN != 1 { t.Fatal("Incorrect number of rows affected: "+strconv.Itoa(int(updN))) }

	//Select the row again
	res, err = ps.Select(newSchema, nil, SelectQuery{
		Table: "testTable",
		Selection: updateSel,
	})
	if err != nil { t.Fatal(err.Error()) }

	if !res.Next(){
		t.Fatal("No rows retrieved")
	}
	resDict, err = res.Get()

	//Ensure data matches
	for k, _ := range updateQuery.Data {
		if resDict[k] != updateQuery.Data[k] {
			t.Log(resDict[k])
			t.Log(updateQuery.Data[k])
			t.Fatal("Retrieved data does not match updated data")
		}
	}
}



//Helper function to test relational filtering etc
// This functionality is normally provided by the engine layer
func RelationalSelect(db AutoscopeDB, schema map[string]Table, stats map[string]TableQueryStats, query SelectQuery) (RetrievalResult, error){
	prefixes, err := genPrefixes(schema, stats, query.Table, query.Selection)
	if err != nil { return nil, err }
	r, err := db.Select(schema, prefixes, query)
	return r, err
}

func TestRelationalFiltering(t *testing.T){
	var ps PostgresDB

	//Connect to the database
	err := ps.Connect(config)
	if err != nil {
		t.Fatal(err.Error())
	}

	//Create test tables
	cols := map[string]string{
		"id": "serial",
		"a": "int",
		"b": "int",
		"autoscope_objectfields": "jsonb",
	}
	rtest_1 := Table{ Name: "rtest_1", Columns: cols, Status: "created"}
	rtest_2 := Table{ Name: "rtest_2", Columns: cols, Status: "created"}
	rtest_3 := Table{ Name: "rtest_3", Columns: cols, Status: "created"}

	currentSchema, err := ps.CurrentSchema()
	if err != nil { t.Fatal(err.Error()) }
	updSchema := map[string]Table{
		"rtest_1": rtest_1,
		"rtest_2": rtest_2,
		"rtest_3": rtest_3,
	}
	steps, err := CreateMigration(config, currentSchema, updSchema)
	if err != nil { t.Fatal(err.Error()) }
	err = ps.PerformMigration(steps)
	if err != nil { t.Fatal(err.Error()) }

	newSchema, err := ps.CurrentSchema()
	if err != nil { t.Fatal(err.Error()) }

	//Insert a row into rtest_1, referencing a -> row of rtest_2
	insertQuery := InsertQuery{
		Table: "rtest_1",
		Data: map[string]interface{}{	"a": "1",	"b": "42"},
	}
	_, err = ps.Insert(newSchema, insertQuery)
	if err != nil { t.Fatal(err.Error()) }

	//Insert a row into rtest_2, referencing b -> row of rtest_3
	insertQuery = InsertQuery{
		Table: "rtest_2",
		Data: map[string]interface{}{"a": "42",	"b": "1", "autoscope_objectfields": "{\"c\": 1}"},
	}
	_, err = ps.Insert(newSchema, insertQuery)
	if err != nil { t.Fatal(err.Error()) }

	//Insert a row into rtest_3
	insertQuery = InsertQuery{
		Table: "rtest_3",
		Data: map[string]interface{}{"a": "42",	"b": "99"},
	}
	_, err = ps.Insert(newSchema, insertQuery)
	if err != nil { t.Fatal(err.Error()) }

	//Generate some stats to link rtest_1.a -> rtest_2 and rtest_2.b -> rtest_3
	// We'll also link rtest_2.c -> an uncreated table rtest_uncreated
	stats := map[string]TableQueryStats{
		"rtest_1": TableQueryStats{
			ForeignKeyCount: map[string]map[string]int64{
				"a": map[string]int64{ "rtest_2": 1, },
			},
		},
		"rtest_2": TableQueryStats{
			ForeignKeyCount: map[string]map[string]int64{
				"b": map[string]int64{ "rtest_3": 1, },
				"c": map[string]int64{ "rtest_uncreated": 1, },

			},
		},
		"rtest_uncreated": TableQueryStats{
			ForeignKeyCount: map[string]map[string]int64{
				"b": map[string]int64{ "rtest_3": 1, },
			},
		},

	}

	//Perform basic relational filtering query,
	// ensuring values are correct
	query := SelectQuery{
		Table: "rtest_1",
		Selection: AttrSelection{AttrA: "a__a", AttrB: "b", Op: "="},
	}

	res, err := RelationalSelect(&ps, newSchema, stats, query)
	if err != nil { t.Fatal(err.Error()) }

	if !res.Next(){
		t.Fatal("No rows retrieved")
	}
	resDict, err := res.Get()
	if err != nil { t.Fatal(err.Error()) }
	if resDict["a"] != int64(1) { t.Fatal("Incorrect relational value retrieved") }
	if resDict["b"] != int64(42) { t.Fatal("Incorrect relational value retrieved") }


	//Perform basic relational filtering query,
	// ensuring values are correct
	query = SelectQuery{
		Table: "rtest_1",
		Selection: AttrSelection{AttrA: "a__b__a", AttrB: "b", Op: "="},
	}

	res, err = RelationalSelect(&ps, newSchema, stats, query)
	if err != nil { t.Fatal(err.Error()) }

	if !res.Next(){
		t.Fatal("No rows retrieved")
	}
	resDict, err = res.Get()
	if err != nil { t.Fatal(err.Error()) }
	if resDict["a"] != int64(1) { t.Fatal("Incorrect relational value retrieved") }
		if resDict["b"] != int64(42) { t.Fatal("Incorrect relational value retrieved") }

	//Insert a row into autoscope_unassigned
	insertQuery = InsertQuery{
		Table: "autoscope_unassigned",
		Data: map[string]interface{}{
			"table_name": "rtest_uncreated",
			"autoscope_objectfields": "{\"dne_a\": 42, \"b\": 1}"},
	}
	_, err = ps.Insert(newSchema, insertQuery)
	if err != nil { t.Fatal(err.Error()) }


	//Perform query referencing table that DNE (rtest_uncreated)
	// and a column that doesn't exist (c) in a table that does
	query = SelectQuery{
		Table: "rtest_2",
		Selection: AttrSelection{AttrA: "c__dne_a", AttrB: "a", Op: "="},
	}

	res, err = RelationalSelect(&ps, newSchema, stats, query)
	if err != nil { t.Fatal(err.Error()) }

	if !res.Next(){
		t.Fatal("No rows retrieved")
	}
	resDict, err = res.Get()
	if err != nil { t.Fatal(err.Error()) }
	if resDict["a"] != int64(42) { t.Fatal("Incorrect relational value retrieved") }
	if resDict["b"] != int64(1) { t.Fatal("Incorrect relational value retrieved") }
	if resDict["c"] != float64(1) { t.Fatal("Incorrect relational value retrieved") }

	//Perform query referencing starting at table that DNE (rtest_uncreated)
	query = SelectQuery{
		Table: "rtest_uncreated",
		Selection: AttrSelection{AttrA: "b__a", AttrB: "dne_a", Op: "="},
	}

	res, err = RelationalSelect(&ps, newSchema, stats, query)
	if err != nil { t.Fatal(err.Error()) }

	if !res.Next(){
		t.Fatal("No rows retrieved")
	}
	resDict, err = res.Get()
	if err != nil { t.Fatal(err.Error()) }
	if resDict["b"] != float64(1) { t.Fatal("Incorrect relational value retrieved") }
	if resDict["dne_a"] != float64(42) { t.Fatal("Incorrect relational value retrieved") }

	//Test updating a column that DNE

	//Make an update
	updateSel := ValueSelection{ Attr: "id", Value: 1, Op: "=" }
	updateQuery := UpdateQuery{
		Table: "rtest_2",
		Data: map[string]interface{}{
			"col_dne": 47,
			"col_dne2": 43,
		},
		Selection: updateSel,
	}
	updRes, err := ps.Update(newSchema, make(map[string]RelationPath, 0), updateQuery)
	if err != nil { t.Fatal(err.Error()) }

	//Ensure one row was affected
	updN, err := updRes.RowsAffected()
	if err != nil { t.Fatal(err.Error()) }
	if updN != 1 { t.Fatal("Incorrect number of rows affected: "+strconv.Itoa(int(updN))) }

	//Select the row again
	res, err = RelationalSelect(&ps, newSchema, stats, SelectQuery{
		Table: "rtest_2",
		Selection: updateSel,
	})
	log.Println(updateSel)
	if err != nil { t.Fatal(err.Error()) }
	if !res.Next(){	t.Fatal("No rows retrieved")}
	resDict, err = res.Get()

	//Ensure data matches
	for k, _ := range updateQuery.Data {
		if resDict[k] != updateQuery.Data[k] {
			if int(resDict[k].(float64)) != updateQuery.Data[k] {
				t.Fatal("Retrieved data does not match updated data")
			}
		}
	}


	//Test updating a field in a table that DNE
}
