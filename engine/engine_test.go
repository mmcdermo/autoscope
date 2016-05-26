package engine

import (
	"testing"
	"fmt"
)


//Generate some test stats
func testStats() TableQueryStats {
	restrictions := map[string]int64{
			"someCol": 22,
	}
	ofc := make(map[string]map[string]int64, 0)
	ofc["someCol"] = map[string]int64{ "string": 22 }
	fkc := make(map[string]map[string]int64, 0)
	fkc["someCol"] = map[string]int64{ "someTable": 55 }

	return TableQueryStats{
		InsertQueries: 44,
		SelectQueries: 22,
		UpdateQueries: 33,
		Restrictions: restrictions,
		ObjectFieldCount: ofc,
		ForeignKeyCount: fkc,
	}
}

func cmpStats(ts1 TableQueryStats, ts2 TableQueryStats) bool {
	if ts1.InsertQueries != ts2.InsertQueries { return false }
	if ts1.SelectQueries != ts2.SelectQueries { return false }
	if ts1.UpdateQueries != ts2.UpdateQueries { return false }
	for k, v := range ts2.Restrictions {
		if ts1.Restrictions[k] != v {
			fmt.Println(ts1.Restrictions)
			fmt.Println(ts2.Restrictions)
			return false
		}
	}
	for t, m := range ts2.ObjectFieldCount {
		for k, v := range m {
			if ts1.ObjectFieldCount[t][k] != v {
				fmt.Println(ts1.ObjectFieldCount)
				fmt.Println(ts2.ObjectFieldCount)
				return false
			}
		}
	}
	for t, m := range ts2.ForeignKeyCount {
		for k, v := range m {
			if ts1.ForeignKeyCount[t][k] != v {
				fmt.Println(ts1.ForeignKeyCount)
				fmt.Println(ts2.ForeignKeyCount)
				return false
			}
		}
	}
	return true
}

func TestLoadFlushStats(t *testing.T){
	var e Engine
	config := Config{
		DatabaseType: "memdb",
	}
	err := e.Init(&config)
	if err != nil { t.Fatal(err.Error()) }

	err = e.loadGlobalStats()
	if err != nil { t.Fatal(err.Error()) }

	err = e.flushStatsToDB()
	if err != nil { t.Fatal(err.Error()) }

	//Now, we'll modify some stats, write them out, and see if they're correct
	stats := testStats()
	e.LocalStats["magicTable"] = stats
	err = e.flushStatsToDB()
	if err != nil { t.Fatal(err.Error()) }

	err = e.loadGlobalStats()
	if err != nil { t.Fatal(err.Error()) }

	//Compare our local stats to the global stats
	if !cmpStats(testStats(), e.GlobalStats["magicTable"]) {
		t.Fatal("Loaded stats don't match generated stats")
	}
}

func TestQueryStats(t *testing.T){
	//Ensure that performing queries generates the correct
	// changes in local stats
	var e Engine
	config := Config{
		DatabaseType: "memdb",
	}
	err := e.Init(&config)
	if err != nil { t.Fatal(err.Error()) }
	

	//Create test user, test group, and add that group to
	// table ownership. 
	uid, err := CreateUser(&e, "username", "password")
	gid, err := CreateGroup(&e, "group_with_user")
	if err != nil { t.Fatal(err.Error()) }
	err = AddUserToGroup(&e, uid, gid)
	if err != nil { t.Fatal(err.Error()) }
	AddTableGroup(&e, "test_table0", gid)
	AddTableGroup(&e, "test_table1", gid)
	
	_, err = e.Insert(uid, InsertQuery{
		Table: "test_table0",
		Data: map[string]interface{}{
			"strcol": "strval0",
		},
	})
	if err != nil { t.Fatal(err.Error()) }
	
	_, err = e.Insert(uid, InsertQuery{
		Table: "test_table1",
		Data: map[string]interface{}{
			"strcol": "strval",
			"intcol": 4,
			"fkcol": 0,
		},
		ForeignKeys: map[string]string{
			"fkcol": "test_table0",
		},
	})
	if err != nil { t.Fatal(err.Error()) }

	_, err = e.Update(uid, UpdateQuery{
		Table: "test_table1",
		Selection: ValueSelection{
			Attr: "strcol",
			Op: "=",
			Value: "strval",
		},
		Data: map[string]interface{}{ "intcol": 5 },
	})
	
	res2, err := e.Select(uid, SelectQuery{
		Table: "test_table1",
		Selection: ValueSelection{
			Attr: "strcol",
			Op: "=",
			Value: "strval",
		},
	})
	if err != nil { t.Fatal(err.Error()) }
	res2.Next()
	dict, err := res2.Get()
	if dict["intcol"] != 5 {
		t.Log(dict)
		t.Fatal("Update failed")
	}


	if err != nil { t.Fatal(err.Error()) }


	err = e.flushStatsToDB()
	if err != nil { t.Fatal(err.Error()) }
	err = e.loadGlobalStats()
	if err != nil { t.Fatal(err.Error()) }


	if e.GlobalStats["test_table0"].InsertQueries != 1 {
		t.Fatal("Incorrect stats")
	}
	if e.GlobalStats["test_table1"].InsertQueries != 1 {
		t.Fatal("Incorrect stats")
	}
	if e.GlobalStats["test_table1"].SelectQueries != 1 {
		t.Fatal("Incorrect stats")
	}
	if e.GlobalStats["test_table1"].UpdateQueries != 1 {
		t.Fatal("Incorrect stats")
	}
	if e.GlobalStats["test_table1"].ForeignKeyCount["fkcol"]["test_table0"] != 1 {
		t.Fatal("Incorrect stats")
	}
	if e.GlobalStats["test_table1"].ObjectFieldCount["strcol"]["string"] != 1 {
		t.Fatal("Incorrect stats")
	}
	if e.GlobalStats["test_table1"].ObjectFieldCount["intcol"]["int"] != 2 {
		t.Fatal("Incorrect stats")
	}
}


func TestMigration(t *testing.T){
	var e Engine
	config := Config{
		DatabaseType: "memdb",
		AutoMigrate: false,
		NewTableRowsThreshhold: 2,
		NewFieldThreshhold: 3,
	}
	err := e.Init(&config)
	if err != nil { t.Fatal(err.Error()) }



	//Create test user, test group, and add that group to
	// table ownership. 
	uid, err := CreateUser(&e, "username", "password")
	gid, err := CreateGroup(&e, "group_with_user")
	if err != nil { t.Fatal(err.Error()) }
	err = AddUserToGroup(&e, uid, gid)
	if err != nil { t.Fatal(err.Error()) }
	AddTableGroup(&e, "test_table0", gid)
	AddTableGroup(&e, "test_table1", gid)
	
	_, err = e.Insert(uid, InsertQuery{
		Table: "test_table0",
		Data: map[string]interface{}{"strcol": "strval0",},
	})
	if err != nil { t.Fatal(err.Error()) }

	steps, err := e.MigrationFromStats()
	if err != nil { t.Fatal(err.Error()) }
	if len(steps) > 0 { t.Fatal("Premature migration generation") }

	
	_, err = e.Insert(uid, InsertQuery{
		Table: "test_table0",
		Data: map[string]interface{}{"strcol": "strval1",},
	})
	if err != nil { t.Fatal(err.Error()) }

	//After two inserts, autoscope should be ready to create the table
	steps, err = e.MigrationFromStats()
	if err != nil { t.Fatal(err.Error()) }
	if len(steps) != 1 { t.Fatal("Incorrect number of steps") }
	if steps[0].(MigrationStepCreateTable).tableName != "test_table0" {
		t.Fatal("Incorrect migration")
	}
	
	_, err = e.Insert(uid, InsertQuery{
		Table: "test_table0",
		Data: map[string]interface{}{"strcol": "strval2",},
	})
	if err != nil { t.Fatal(err.Error()) }

	//After three, it should be ready to create the object field, but
	// the table doesn't yet exist so it shouldn't create a migration yet
	steps, err = e.MigrationFromStats()
	if err != nil { t.Fatal(err.Error()) }

	if len(steps) != 1 { t.Fatal("Incorrect number of steps") }
	if steps[0].(MigrationStepCreateTable).tableName != "test_table0" {
		t.Fatal("Incorrect migration")
	}

	//Now we migrate the table creation
	err = e.DB.PerformMigration(steps)
	if err != nil { t.Fatal(err.Error()) }

	//Reload the schema
	err = e.LoadSchema()
	if err != nil { t.Fatal(err.Error()) }
	
	//Now, the objectfield should be ready for creation
	steps, err = e.MigrationFromStats()
	if err != nil { t.Fatal(err.Error()) }
	if len(steps) != 3 {
		t.Fatal("Incorrect number of steps")
	}
	strColM := false
	idColM := false
	asColM := false
	for _, step := range steps {
		if step.(MigrationStepPromoteField).column == "strcol" {
			strColM = true
		}
		if step.(MigrationStepPromoteField).column == "id" {
			idColM = true
		}
		if step.(MigrationStepPromoteField).column == "autoscope_uid" {
			asColM = true
		}
		
	}
	if strColM == false || idColM == false || asColM == false {
		t.Fatal("Missing column migration")
	}

	//Perform the migration
	err = e.DB.PerformMigration(steps)
	if err != nil { t.Fatal(err.Error()) }

	//Reload the schema
	err = e.LoadSchema()
	if err != nil { t.Fatal(err.Error()) }

	//Now, no new migrations should be needed, even after
	// numerous inserts and updates.
	i := 0
	for i < 10 {
		i += 1
		_, err = e.Insert(uid, InsertQuery{
			Table: "test_table0",
			Data: map[string]interface{}{"strcol": "strval1",},
		})
		if err != nil { t.Fatal(err.Error()) }
		_, err = e.Update(uid, UpdateQuery{
			Table: "test_table0",
			Selection: Restrictions(map[string]interface{}{"strcol": "strval1",}),
			Data: map[string]interface{}{"strcol": "strval1",},
		})
		if err != nil { t.Fatal(err.Error()) }
		
	}

	steps, err = e.MigrationFromStats()
	if err != nil { t.Fatal(err.Error()) }
	if len(steps) != 0 {
		t.Log(steps)
		t.Fatal("Incorrect number of steps")
	}

}
