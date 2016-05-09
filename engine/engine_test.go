package engine

import (
	"testing"
)

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

	restrictions := map[string]int64{
			"someCol": 22,
	}
	ofc := make(map[string]map[string]int64, 0)
	ofc["someCol"] = map[string]int64{ "string": 22 }
	fkc := make(map[string]map[string]int64, 0)
	fkc["someCol"] = map[string]int64{ "someTable": 55 }

	//Now, we'll modify some stats, write them out, and see if they're correct
	e.LocalStats["magicTable"] = TableQueryStats{
		InsertQueries: 44,
		SelectQueries: 22,
		UpdateQueries: 33,
		Restrictions: restrictions,
		ObjectFieldCount: ofc,
		ForeignKeyCount: fkc,
	}
	err = e.flushStatsToDB()
	if err != nil { t.Fatal(err.Error()) }
	t.Log("Test")

	err = e.loadGlobalStats()
	if err != nil { t.Fatal(err.Error()) }

}
