package engine

import (
	"testing"
	"fmt"
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
