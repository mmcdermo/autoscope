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
}
