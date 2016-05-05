package engine

import (
	"testing"
)

func TestTableFromConfig(t *testing.T){
	tables, err := AutoscopeTableSchemas()
	if err != nil {
		t.Fatal(err.Error())
	}
	t.Log(tables)
}
