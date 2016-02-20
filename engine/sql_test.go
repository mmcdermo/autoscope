package engine

import (
	"testing"
	_ "encoding/json"
	"strings"
)

func TestSelectSQLGeneration(t *testing.T){
	valSel := ValueSelection{AttrA:"AttributeA", Op:"=", Value:"42"}
	attrSel := AttrSelection{AttrA:"AttributeA", Op:"=", AttrB:"AttributeB"}
	or := Or{A: valSel, B: attrSel}
	and := And{A: valSel, B: attrSel}
	not := Not{ A: or }
	andornot := And{A: or, B: not}
	query := SelectQuery{Table:"myTable", Selection:or}

	orsql, err := or.toSQL()
	if err != nil {
		t.Fatal("Error: "+err.Error())
	}
	andsql, err := and.toSQL()
	if err != nil {
		t.Fatal("Error: "+err.Error())
	}
	andornotsql, err := andornot.toSQL()
	if err != nil {
		t.Fatal("Error: "+err.Error())
	}

	if orsql.SQL != "(? = ? OR ? = ?)" {
		t.Fatal("Incorrect SQL for Or: "+orsql.SQL)
	}
	if andsql.SQL != "(? = ? AND ? = ?)" {
		t.Fatal("Incorrect SQL for And: "+andsql.SQL)
	}
	if andornotsql.SQL != "((? = ? OR ? = ?) AND NOT ((? = ? OR ? = ?)))" {
		t.Fatal("Incorrect SQL for And: "+andornotsql.SQL)
	}
	if strings.Join(orsql.Args,",") != "AttributeA,42,AttributeA,AttributeB" {
		t.Fatal("Incorrect Args for Or: "+strings.Join(orsql.Args, ","))
	}
	if strings.Join(andsql.Args,",") != "AttributeA,42,AttributeA,AttributeB" {
		t.Fatal("Incorrect Args for And: "+strings.Join(andsql.Args, ","))
	}
}
