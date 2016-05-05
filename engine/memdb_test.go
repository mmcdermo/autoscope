package engine

import (
	"testing"
)


func TestQueries(t *testing.T){
	var m MemDB
	m.Connect(nil)

	data := make(map[string]interface{}, 0)
	data["intval"] = 44
	data["floatval"] = 44.2
	data["strval"] = "Hampersand"

	//Insert data into memdb
	_, err := m.Insert(nil, InsertQuery{ Data: data, Table: "testTable" })
	if err != nil {
		t.Fatal(err.Error())
	}

	//Retrieve it
	res, err := m.Select(nil, nil, SelectQuery{
		Selection: ValueSelection{
			Attr: "floatval",
			Value: 44.2,
		},
		Table: "testTable",
	})

	//Ensure the RetrievalResult functions correctly
	r, err := res.Get()
	if err == nil {
		t.Fatal("Shouldn't be able to retrieve row without first calling .Next()")
	}
	b := res.Next()
	if b == false {
		t.Fatal("Next() failed")
	}

	//Ensure the data is correct
	r, err = res.Get()
	if err != nil { t.Fatal(err.Error()) }
	for k, v := range data {
		if v != r[k] {
			t.Fatal("Incorrect value for key: "+k)
		}
	}

	//Update the data
	dataUpd := make(map[string]interface{}, 0)
	dataUpd["intval"] = 49
	updateQuery := UpdateQuery{
		Table: "testTable",
		Data: dataUpd,
		Selection: ValueSelection{Attr: "id", Value: r["id"]},
	}

	updRes, err := m.Update(nil, nil, updateQuery)
	if err != nil { t.Fatal(err.Error()) }
	numUpdated, err := updRes.RowsAffected()
	if err != nil { t.Fatal(err.Error()) }
	if numUpdated != 1 { t.Fatal("Row not updated") }

	//Ensure the updated data is correct
	selectQuery := SelectQuery{
		Table: "testTable",
		Selection: ValueSelection{Attr: "id", Value: r["id"]},
	}
	res, err = m.Select(nil, nil, selectQuery)
	if err != nil { t.Fatal(err.Error()) }
	b = res.Next()
	if b == false { t.Fatal("Failed to retrieve row") }
	updated, err := res.Get()
	t.Log(updated)
	if err != nil { t.Fatal(err.Error()) }
	for k, v := range dataUpd {
		if v != updated[k] {
			t.Fatal("Incorrect value for key: "+k)
		}
	}
}
