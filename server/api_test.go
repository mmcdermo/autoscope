package server

import (
	"testing"
	"net/http"
	"net/url"
	"time"
	"encoding/json"
	"io/ioutil"
	"errors"
	"autoscope/engine"
	"strconv"
	"bytes"
)

type StrMap map[string]string //Wrapper type for UnmarshalJSON
func (strmap *StrMap) UnmarshalJSON(b []byte) (err error) {
	_map := make(map[string]string)
	err = json.Unmarshal(b, &_map)
	*strmap = _map
	return err
}

func APICall(api_url string, method string, args map[string]string, obj json.Unmarshaler) error {
	data := url.Values{}
	for k, v := range args { data.Set(k, v) }
	req, err := http.NewRequest(method, api_url, bytes.NewBufferString(data.Encode()))
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))
	req.Header.Add("Content-Type","application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	// Extract the returned data
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
    if err != nil { return err }

	if "200 OK" != resp.Status {
		return errors.New("Bad Status: "+resp.Status+"\n\tBody: "+string(body))
	}

	err = obj.UnmarshalJSON([]byte(body))
	if err != nil { return errors.New(err.Error()+ " Body: "+string(body))}
	return nil
}

func TestMain(m *testing.M){
	go RunServer(nil)
	time.Sleep(250 * time.Millisecond)
	m.Run()
}

func TestOne(t *testing.T){
	var res StrMap

	valSel := engine.ValueSelection{AttrA:"AttributeA", Op:"=", Value:"42"}
	or := engine.Or{A: valSel, B: valSel}
	query := engine.SelectQuery{Table:"myTable", Selection:or}

	queryStr, err := json.Marshal(query)
	t.Log("Query str: "+string(queryStr))
	if err != nil {
		t.Fatalf("Could not make create query string: %v", err)
	}
	args :=  map[string]string { "query": string(queryStr),
		"query_type": "SELECT", }

	err = APICall("http://localhost:4210/api/choon/", "POST", args, &res)
	if err != nil {
		t.Fatalf("API call failed: %v", err)
	}
	t.Log(res)
	t.Fatal("Hello")
}
