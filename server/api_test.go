package server

import (
	"testing"
	"net/http"
	"net/url"
	"time"
	"log"
	"encoding/json"
	"io/ioutil"
	"errors"
	"github.com/mmcdermo/autoscope/engine"
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

type IStrMap map[string]interface{} //Wrapper type for UnmarshalJSON
func (strmap *IStrMap) UnmarshalJSON(b []byte) (err error) {
	_map := make(map[string]interface{})
	err = json.Unmarshal(b, &_map)
	*strmap = _map
	return err
}

var (
	username string
	sessionId string
)

func APICall(api_url string, method string, args map[string]string, obj json.Unmarshaler) error {
	data := url.Values{}
	for k, v := range args { data.Set(k, v) }
	req, err := http.NewRequest(method, api_url, bytes.NewBufferString(data.Encode()))
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))
	req.Header.Add("Content-Type","application/x-www-form-urlencoded")
	if sessionId != "" {
		req.Header.Add("X-User-Id", username)
		req.Header.Add("X-Session-Id", sessionId)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	// Extract the returned data
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if "200 OK" != resp.Status {
		return errors.New("Bad Status: "+resp.Status+"\n\tBody: "+string(body))
	}

	err = obj.UnmarshalJSON([]byte(body))
	if err != nil {
		return errors.New(err.Error()+ " Body: "+string(body))
	}
	return nil
}

func setupSession() (error) {
	username = "test"
	password := "test"
	_, err := engine.CreateUser(&e, username, password)
	if err != nil { return err }
	
	var res StrMap
	err = APICall("http://localhost:4210/api/login/", "POST", map[string]string{
		"username": username,
		"password": password,
	}, &res)
	if err != nil {
		return err
	}
	log.Println("Logion result")
	log.Println(res)

	sessionId = res["session_id"]
	
	return nil
}

func setupPermissions() error {
	perms := engine.ObjectPermissions{
		Owner: engine.Permissions{ Read: true, Update: true, Insert: true, },
		Group: engine.Permissions{ Read: true, Update: true, Insert: true, },
		Everyone: engine.Permissions{ Read: true, Update: true, Insert: true, },
	}
	e.Permissions["choon"] = perms
	return nil
}

func TestMain(m *testing.M){
	go RunServer(&engine.Config{ Port: "4210", DatabaseType: "memdb" }, nil)
	time.Sleep(250 * time.Millisecond)
	err := setupSession()
	if err != nil { log.Fatal(err.Error()) }
	err = setupPermissions()
	if err != nil { log.Fatal(err.Error()) }
	
	m.Run()
}

func TestSelectInsert(t *testing.T){
	var res IStrMap

	data := map[string]interface{}{
		"AttributeA": 42,
		"AttributeB": 42,
	}

	b, err := json.Marshal(data)
	insArgs :=  map[string]string { "data": string(b) }
	err = APICall("http://localhost:4210/api/choon/", "PUT", insArgs, &res)
	if err != nil {
		t.Fatalf("API Insert Error: %v", err)
	}

	//valSel := engine.ValueSelection{Attr:"AttributeA", Op:"=", Value:"42"}
	//or := engine.Or{A: valSel, B: valSel}
	queryStr, err := json.Marshal(engine.Tautology{})
	
	t.Log("Query str: "+string(queryStr))
	if err != nil {
		t.Fatalf("Could not make create query string: %v", err)
	}
	args :=  map[string]string { "selection": string(queryStr),
		"query_type": "SELECT",}
	

	err = APICall("http://localhost:4210/api/choon/", "POST", args, &res)
	if err != nil {
		t.Fatalf("API call failed: %v", err)
	}
	t.Log(res)
	t.Fatal("Hello")
}
