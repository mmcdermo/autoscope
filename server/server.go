package server

import (
	//"database/sql"
	//	"time"
	//	"strconv"

	"encoding/json"
	"errors"
	"log"
	"fmt"
	"net/http"
	"github.com/gorilla/mux"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strconv"
	engine "github.com/mmcdermo/autoscope/engine"
)

var (
	e engine.Engine
)

func report_api_error(w http.ResponseWriter, err error, user_error string){
	w.Header().Set("Content-Type", "text/json")
	http.Error(w, "{\"error\": \"" + user_error + "--" + err.Error() + "\"}", 200)
	log.Printf(err.Error())
}

func InsertHandler(uid int64, w http.ResponseWriter, r *http.Request){
	vars := mux.Vars(r)
	obj, ok := vars["object"]
	if !ok {
		report_api_error(w, errors.New("No object provided"), "No object provided")
		return
	}

	queryStr := r.FormValue("data")
	var mapA map[string]interface{}
	err := json.Unmarshal([]byte(queryStr), &mapA)
	if err != nil {
		report_api_error(w, err, "Unable to parse data "+string(queryStr))
		return
	}

	fmt.Println("INSERT HANDLER "+obj)
	fmt.Println(queryStr)
	fmt.Println(mapA)
	res, err = e.Insert(uid, engine.InsertQuery{
		Table: obj,
		Data: mapA,
	})
	if err != nil {
		report_api_error(w, err, "Error performing query")
		return
	}

	lii, err := res.LastInsertId()
	if err != nil {
		report_api_error(w, err, "Error performing query")
		return
	}
	
	z, err := json.Marshal(map[string]interface{}{"status": "success",
		"inserted_id": lii,
	})
	fmt.Fprintf(w, "%s", z)
}

func UpdateHandler(uid int64, w http.ResponseWriter, r *http.Request){
	/*queryStr := r.FormValue("query")
	paramStr := r.FormValue("query")
	vars := mux.Vars(r)

	obj, ok := vars["object"]
	if !ok {
		report_api_error(w, errors.New("No object provided"), "No object provided")
		return
	}
*/
	//...
}

func DeleteHandler(uid int64, w http.ResponseWriter, r *http.Request){
	vars := mux.Vars(r)
	_, ok := vars["object"]
	if !ok {
		report_api_error(w, errors.New("No object provided"), "No object provided")
		return
	}

}

func SelectHandler(uid int64, w http.ResponseWriter, r *http.Request){
	vars := mux.Vars(r)
	obj, ok := vars["object"]

	if !ok {
		report_api_error(w, errors.New("No object provided"), "No object provided")
		return
	}

	selectionStr := r.FormValue("selection")
	fmt.Println(selectionStr)	
	sq := engine.SelectQuery{ Table: obj }


	var err error
	sq.Selection, err = engine.FormulaFromJSON([]byte(selectionStr))
	if err != nil {
		report_api_error(w, err, "Unable to parse query object "+selectionStr)
		return
	}
	res, _, err := e.RawSelect(sq)
	if err != nil {
		log.Println(err)
		report_api_error(w, err, "SELECT Query Error")
		return
	}

	rows := make([]map[string]interface{}, 0)
	for res.Next() {
		m, err := res.Get()
		if err != nil {
			fmt.Println(err)
			report_api_error(w, err, "Result Query Error")
			return
		}
		rows = append(rows, m)
	}
	fmt.Println(rows)
	fmt.Println("^^ rows")
	result := map[string]interface{}{
		"rows": rows,
	}
	b, err := json.Marshal(result)
	if err != nil {
		report_api_error(w, err, "Result Query Error")
		return
	}
	fmt.Fprintf(w, "%s", b)
}

func RESTHandler(w http.ResponseWriter, r *http.Request){
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	var maxSessionLength int64 // (seconds)
	maxSessionLength = 60 * 60
	
	uids, err := engine.RequireAuth(&e, r, maxSessionLength)
	if err != nil {
		report_api_error(w, err, "User not logged in or session expired.")
		return
	}
	uid, err := strconv.ParseInt(uids, 10, 64)
	if err != nil {
		report_api_error(w, err, "Invalid User ID")
		return
	}
	
	if r.Method == "POST" {
		queryType := r.FormValue("query_type")
		if "SELECT" == queryType {
			SelectHandler(uid, w, r)
		} else if "UPDATE" == queryType {
			UpdateHandler(uid, w, r)
		}
	} else if r.Method == "PUT" {
		InsertHandler(uid, w, r)
	} else if r.Method == "DELETE" {
		DeleteHandler(uid, w, r)
	} else {
		//Assume GET as per http documentation
		SelectHandler(uid, w, r)
		//TODO: Make SelectHandler accept args for REST
	}
}

func LoginHandler(w http.ResponseWriter, r *http.Request){
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	username := r.FormValue("username")
	password := r.FormValue("password")

	sessionId, err := engine.LoginAttempt(&e, username, password)
	if err != nil {
		report_api_error(w, err, "Error with login")
		return
	}
	
	s,_ := json.Marshal(map[string]string{"session_id": sessionId})
	fmt.Fprintf(w, "%s", s)
}

func RunHTTPServer(port string, router *mux.Router) error{
	var r *mux.Router
	if router == nil {
		r = mux.NewRouter()
	} else {
		r = router
	}

	r.HandleFunc("/api/login/", LoginHandler)
	r.HandleFunc("/api/{object}/", RESTHandler)
	http.Handle("/", r)
	http.ListenAndServe(":"+port, nil)
	return nil
}

func InitEngine(defaultConfig *engine.Config) (*engine.Engine, error){
	config := engine.Config{}
	if defaultConfig != nil {
		log.Println("Using provided config object.")
		config = *defaultConfig
	} else {
		log.Println("Loading config file.")
		
		contents, err := ioutil.ReadFile("autoscope.yml")
		if err != nil {
			log.Fatal("Failed to read config file.")
		}

		err = yaml.Unmarshal([]byte(contents), &config)
		if err != nil {
			log.Fatal("Failed to load yaml from config file.")
		}
	}
	log.Println("Loaded config file. Port is: "+config.Port)
	
	err := e.Init(&config)
	if err != nil {
		return nil, err
	}
	return &e, nil

}
func RunServer (defaultConfig *engine.Config, router *mux.Router){
	_, err := InitEngine(defaultConfig)
	if err != nil {
		log.Fatalf("Engine initialization error: %v", err)
	}
	err = RunHTTPServer(defaultConfig.Port, router)
	if err != nil {
		log.Fatalf("HTTP Server error: %v", err)
	}
}
