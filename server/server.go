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
	"github.com/gorilla/handlers"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strconv"
	engine "github.com/mmcdermo/autoscope/engine"
)

var (
	e engine.Engine
)

func report_api_error_code(w http.ResponseWriter, err error, user_error string, code int){
	w.Header().Set("Content-Type", "text/json")
	http.Error(w, "{\"error\": \"" + user_error + "--" + err.Error() + "\"}", code)
	log.Printf(err.Error())
}

func report_api_error(w http.ResponseWriter, err error, user_error string){
	report_api_error_code(w, err, user_error, 400);
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

	res, err := e.Insert(uid, engine.InsertQuery{
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
	w.WriteHeader(http.StatusOK)	
	fmt.Fprintf(w, "%s", z)
}

func UpdateHandler(uid int64, w http.ResponseWriter, r *http.Request){
	vars := mux.Vars(r)

	//Get the object/table name
	obj, ok := vars["object"]
	if !ok {
		report_api_error(w, errors.New("No object provided"), "No object provided")
		return
	}

	//Extract data from POST parameters
	queryStr := r.FormValue("data")
	var data map[string]interface{}
	err := json.Unmarshal([]byte(queryStr), &data)
	if err != nil {
		report_api_error(w, err, "Unable to parse data "+string(queryStr))
		return
	}

	//Extract selection parameters
	selectionStr := r.FormValue("selection")
	fmt.Println(selectionStr)	
	uq := engine.UpdateQuery{ Table: obj, Data: data }
	uq.Selection, err = engine.FormulaFromJSON([]byte(selectionStr))
	if err != nil {
		report_api_error(w, err, "Unable to parse query object "+selectionStr)
		return
	}

	//Perform query
	res, err := e.Update(uid, uq)
	if err != nil {
		log.Println(err)
		report_api_error(w, err, "UPDATE Query Error")
		return
	}

	//Output result
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		report_api_error(w, err, "Error performing query")
		return
	}
	
	z, err := json.Marshal(map[string]interface{}{"status": "success",
		"rows_affected": rowsAffected,
	})
	w.WriteHeader(http.StatusOK)	
	fmt.Fprintf(w, "%s", z)
}

func DeleteHandler(uid int64, w http.ResponseWriter, r *http.Request){
	vars := mux.Vars(r)
	obj, ok := vars["object"]

	if !ok {
		report_api_error(w, errors.New("No object provided"), "No object provided")
		return
	}

	selectionStr := r.FormValue("selection")
	sq := engine.SelectQuery{ Table: obj }

	var err error
	sq.Selection, err = engine.FormulaFromJSON([]byte(selectionStr))
	if err != nil {
		report_api_error(w, err, "Unable to parse query object "+selectionStr)
		return
	}
	res, err := e.Delete(uid, sq)
	if err != nil {
		log.Println(err)
		report_api_error(w, err, "DELETE Query Error")
		return
	}

	result := map[string]interface{}{
		"rows_affected": res.RowsAffected,
	}
	b, err := json.Marshal(result)
	if err != nil {
		report_api_error(w, err, "Result Query Error")
		return
	}
	w.WriteHeader(http.StatusOK)	
	fmt.Fprintf(w, "%s", b)
}


func SelectHandler(uid int64, w http.ResponseWriter, r *http.Request){
	vars := mux.Vars(r)
	obj, ok := vars["object"]

	if !ok {
		report_api_error(w, errors.New("No object provided"), "No object provided")
		return
	}

	selectionStr := r.FormValue("selection")
	sq := engine.SelectQuery{ Table: obj }


	var err error
	sq.Selection, err = engine.FormulaFromJSON([]byte(selectionStr))
	if err != nil {
		report_api_error(w, err, "Unable to parse query object "+selectionStr)
		return
	}
	res, err := e.Select(uid, sq)
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

	result := map[string]interface{}{
		"rows": rows,
	}
	b, err := json.Marshal(result)
	if err != nil {
		report_api_error(w, err, "Result Query Error")
		return
	}
	w.WriteHeader(http.StatusOK)	
	fmt.Fprintf(w, "%s", b)
}

func RESTHandler(w http.ResponseWriter, r *http.Request){
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	var maxSessionLength int64 // (seconds)
	maxSessionLength = 60 * 60
	
	uids, err := engine.RequireAuth(&e, r, maxSessionLength)
	if err != nil {
		report_api_error_code(w, err, "User not logged in or session expired.", 403)
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

	username := r.FormValue("username")
	password := r.FormValue("password")

	sessionId, err := engine.LoginAttempt(&e, username, password)
	if err != nil {
		report_api_error_code(w, err, "Error with login", 401)
		return
	}
	
	s,_ := json.Marshal(map[string]string{"session_id": sessionId})
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s", s)
}

func SchemaHandler(w http.ResponseWriter, r *http.Request){
	var maxSessionLength int64 // (seconds)
	maxSessionLength = 60 * 60

	_, err := engine.RequireAuth(&e, r, maxSessionLength)
	if err != nil {
		report_api_error_code(w, err, "User not logged in or session expired.", 403)
		return
	}

	if &e == nil {
		report_api_error_code(w, err, "No engine configured.", 500)
		return
	}
	
	//TODO: SECURITY
	schema := e.Schema
	s,_ := json.Marshal(schema)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s", s)
}

func StatsHandler(w http.ResponseWriter, r *http.Request){
	var maxSessionLength int64 // (seconds)
	maxSessionLength = 60 * 60

	_, err := engine.RequireAuth(&e, r, maxSessionLength)
	if err != nil {
		report_api_error_code(w, err, "User not logged in or session expired.", 403)
		return
	}

	if &e == nil {
		report_api_error_code(w, err, "No engine configured.", 500)
		return
	}

	//TODO: SECURITY
	stats := e.GlobalStats
	s,_ := json.Marshal(stats)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "%s", s)
}

func RunHTTPServer(port string, router *mux.Router) error{
	var r *mux.Router
	if router == nil {
		r = mux.NewRouter()
	} else {
		r = router
	}

	r.HandleFunc("/asapi/schema/", SchemaHandler)
	r.HandleFunc("/asapi/stats/", StatsHandler)
	r.HandleFunc("/asapi/login/", LoginHandler)
	r.HandleFunc("/api/{object}/", RESTHandler)
	//http.Handle("/", r)
	log.Println("Running Autoscope HTTP API on port " + port)
	http.ListenAndServe(":"+port, handlers.CORS()(r))
	//http.ListenAndServe(":"+port, nil)
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
