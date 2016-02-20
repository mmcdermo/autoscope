package server

import (
	//"database/sql"
	//	"time"
	//	"strconv"
	"encoding/json"
	"log"
	"fmt"
	"net/http"
	"github.com/gorilla/mux"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"autoscope/engine"
)

func report_api_error(w http.ResponseWriter, err error, user_error string){
	w.Header().Set("Content-Type", "text/json")
	http.Error(w, "{\"error\": \"" + user_error + "--" + err.Error() + "\"}", 200)
	log.Printf(err.Error())
}

func InsertHandler(w http.ResponseWriter, r *http.Request){

}
func DeleteHandler(w http.ResponseWriter, r *http.Request){

}

func SelectHandler(w http.ResponseWriter, r *http.Request){
	queryStr := r.FormValue("query")

	var mapA map[string]interface{}
	err := json.Unmarshal([]byte(queryStr), &mapA)

	str, err := json.Marshal(mapA)

	var sq engine.SelectQuery
	err = engine.UnmarshalSelectQuery(&sq, []byte(queryStr))


	if err != nil {
		report_api_error(w, err, "Unable to parse query object "+string(str))
		return
	}
	res, err := engine.Select(sq)
	fmt.Fprintf(w, "%s", res.Status)
}

func RESTHandler(w http.ResponseWriter, r *http.Request){
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	if r.Method == "POST" {
		queryType := r.FormValue("query_type")
		if "SELECT" == queryType {
			SelectHandler(w, r)
		}
	} else if r.Method == "PUT" {
		InsertHandler(w, r)
	}	else if r.Method == "DELETE" {
		DeleteHandler(w, r)
	} else {
		//Assume GET as per http documentation
	}
}

func RunHTTPServer(port string) error{
	r := mux.NewRouter()
	r.HandleFunc("/api/{object}/", RESTHandler)
	http.Handle("/", r)
	http.ListenAndServe(":"+port, nil)
	return nil
}

func RunServer (defaultConfig *engine.Config) {
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


	err := engine.DBConnect(&config)
	if err != nil {
		log.Fatalf("Database Connection Error: %v", err)
	}

	err = RunHTTPServer(config.Port)

	if err != nil {
		log.Fatalf("HTTP Server error: %v", err)
	}
}
