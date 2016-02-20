package engine

import (
	"database/sql"
	_ "github.com/lib/pq"
	"fmt"
	_ "errors"
)

var (
	conn *sql.DB
	config *Config
)

//Autoscope config struct
type Config struct {
	Port string
	DB_USER string
	DB_HOST string
	DB_NAME string
	DB_PASSWORD string
}

type Result struct {
	Status string `json:"status"`
}
//Function that performs a given SELECT query
func Select(query SelectQuery) (res Result, err error) {
	whereClause, err := query.Selection.toSQL()
	if err != nil {
		return res, err
	}
	fmt.Println(whereClause.SQL)
	res.Status = whereClause.SQL
	return res, nil
}

func DBConnect(_config *Config) error{
	config = _config
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable",
		config.DB_USER, config.DB_PASSWORD, config.DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	if err == nil {
		conn = db
	}
	return err
}
