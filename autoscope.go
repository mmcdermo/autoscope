3package main

import (
	//"database/sql"
	//	"time"
	//	"encoding/json"
	//	"strconv"
	"github.com/mmcdermo/autoscope/server"
)

type Config struct {
	Port string
}

func main(){
	server.RunServer(nil, nil)
}
