package engine
import (
	"testing"
	"log"
)

func TestLogin(t *testing.T){
	//Create and initialize a full engine so it has requisite user tables
	var e Engine
	config := Config{
		DatabaseType: "memdb",
	}
	err := e.Init(&config)
	if err != nil { t.Fatal(err.Error()) }

	_, err = CreateUser(&e, "myUser", "password")
	if err != nil { t.Fatal(err.Error()) }

	loggedIn, err := Login(&e, "myUser", "password")
	if err != nil { t.Fatal(err.Error()) }
	
	if loggedIn != true { t.Fatal("Could not log user in") }

	log.Println("User login test complete")
}

func TestSession(t *testing.T){
	var e Engine
	config := Config{
		DatabaseType: "memdb",
	}
	err := e.Init(&config)
	if err != nil { t.Fatal(err.Error()) }

	//Attempt to load garbage session ID should fail
	auth, err := Authorize(&e, "myUser", "garbageSessionID", 300)
	if err != nil { t.Fatal(err.Error()) }
	if auth == true { t.Fatal("Invalid authorization") }

	session_id, err := CreateSession(&e, "myUser")
	if err != nil { t.Fatal(err.Error()) }

	//Negative expiry time should ensure session is no longer valid
	auth, err = Authorize(&e, "myUser", session_id, -1)
	if err != nil { t.Fatal(err.Error()) }
	if auth == true { t.Fatal("Invalid Authorization") }
	
	//5 min expiry time time should ensure session is valid
	auth, err = Authorize(&e, "myUser", session_id, 300)
	if err != nil { t.Fatal(err.Error()) }
	if auth == false { t.Fatal("User should be authorized") }

	log.Println("User session test complete")
}
