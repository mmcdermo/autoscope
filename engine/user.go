package engine

import (
	"golang.org/x/crypto/bcrypt"
	"strconv"
	"time"
	"fmt"
	"math/rand"
)

//Perform a login attempt for user `username`. Returns true on success
// and false on failure. `db` must already be connected.
// On success, creates a new session for user. 
func Login(e *Engine, username string, password string) (bool, error) {
	res, err := e.Select(Filter("autoscope_users",
		map[string]interface{}{"username": username}))
	if err != nil { return false, err }
	user, err := GetRow(res)
	if err != nil { return false, err }

	//CompareHashAndPassword returns nil on success
	salted := password + strconv.FormatInt(user["salt"].(int64), 10)
	hashErr := bcrypt.CompareHashAndPassword([]byte(user["passhash"].(string)),
		[]byte(salted))
	return hashErr == nil, hashErr
}

// Create a new user with given password
func CreateUser(e *Engine, username string, password string) (int64, error) {
	salt := rand.Int31()
	salted := password + strconv.FormatInt(int64(salt), 10)
	passhash, err := bcrypt.GenerateFromPassword([]byte(salted), 10)
	if err != nil { return -1, err }
	res, err := e.Insert(InsertQuery{
		Table: "autoscope_users",
		Data: map[string]interface{}{
			"username": username,
			"passhash": string(passhash),
			"salt": salt,
		},
	})
	insertId, err := res.LastInsertId()
	return insertId, err
}

//Authorize a request for the given user. 
func Authorize(e *Engine, username string, session_id string, expiry_time int64) (bool, error){
	t := time.Now().Unix()
	res, err := e.Select(SelectQuery{
		Table:"autoscope_user_sessions",
		Selection: NestAnds([]Formula{
			ValueSelection{ Attr: "username",
				Value: username, Op: "="},
			ValueSelection{ Attr: "session_id",
				Value: string(session_id), Op: "="},
			ValueSelection{ Attr: "time",
				Value: t - expiry_time, Op: ">"}, 
		}),
	})
	if err != nil { return false, err }
	return res.Next(), nil
}

// Create a new session for user `username`. Returns the session ID.
func CreateSession(e *Engine, username string) (string, error) {
	t := time.Now().Unix()

	session_id, err := bcrypt.GenerateFromPassword([]byte(username + strconv.FormatInt(t, 10) + "8LF5NjBRiL9e1jmOCh53"), 10)
	_, err = e.Insert(InsertQuery{
		Table: "autoscope_user_sessions",
		Data: map[string]interface{}{
			"username": username,
			"time": t,
			"session_id": string(session_id),
		},
	})
	return string(session_id), err
}

//Clear any sessions older than `duration` from the database.
func ClearSessions(db AutoscopeDB, duration int64) error {
	return nil
}
