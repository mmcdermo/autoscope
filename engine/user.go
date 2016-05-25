package engine

import (
	"golang.org/x/crypto/bcrypt"
	"strconv"
	"time"
	_ "fmt"
	"math/rand"
)

//Perform a login attempt for user `username`. Returns true on success
// and false on failure. `db` must already be connected.
// On success, creates a new session for user. 
func Login(e *Engine, username string, password string) (bool, error) {
	res, _, err := e.RawSelect(Filter("autoscope_users",
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
	res, err := e.RawInsert(InsertQuery{
		Table: "autoscope_users",
		Data: map[string]interface{}{
			"username": username,
			"passhash": string(passhash),
			"salt": salt,
		},
	})
	if err != nil { return -2, err }
	insertId, err := res.LastInsertId()
	return insertId, err
}

//Authorize a request for the given user. 
func Authorize(e *Engine, username string, session_id string, expiry_time int64) (bool, error){
	t := time.Now().Unix()
	res, _, err := e.RawSelect(Filter("autoscope_user_sessions", map[string]interface{}{
		"username": username,
		"session_id": session_id,
		"time__gt": t - expiry_time,
	}))
	if err != nil { return false, err }
	return res.Next(), nil
}

// Create a new session for user `username`. Returns the session ID.
func CreateSession(e *Engine, username string) (string, error) {
	t := time.Now().Unix()

	session_id, err := bcrypt.GenerateFromPassword([]byte(username + strconv.FormatInt(t, 10) + "8LF5NjBRiL9e1jmOCh53"), 10)
	_, err = e.RawInsert(InsertQuery{
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

//Create a group
func CreateGroup(e *Engine, name string) (int64, error) {
	res, err := e.RawInsert(InsertQuery{
		Table: "autoscope_groups",
		Data: map[string]interface{}{
			"name": name,
		},
	})
	insertId, err := res.LastInsertId()
	return insertId, err
}

//Add a user to an existing group
func AddUserToGroup(e *Engine, userId int64, groupId int64) error {
	_, err := e.RawInsert(InsertQuery{
		Table: "autoscope_user_groups",
		Data: map[string]interface{}{
			"user_id": userId,
			"group_id": groupId,
		},
	})
	return err
}

//Return a list of the IDs of all the groups a user is member of 
func UserGroups(e *Engine, userId int64) ([]int64, error){
	res, _, err := e.RawSelect(Filter("autoscope_user_groups",
			map[string]interface{}{"user_id": userId,},
		))
	if err != nil { return nil, err }
	groups := make([]int64, 0)
	for res.Next() {
		m, err := res.Get()
		if err != nil { return nil, err }
		groups = append(groups, m["group_id"].(int64))
	}
	return groups, nil
}

//Test whether a user is in a given group
// TODO: Cache an in-memory user->groups map for logged-in users
func UserInGroup(e *Engine, userId int64, groupId int64) (bool, error) {
	res, _, err := e.RawSelect(Filter("autoscope_user_groups", map[string]interface{}{
		"user_id": userId,
		"group_id": groupId,
	}))
	if err != nil { return false, err }
	return res.Next(), nil
}
