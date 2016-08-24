package engine

import (
	"net/http"
	"errors"
	"strconv"
	_ "log"
)

//Perform a login request for a user
// Upon success, a sessionId will be returned and err will be nil
func LoginAttempt(a *Engine, username string, password string) (sessionId string, err error){
	success, err := Login(a, username, password)
	if true == success && nil == err {
		return CreateSession(a, username)
	} else if err != nil {
		return "", err
	} else { //!success
		return "", errors.New("Unknown username password combination.")
	}
}

//Require authorization for a given request.
// Upon success, userId will be returned and err will be nil
func RequireAuth(e *Engine, r *http.Request, maxSessionDuration int64) (userId string, err error){
	var username, sessionId string
	if vals, ok := r.Header["X-User-Id"]; !ok {
		return "", errors.New("No User-Id header present")
	} else {
		if len(vals) != 1 { return "", errors.New("Incorrect num. User-Id headers") }
		username = vals[0]
	}

	if vals, ok := r.Header["X-Session-Id"]; !ok {
		return "", errors.New("No Session-Id header present")
	} else {
		if len(vals) != 1 { return "", errors.New("Incorrect num. Session-Id headers") }
		sessionId = vals[0]
	}
	authed, err := Authorize(e, username, sessionId, maxSessionDuration)
	if true == authed && nil == err {
		userId, err := GetUserId(e, username)
		if err != nil { return "", err }
		return strconv.FormatInt(userId, 10), nil
	} else if err != nil {
		return "", err
	} else { //authed == false
		return "", errors.New("No valid sessions for user. Please login again.")
	}
}

//Test if a user is a member of the given group
func UserInGroupStr(e *Engine, userId string, groupName string) (bool, error){
	var uid, gid int64
	uid, err := strconv.ParseInt(userId, 10, 64)
	if err != nil { return false, err }

	gid, err = GetGroupId(e, groupName)
	if err != nil { return false, err }
	
	return UserInGroup(e, uid, gid)
}
