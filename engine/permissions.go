package engine
import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"errors"
)

//  TODO: For efficiency, several optimizations need to be made here.
//   - Caching of logged-in user group membership in the engine
//   - For production mode, generation of UPDATE/SELECT queries that
//     effectively perform user/group permission testing, rather than
//     executing additional queries. This is unsuitable for debug mode,
//     since it would e.g. return 0 results rather than a permission error.

type Permissions struct {
	Read bool
	Update bool
	Insert bool
}


type ObjectPermissions struct {
	Owner Permissions
	Group Permissions
	Everyone Permissions
}

func DefaultPermissions() ObjectPermissions {
	return ObjectPermissions{
		Owner: Permissions{ Read: true, Insert: true, Update: true},
		Group: Permissions{ Read: true, Insert: true },
		Everyone: Permissions{},
	}
}

//Turn a string e.g. "read, write" into Permissions{ Read: true, Update: true, Insert: true}
func PermissionsFromString(perms string) (Permissions, error) {
	parts := strings.Split(strings.ToLower(strings.Replace(perms, " ", "", -1)), ",")
	p := Permissions {
		Read: false,
		Update: false,
		Insert: false,
	}
	for _, part := range parts {
		switch(part){
		case "read":
			p.Read = true
			break
		case "insert":
			p.Insert = true
			break
		case "update":
			p.Update = true
			break
		case "write":
			p.Insert = true
			p.Update = true
			break
		case "none":
			break
		default:
			return p, errors.New("Invalid permission value: "+part)
		}
	}
	return p, nil
}

//Extract autoscope table permissions from autoscope_permissions.yml
func AutoscopePermissions() (map[string]ObjectPermissions, error){
	contents, err := ioutil.ReadFile(os.Getenv("AUTOSCOPE_CONFIG_DIR") + "/autoscope_permissions.yml")
	if err != nil {
		log.Fatal("Failed to read autoscope_permissions.yml")
	}

	var permissions map[string]map[string]map[string]string
	err = yaml.Unmarshal([]byte(contents), &permissions)
	if err != nil {
		log.Fatal("Failed to load yaml from config file: "+err.Error())
	}
	res := make(map[string]ObjectPermissions, 0)
	for k, table := range permissions["permissions"] {
		defaults := map[string]string{
			"owner": "read, write",
			"group": "read",
			"everyone": "none",
		}
		actual := make(map[string]Permissions)
		for entity, defs := range defaults {
			if perms, ok := table[entity]; ok {
				actual[entity], err = PermissionsFromString(perms)
				if err != nil { return nil, err}
			} else {
				actual[entity], err = PermissionsFromString(defs)
				if err != nil { return nil, err}
			}
		}
		res[k] = ObjectPermissions{
			Owner: actual["owner"],
			Group: actual["group"],
			Everyone: actual["everyone"],
		}
	}
	return res, nil
}

//Modify a SELECT or UPDATE to include necessary permissions in query
// - `groups` is a list of groups including the given user
//   NOTE: If access should be denied before querying, the returned bool is false.
func AddPermissionsToSelection(selection Formula, permissions ObjectPermissions, userId int64, groups []int64, action func(Permissions) bool) (Formula, bool) {
	//If everyone is allowed to perform this SELECT or UPDATE, do nothing
	if action(permissions.Everyone){
		return selection, true
	}

	// If no one can perform this action, return false
	var permFormula Formula
	if !action(permissions.Everyone) &&
		!action(permissions.Group) &&
		!action(permissions.Owner) {
		return nil, false
	}
	
	if action(permissions.Group) && len(groups) > 0 {
		groupFormulas := make([]Formula, 0)
		for _, gid := range groups {
			groupFormulas = append(groupFormulas,
				ValueSelection{
					Attr: "autoscope_gid",
					Value: gid,
					Op: "=",
				})
			
		}
		permFormula = NestOrs(groupFormulas)
	}
	if action(permissions.Owner){
		ownerFormula := ValueSelection{
			Attr: "autoscope_uid",
			Value: userId,
			Op: "=",
		}
		if permFormula == nil {
			permFormula = ownerFormula
		} else {
			permFormula = Or{
				A: permFormula,
				B: ownerFormula,
			}
		}
	}

	// If the user can't perform this action (not in any groups
	// while only group members can perform the action, for example)
	// return false. 
	if permFormula == nil {
		return nil, false
	}
	return And{
		A: selection,
		B: permFormula,
	}, true
}


//Generic helper function to test for a given permission.
// `f` is a function that takes a permissions object
// and returns Permission.Read/Write/Insert
// like a lens/functional getter. 
func hasPermission(e *Engine, tableName string, userId int64, rowUID int64, rowGID int64, f func(Permissions) bool) bool {
	perms, ok := e.GetTablePermissions(tableName)
	
	//Default to disallowing all actions in the absence of permissions	
	if !ok { return false }
	
	if f(perms.Everyone) {
		return true
	} else if f(perms.Owner) && userId == rowUID {
		return true
	} else if f(perms.Group) {
		//Test if user is in group.
		b, err := UserInGroup(e, userId, rowGID)
		return b && err == nil
	}
	return false
}
func HasSelectPermissions(e *Engine, tableName string, userId int64, rowUID int64, rowGID int64) bool {
	f := func(p Permissions) bool { return p.Read }
	return hasPermission(e, tableName, userId, rowUID, rowGID, f)
}

func HasUpdatePermissions(e *Engine, tableName string, userId int64, rowUID int64, rowGID int64) bool {
	f := func(p Permissions) bool { return p.Update }
	return hasPermission(e, tableName, userId, rowUID, rowGID, f)
}

// Every table has some number of groups assigned that have insert permissions.
// In the absence of a row-specific gid, the group of a row is considered
// to be the union of all associated groups for its containing table. 
func AddTableGroup(e *Engine, tableName string, groupId int64) error {
	_, err := e.RawInsert(InsertQuery{
		Table: "autoscope_table_groups", 
		Data: map[string]interface{}{
			"table_name": tableName,
			"group_id": groupId,
			},
	})
	return err
}

//TODO: Cache group IDs for each table in memory
func GetTableGroups(e *Engine, tableName string) ([]int64, error){
	res, _, err := e.RawSelect(Filter("autoscope_table_groups", map[string]interface{}{
		"table_name": tableName,
	}))
	if err != nil { return nil, err }

	groups := make([]int64, 0)
	for res.Next() {
		g, err := res.Get()
		if err != nil { return nil, err }
		groups = append(groups, g["group_id"].(int64))
	}
	return groups, err
}

func HasInsertPermissions(e *Engine, tableName string, userId int64) (bool, error) {
	f := func(p Permissions) bool { return p.Insert }

	//If the table doesn't yet exist, we need to check against the default permissions
	perms := DefaultPermissions()
	if perms.Owner.Insert == true {
		return true, nil
	}

	//Test permissions for non-existent group, to efficiently test for Everyone = true
	if hasPermission(e, tableName, userId, -1, -1, f){
		return true, nil
	}

	//Test permissions for each table-group
	groups, err := GetTableGroups(e, tableName)
	if err != nil { return false, err }
	for _, group := range groups {
		if hasPermission(e, tableName, userId, -1, group, f){
			return true, nil
		}
	}
	return false, nil
}


func ReadAction(p Permissions) bool { return p.Read }
func InsertAction(p Permissions) bool { return p.Insert }
func UpdateAction(p Permissions) bool { return p.Update }
