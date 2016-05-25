package engine
import (
	"testing"
	"strconv"
	"math/rand"
)

func TestLoadPermissions(t *testing.T){
	_, err := AutoscopePermissions()
	if err != nil { t.Fatal(err.Error()) }
}


//Helper to generate a random permissions object
func randPermissions() Permissions{
	return Permissions{
		Read: rand.Float32() > 0.5,
		Update: rand.Float32() > 0.5,
		Insert: rand.Float32() > 0.5,
	}
}

//Helper function that returns whether a given action should be permitted
func shouldBePermitted(action func(Permissions) bool, perms ObjectPermissions, userInGroup bool, userIsOwner bool) bool {
	if action(perms.Everyone) {
		return true
	} else if action(perms.Owner) && userIsOwner {
		return true
	} else if action(perms.Group) && userInGroup {
		return true
	}
	return false
}

//Strategy: Generate tables with random permissions,
// then test read/update/insert and ensure correct behavior
func TestPermissions(t *testing.T){
	//Create and initialize an engine
	var e Engine
	config := Config{
		DatabaseType: "memdb",
	}
	err := e.Init(&config)
	if err != nil { t.Fatal(err.Error()) }

	//Create user, group, and add them to the group. 
	uid, err := CreateUser(&e, "username", "password")
	if err != nil { t.Fatal(err.Error()) }

	gid, err := CreateGroup(&e, "group_with_user")
	if err != nil { t.Fatal(err.Error()) }

	err = AddUserToGroup(&e, uid, gid)
	if err != nil { t.Fatal(err.Error()) }

	//Create other user and another group
	otherUID, err := CreateUser(&e, "username2", "password")
	if err != nil { t.Fatal(err.Error()) }
	
	otherGID, err := CreateGroup(&e, "group_without_user")
	if err != nil { t.Fatal(err.Error()) }

	err = AddUserToGroup(&e, otherUID, otherGID)
	if err != nil { t.Fatal(err.Error()) }
	
	i := 0
	for i < 100 {
		i += 1
		tableName := "test_table_"+strconv.Itoa(i)

		//Set permissions for table
		perms := ObjectPermissions{
			Owner: randPermissions(),
			Group: randPermissions(),
			Everyone: randPermissions(),
		}
		e.Permissions[tableName] = perms
		t.Log(perms)
		
		//Determine which group to use for the row
		creatorGroupId := gid
		creatorUserId := uid
		
		if rand.Float32() > 0.5 {
			creatorGroupId = otherGID
		}

		if rand.Float32() > 0.5 {
			creatorUserId = otherUID
		}

		//Grant insert privileges on the table to the creator
		// via their membership in their group
		AddTableGroup(&e, tableName, creatorGroupId)
		
		//Insert a random row
		_, err := e.RawInsert(InsertQuery{
			Table: tableName, 
			Data: map[string]interface{}{
				"strcol": "strval",
				"intcol": 42,
				"autoscope_uid": creatorUserId,
				"autoscope_gid": creatorGroupId,
			},
		})
		//rowId, err := res.LastInsertId()

		//Attempt a SELECT
		res, err := e.Select(uid, Filter(tableName, map[string]interface{}{
			"intcol": 42,
		}))
		hasRows := res.Next()
		ideal := shouldBePermitted(ReadAction, perms, creatorGroupId == gid,
			creatorUserId == uid)
		if false == ideal && hasRows   {
			t.Fatal("SELECT allowed despite contrary permissions.")
		}
		if true == ideal && !hasRows  {
			t.Log(err)
			t.Fatal("SELECT not allowed despite permissions.")
		}

		//Attempt an Insert
		_, err = e.Insert(uid, InsertQuery{
			Table: tableName,
			Data: map[string]interface{}{
				"strcol": "strval2",
				"intcol": 43,
				"autoscope_uid": creatorUserId,
				"autoscope_gid": creatorGroupId,
			},
		})
		//Note: There is no owner for an insert, so user is not owner by defn.
		ideal = shouldBePermitted(InsertAction, perms, creatorGroupId == gid,
			false)
		if false == ideal && nil == err  {
			t.Fatal("Insert allowed despite contrary permissions.")
		}
		if true == ideal && nil != err  {
			t.Log(err)
			t.Fatal("Insert not allowed despite permissions.")
		}

		//Attempt an Update
		ures, err := e.Update(uid, Update(
			tableName,
			map[string]interface{}{
				"strcol": "strval",
			}, map[string]interface{}{
				"intcol": 44,
			}))
		rowsAffected, err := ures.RowsAffected()
		if err != nil { t.Fatal(err.Error()) }
		
		ideal = shouldBePermitted(UpdateAction, perms, creatorGroupId == gid,
			creatorUserId == uid)
		if false == ideal && rowsAffected > 0  {
			t.Fatal("UPDATE allowed despite contrary permissions.")
		}
		if true == ideal && rowsAffected == 0  {
			t.Log(rowsAffected)
			t.Fatal("UPDATE not allowed despite permissions.")
		}
	}
}
