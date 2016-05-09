package engine

import (
	"sync"
	"errors"
	"fmt"
	_ "strconv"
)

/* MemDB provides a simple, thread-safe in-memory database
   At present, it is best suited for testing purposes only.

   Current performance issues:
   - No indexing
*/

type MemDB struct {
	Tables map[string]*MemTable
	Config *Config
	TableLock sync.RWMutex
}

//Type representing a single row
type MemRow map[string]interface{}

//Type representing a table
type MemTable struct {
	//Stored type of each column
	Columns map[string]string
	//Map from primary key -> row
	Rows map[int64]MemRow
	//Last index used
	LastIndex int64
	//Table level lock
	Lock sync.RWMutex
}

func (memDB *MemDB) Connect(config *Config) error {
	memDB.Config = config
	memDB.Tables = make(map[string]*MemTable, 0)
	fmt.Println("MemDB Initialized")
	return nil
}

func (memDB *MemDB) CurrentSchema() (map[string]Table, error) {
	tables := make(map[string]Table, 0)
	for tableName, table := range memDB.Tables {
		tables[tableName] = Table{
			Name: tableName,
			Columns: table.Columns,
			Status: "created",
		}
	}
	return tables, nil
}

func (memDB *MemDB) PerformMigration(steps []MigrationStep) error {
	for _, step := range steps {
		switch val := step.(type){
		case MigrationStepCreateTable:
			err := memDB.MigrationCreateTable(val)
			if err != nil { return err }
		case MigrationStepPromoteField:
			// All fields stored in the same way
			return nil
		case MigrationStepIndexColumn:
			// Indexing not yet supported
			return nil
		default:
			return errors.New("memDB: Unknown migration step type")
		}
	}
	return nil
}

func (memDB *MemDB) MigrationCreateTable(ct MigrationStepCreateTable) error {
	if _, ok := memDB.Tables[ct.tableName]; ok {
		return errors.New("memDB: Table already exists")
	}
	memDB.Tables[ct.tableName] = &MemTable{
		Columns: ct.table.Columns,
		Rows: make(map[int64]MemRow, 0),
		LastIndex: 0,
	}
	return nil
}

type MemDBRetrievalResult struct {
	Table Table
	Rows []MemRow
	CurrentIndex int
}

func (res *MemDBRetrievalResult) Next() bool {
	res.CurrentIndex += 1
	if res.CurrentIndex >= len(res.Rows) {
		return false
	}
	return true
}

func (res *MemDBRetrievalResult) Get() (map[string]interface{}, error) {
	if res.CurrentIndex >= len(res.Rows) {
		return nil, errors.New("memDB: No more rows to retrieve")
	}
	if res.CurrentIndex < 0 {
		return nil, errors.New("memDB: You must call .Next() before calling .Get()")
	}
	return res.Rows[res.CurrentIndex], nil
}

type MemDBModificationResult struct {
	id int64
	rowsAffected int64
}

func (res MemDBModificationResult) LastInsertId() (int64, error){
	return res.id, nil
}

func (res MemDBModificationResult) RowsAffected() (int64, error){
	return res.rowsAffected, nil
}

//Recursively evaluate a restriction formula for a given row
//TODO: Function correctly for relational queries (venue__owner)
func (memDB *MemDB) evalFormula(prefixes map[string]RelationPath, row MemRow, formula Formula) bool {
	switch formula.(type){
	case AttrSelection:
		if attrA, ok := row[formula.(AttrSelection).AttrA]; ok {
			if attrB, ok := row[formula.(AttrSelection).AttrB]; ok {
				if attrA == attrB {
					return true
				}
			}
		}
		return false
	case ValueSelection:
		if attr, ok := row[formula.(ValueSelection).Attr]; ok {
			if attr == formula.(ValueSelection).Value {
				return true
			}
		}
		return false
	case Or:
		return memDB.evalFormula(prefixes, row, formula.(Or).A) || memDB.evalFormula(prefixes, row, formula.(Or).B)
	case And:
		return memDB.evalFormula(prefixes, row, formula.(And).A) && memDB.evalFormula(prefixes, row, formula.(And).B)
	case Not:
		return !memDB.evalFormula(prefixes, row, formula.(Not).A)
	}
	return false
}

//Select a row from the memDB.
// For now, we will just perform a linear scan on the table
func (memDB *MemDB) Select(schema map[string]Table, prefixes map[string]RelationPath, query SelectQuery) (RetrievalResult, error) {
	r := MemDBRetrievalResult{
		CurrentIndex: -1,
		Rows: make([]MemRow, 0),
	}
	memDB.TableLock.RLock()
	defer memDB.TableLock.RUnlock()
	if _, ok := memDB.Tables[query.Table]; !ok {
		return nil, errors.New("memDB: Table does not exist")
	}

	t := memDB.Tables[query.Table]
	t.Lock.Lock()
	defer t.Lock.Unlock()

	for _, row := range memDB.Tables[query.Table].Rows {
		if  query.Selection == nil || memDB.evalFormula(prefixes, row, query.Selection){
			r.Rows = append(r.Rows, row)
		}
	}
	return &r, nil
}

func (memDB *MemDB) Insert(schema map[string]Table, query InsertQuery) (ModificationResult, error) {
	var r MemDBModificationResult

	memDB.TableLock.Lock()
	defer memDB.TableLock.Unlock()
	//Create the table if it doesn't exist
	// This will make the output of .CurrentSchema() different from
	// other backends, but it will massively simplify everything internally
	if _, ok := memDB.Tables[query.Table]; !ok {
		memDB.Tables[query.Table] = &MemTable{
			Columns: make(map[string]string, 0),
			Rows: make(map[int64]MemRow, 0),
			LastIndex: 0,
		}
	}


	table := memDB.Tables[query.Table]
	r.id = table.LastIndex
	r.rowsAffected = 1
	query.Data["id"] = table.LastIndex

	table.Lock.Lock()
	table.LastIndex += 1
	table.Rows[table.LastIndex] = make(map[string]interface{})
	for k, v := range query.Data {
		table.Rows[table.LastIndex][k] = v
		table.Rows[table.LastIndex][k] = v
	}
	table.Lock.Unlock()
	return r, nil
}

func (memDB *MemDB) Update(schema map[string]Table, prefixes map[string]RelationPath, query UpdateQuery) (ModificationResult, error) {
	r := MemDBModificationResult{
		id: -1,
		rowsAffected: 0,
	}
	if _, ok := memDB.Tables[query.Table]; !ok {
		return nil, errors.New("memDB: Tables does not exist")
	}

	for pk, row := range memDB.Tables[query.Table].Rows {
		if memDB.evalFormula(prefixes, row, query.Selection){
			r.rowsAffected += 1
			for k, v := range query.Data {
				row[k] = v
			}
			memDB.Tables[query.Table].Rows[pk] = row
		}
	}
	return r, nil
}
