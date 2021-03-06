package engine

import (
	"errors"
)

type AutoscopeDB interface {
	Connect(*Config) error
	PerformMigration([]MigrationStep) error
	CurrentSchema() (map[string]Table, error)
	Delete(map[string]Table, map[string]RelationPath, SelectQuery) (ModificationResult, error)
	Update(map[string]Table, map[string]RelationPath, UpdateQuery) (ModificationResult, error)
	Select(map[string]Table, map[string]RelationPath, SelectQuery) (RetrievalResult, error)
	Insert(map[string]Table, InsertQuery) (ModificationResult, error)
	/*pseudoJoinWhere(map[string]Table, Formula) (bool, error)*/
}

type RetrievalResult interface {
	Next() bool
	Get() (map[string]interface{}, error)
}

type ModificationResult interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

//Attempts to retrieve a single row from result, and fails if it cannot
func GetRow (res RetrievalResult) (map[string]interface{}, error) {
	if res.Next() == false { return nil, errors.New("No rows to retrieve")}
	return res.Get()
}

type EmptyModificationResult struct {
}
func (r EmptyModificationResult) LastInsertId() (int64, error){
	return -1, nil
}
func (r EmptyModificationResult) RowsAffected() (int64, error){
	return 0, nil
}

type EmptyRetrievalResult struct {
}
func (r EmptyRetrievalResult) Next() (bool){
	return false
}
func (r EmptyRetrievalResult) Get() (map[string]interface{}, error){
	return nil, nil
}
