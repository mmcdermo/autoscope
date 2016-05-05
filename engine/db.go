package engine

type AutoscopeDB interface {
	Connect(*Config) error
	PerformMigration([]MigrationStep) error
	CurrentSchema() (map[string]Table, error)
	Select(map[string]Table, map[string]RelationPath, SelectQuery) (RetrievalResult, error)
	Insert(map[string]Table, InsertQuery) (ModificationResult, error)
	Update(map[string]Table, map[string]RelationPath, UpdateQuery) (ModificationResult, error)
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
