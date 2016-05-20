package engine
/* query.go

   Utility functions to easily generate queries as defined in sql.go
*/

//Returns a query on the given table where each column is equal to the given value
// specified in `values`. IE) {id: 42}
func Filter(table string, values map[string]interface{}) SelectQuery {
	restrictions := make([]Formula, 0)
	for col, val := range values {
		//TODO: Add django like annotations (__lt, __gt etc)
		restrictions = append(restrictions, ValueSelection{
			Attr: col,
			Value: val,
			Op: "=",
		})
	}
	query := SelectQuery{
		Table: table,
		Selection: NestAnds(restrictions),
	}
	return query
}
