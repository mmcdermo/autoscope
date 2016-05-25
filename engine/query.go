package engine
import (
	"strings"
)
/* query.go

   Utility functions to easily generate queries as defined in sql.go
*/

//Separates comparison annotations from column names.
// e.g. transforms) "time__gt" into ("time", ">")
func CmpAnnotations(columnName string) (string, string){
	opMap := map[string]string {
		"gt": ">",
		"eq": "=",
		"lt": "<",
		"gte": ">=",
		"lte": "<=",
		"neq": "!=",
	}
	arr := strings.Split(columnName, "__")
	if len(arr) == 0 {
		return columnName, "="
	}
	if op, ok := opMap[arr[len(arr)-1]]; !ok {
		return columnName, "="		
	} else {
		return strings.Join(arr[0:len(arr)-1], "__"), op
	}
}

func Restrictions(values map[string]interface{}) Formula {
	restrictions := make([]Formula, 0)
	for col, val := range values {
		realCol, op := CmpAnnotations(col)
		restrictions = append(restrictions, ValueSelection{
			Attr: realCol,
			Value: val,
			Op: op,
		})
	}
	return NestAnds(restrictions)
}

//Returns a query to update rows in which each column is equal to the given value
// specified in `values`. IE) {id: 42}
func Update(table string, values map[string]interface{}, data map[string]interface{}) UpdateQuery {
	return UpdateQuery{
		Table: table,
		Selection: Restrictions(values),
		Data: data,
	}
}

//Returns a query on the given table where each column is equal to the given value
// specified in `values`. IE) {id: 42}
func Filter(table string, values map[string]interface{}) SelectQuery {
	return SelectQuery{
		Table: table,
		Selection: Restrictions(values),
	}
}
