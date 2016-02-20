package engine

import (
	"errors"
	"encoding/json"
	"strings"
)
/*IDEA DUMP:
 - How to deal with many-to-one access (event__venue__owner__name = "Charles")
   * Take our Formula and extract out attribute names
   * Extract referenced table chains
   * Incorporate these as LEFT JOINs preceding the WHERE clause
 - How to deal with many-to-many access
   * Allow insertions of arrays of objects
   * insert({type: "band", name: "Charlies", artists: [obj1, pk, ...]})
   * Create associated table and through tables immediately
   * Because we permit instantaneous schema modification,
     additional values can be added to through tables at any time.
 - This also permits instantaneous creation & connection of related objects:
   * insert({type: "painting", name: "Charlies", artist: {name: "Charles", ...}})
*/


//Simple structure that contains an SQL (sub-)query and its
// arguments to be replaced.
// e.g. s := SQLPart{SQL="? = ?", Args={"Charles", "Charlie"}}
type SQLPart struct {
	SQL string
	Args []string
}

//Structure representing the current official database schema
type SchemaInfo struct {

}

//Since we don't have Sum-types, we'll use an interface
// to distinguish structures that can represent formulas.
type Formula interface {
	toSQL() (SQLPart, error)
	validateSemantics(*SchemaInfo) bool
}

//Function to determine if a given string is a valid SQL binary operation
func ValidOp(s string) bool {
	validOps := []string{"<", "<=", "=", "!=", ">=", ">"}
	for _, op := range validOps {
		if s == op {
			return true
		}
	}
	return false
}

// Helper function to extract argument lists from a JSON chunk
func extractArgs(b []byte) (args []json.RawMessage, err error){
	m := make(map[string]json.RawMessage)
	err = json.Unmarshal(b, &m)
	if err != nil { return args, err }

	//Ensure args key exists
	_, ok := m["args"]
	if false == ok { return args, errors.New("No `args` key found") }

	//Extract args
	err = json.Unmarshal(m["args"], &args)
	return args, err
}

//Attribute Selections are an operation on a relation
// e.g. currentPrice < highPrice
// Valid operations \in {<, <=, =, !=, >=, >}
type AttrSelection struct {
	AttrA string `json:"attrA"`
	Op string `json:"op"`
	AttrB string `json:"attrB"`
}
func (as AttrSelection) toSQL() (SQLPart, error) {
	var s SQLPart
	if false == ValidOp(as.Op){ return s, errors.New("Invalid operator") }
	return SQLPart{SQL: "? " + as.Op + " ?", Args:[]string{as.AttrA, as.AttrB}}, nil
}
func (as AttrSelection) validateSemantics(t *SchemaInfo) bool {
	//TODO: Ensure AttrA and AttrB are present in table info (if production mode)
	//TODO: Ensure that if AttrA or AttrB are attributes of other tables,
	//  that they are present in their schemas (if production mode)
	return true
}
func (as AttrSelection) MarshalJSON() (b []byte, err error) {
    return json.Marshal(map[string]interface{}{
			"type": "ATTR_SELECTION",
			"attrA": as.AttrA,
			"op": as.Op,
			"attrB": as.AttrB,
    })
}


//Value Selections are an operation on a relation
// e.g. currentPrice = 34
type ValueSelection struct {
	AttrA string `json:"attrA"`
	Op string `json:"op"`
	Value string `json:"value"`
}
func (vs ValueSelection) toSQL() (SQLPart, error) {
	var s SQLPart
	if false == ValidOp(vs.Op){ return s, errors.New("Invalid operator") }
	return SQLPart{SQL: "? " + vs.Op + " ?", Args:[]string{vs.AttrA, vs.Value}}, nil
}
func (vs ValueSelection) validateSemantics(t *SchemaInfo) bool {
	//TODO: Ensure AttrA is present in table info (if production mode)
	return true
}
func (vs ValueSelection) MarshalJSON() (b []byte, err error) {
    return json.Marshal(map[string]interface{}{
			"type": "VALUE_SELECTION",
			"attrA": vs.AttrA,
			"op": vs.Op,
			"value": vs.Value,
    })
}

//Standard Not unary operation
type Not struct {
	A Formula
}
func (n Not) toSQL() (SQLPart, error) {
	aSQL, err := n.A.toSQL()
	return SQLPart{SQL: "NOT (" + aSQL.SQL + ")", Args:aSQL.Args}, err
}
func (n Not) validateSemantics(t *SchemaInfo) bool {
	return n.A.validateSemantics(t)
}
func (n *Not) FromJSON(b []byte) (err error){
	args, err := extractArgs(b)
	if err != nil { return err }

	//Extract nested formulas
	if len(args) < 1 {
		return errors.New("NOT operator requires one argument")
	}
	f1, err := FormulaFromJSON(args[0])
	if err != nil { return err }
	n = &Not{ A: f1 }
	return nil
}
func (n Not) MarshalJSON() (b []byte, err error) {
	inner, err := json.Marshal(n.A)
	if err != nil {
		return b, err
	}
	return json.Marshal(map[string]interface{}{
		"type": "NOT",
		"args": []json.RawMessage{inner},
	})
}

//Standard Or binary operation
type Or struct {
	A Formula
	B Formula
}
func (o Or) toSQL() (SQLPart, error) {
	aSQL, err := o.A.toSQL()
	if err != nil { return SQLPart{}, err }
	bSQL, err := o.B.toSQL()
	if err != nil { return SQLPart{}, err }
	return SQLPart{SQL: "(" + aSQL.SQL + " OR " + bSQL.SQL + ")",
		Args:append(aSQL.Args, bSQL.Args...)}, err
}
func (o Or) validateSemantics(t *SchemaInfo) bool {
	return o.A.validateSemantics(t) && o.B.validateSemantics(t)
}
func (o *Or) FromJSON(b []byte) (err error){
	args, err := extractArgs(b)
	if err != nil { return err }

	if len(args) < 2 {
		return errors.New("Or operator requires two arguments")
	}

	//Extract nested formulas
	f1, err := FormulaFromJSON(args[0])
	if err != nil { return err }
	f2, err := FormulaFromJSON(args[1])
	if err != nil { return err }
	o = &Or{ A: f1, B: f2 }
	return nil
}
func (o Or) MarshalJSON() (b []byte, err error) {
	innerA, err := json.Marshal(o.A)
	if err != nil {	return b, err }
	innerB, err := json.Marshal(o.B)
	if err != nil {	return b, err }
	return json.Marshal(map[string]interface{}{
		"type": "OR",
		"args": []json.RawMessage{innerA, innerB},
	})
}


//Standard And binary operation
type And struct {
	A Formula
	B Formula
}
func (a And) toSQL() (SQLPart, error) {
	aSQL, err := a.A.toSQL()
	if err != nil { return SQLPart{}, err }
	bSQL, err := a.B.toSQL()
	return SQLPart{SQL: "(" + aSQL.SQL + " AND " + bSQL.SQL + ")",
		Args:append(aSQL.Args, bSQL.Args...)}, err
}
func (a And) validateSemantics(t *SchemaInfo) bool {
	return a.A.validateSemantics(t) && a.B.validateSemantics(t)
}
func (a *And) FromJSON(b []byte) (err error){
	args, err := extractArgs(b)
	if err != nil { return err }

	//Extract nested formulas
	f1, err := FormulaFromJSON(args[0])
	if err != nil { return err }
	f2, err := FormulaFromJSON(args[1])
	if err != nil { return err }
	a = &And{ A: f1, B: f2 }
	return nil
}
func (a And) MarshalJSON() (b []byte, err error) {
	innerA, err := json.Marshal(a.A)
	if err != nil {	return b, err }
	innerB, err := json.Marshal(a.B)
	if err != nil {	return b, err }
	return json.Marshal(map[string]interface{}{
		"type": "AND",
		"args": []json.RawMessage{innerA, innerB},
	})
}


//Convert JSON to a Formula
// JSON Formulas are expected to be of the type:
// formula := {type: "AND/OR/NOT", args: [formula]} | AttrSelection | ValueSelection
func FormulaFromJSON(b []byte) (formula Formula, err error) {
	var f Formula
	m := make(map[string]json.RawMessage)
	err = json.Unmarshal(b, &m)
	if err != nil {
		return f, err
	}

	//Extract type string from map
	var ty string
	_, ok := m["type"]
	if false == ok { return f, errors.New("No `type` key found") }
	err = json.Unmarshal(m["type"], &ty)
	ty = strings.ToUpper(ty)
	if err != nil {
		return f, err
	}

	//Group the binary operators together for processing convenience
	if ty == "AND" {
		var a And
		err = a.FromJSON(b)
		return a, err
	} else if ty == "OR" {
		var o Or
		err = o.FromJSON(b)
		return o, err
	} else if ty == "NOT" {
		var n Not
		err = n.FromJSON(b)
		return n, err
	} else if ty == "ATTR_SELECTION" {
		var attrSel AttrSelection
		err = json.Unmarshal(b, &attrSel)
		return attrSel, err
	} else if ty == "VALUE_SELECTION" {
		var valSel ValueSelection
		err = json.Unmarshal(b, &valSel)
		return valSel, err
	}
	return f, errors.New("Undefined formula type "+ty)
}

//Structure representing a SELECT SQL query
type SelectQuery struct {
	Table string `json:"table"`
	Selection Formula `json:"selection"`
	//Columns []string //Not yet used
}

//Structure representing an INSERT SQL query
type InsertQuery struct {
	Table string `json:"table"`
	Data map[string]interface{} `json:"data"`
}

// We need a custom UnmarshalJSON to use our custom FormulaFromJSON function
//func (sq SelectQuery) UnmarshalJSON(b []byte) (err error) {
func UnmarshalSelectQuery(sq *SelectQuery, b []byte) (err error) {
	m := make(map[string]json.RawMessage)
	err = json.Unmarshal(b, &m)
	if err != nil {
		return err
	}

	//Extract table string from map
	_, ok := m["table"]
	if false == ok { return errors.New("No `table` key found") }
	err = json.Unmarshal(m["table"], &sq.Table)
	if err != nil {	return errors.New("Table err "+err.Error()) }

	//Extract selection from map
	_, ok = m["selection"]
	if false == ok { return errors.New("No `selection` key found") }
	sq.Selection, err = FormulaFromJSON(m["selection"])
	if err != nil {	return errors.New("Selection err "+err.Error()) }
	return nil
}
