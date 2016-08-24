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
   * This might actually be a bad idea. We have no way of enforcing uniqueness
   *  on the inserted artist.
*/


//Simple structure that contains an SQL (sub-)query and its
// arguments to be replaced.
// Idents contains identifiers in sequence of appearance, signified by %
// Args contains arguments in sequence of appearance, signified by ?
// e.g. s := SQLPart{SQL="% = ?", Idents={"name"}, Args={"Charlie"}}
type SQLPart struct {
	SQL string
	Idents []string
	Args []interface{}
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

type Tautology struct {
	
}

func (t Tautology) toSQL() (SQLPart, error){
	return SQLPart{SQL: "true"}, nil
}
func (t Tautology) validateSemantics(s *SchemaInfo) bool{
	return true
}

func (t Tautology) MarshalJSON() (b []byte, err error) {
    return json.Marshal(map[string]interface{}{
			"type": "TAUTOLOGY",
    })
}

//Function to determine if a given string is a valid SQL binary operation
func ValidOp(s string) bool {
	validOps := []string{"<", "<=", "=", "!=", ">=", ">", "LIKE"}
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
	//SQL Type cast for attribute A
	CastA string
	//SQL Type cast for attribute B
	CastB string
}
func (as AttrSelection) toSQL() (SQLPart, error) {
	var s SQLPart
	if false == ValidOp(as.Op){ return s, errors.New("Invalid operator "+as.AttrA+" "+as.Op+" "+as.AttrB) }
	return SQLPart{SQL: cast("%s", as.CastA) + " " + as.Op + " "+cast("%s", as.CastB), Idents:[]string{as.AttrA, as.AttrB}}, nil
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

//Returns the text for an SQL type cast of `contents` to `ty`
func cast(contents string, ty string) string{
	if ty == "" { return contents }
	return "("+contents+")::"+ty
}

//Value Selections are an operation on a relation
// e.g. currentPrice = 34
type ValueSelection struct {
	Attr string `json:"attr"`
	Op string `json:"op"`
	Value interface{} `json:"value"`
	//SQL Type cast for attribute
	Cast string
}
func (vs ValueSelection) toSQL() (SQLPart, error) {
	var s SQLPart
	if false == ValidOp(vs.Op){ return s, errors.New("Invalid operator "+vs.Attr+" via "+vs.Op) }
	return SQLPart{SQL: cast("%s", vs.Cast) + " " + vs.Op + " ?",
		Idents: []string{vs.Attr},
		Args:[]interface{}{vs.Value}}, nil
}
func (vs ValueSelection) validateSemantics(t *SchemaInfo) bool {
	//TODO: Ensure Attr is present in table info (if production mode)
	return true
}
func (vs ValueSelection) MarshalJSON() (b []byte, err error) {
    return json.Marshal(map[string]interface{}{
			"type": "VALUE_SELECTION",
			"attr": vs.Attr,
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
	return SQLPart{SQL: "NOT (" + aSQL.SQL + ")",
		Idents:aSQL.Idents,
		Args:aSQL.Args}, err
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
	n.A = f1
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
		Idents:append(aSQL.Idents, bSQL.Idents...),
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
	o.A = f1
	o.B = f2
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
		Idents:append(aSQL.Idents, bSQL.Idents...),
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
	a.A = f1
	a.B = f2
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
	} else if ty == "TAUTOLOGY" {
		return Tautology{}, nil
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
	//Map of fields to tables they are foreign keys to
	// These values are necessary only until the foreign key relationship
	// has been established
	ForeignKeys map[string]string `json:"foreign_keys"`
	//Map of fields to the autoscope types contained within them
	// Always optional, this simply provides additional control over the stored type
	Types map[string]string `json:"types"`
}

//Structure representing an UPDATE SQL query
type UpdateQuery struct {
	Table string `json:"table"`
	Selection Formula `json:"selection"`
	Data map[string]interface{} `json:"data"`
	ForeignKeys map[string]string `json:"foreign_keys"`
	Types map[string]string `json:"types"`
}


//Helper function to recursively transform formula attributes
func ModifyLeaves(fn func(Formula)Formula, formula Formula) Formula {
	switch formula.(type){
	case AttrSelection:
		return fn(formula)
	case ValueSelection:
		return fn(formula)
	case Or:
		return Or{
			A: ModifyLeaves(fn, formula.(Or).A),
			B: ModifyLeaves(fn, formula.(Or).B),
		}
	case And:
		return And{
			A: ModifyLeaves(fn, formula.(And).A),
			B: ModifyLeaves(fn, formula.(And).B),
		}
	case Not:
		return Not{
			A: ModifyLeaves(fn, formula.(Not).A),
		}
	}
	return formula
}

//Convert a list of Formulas into a tree of nested ANDs with the formulas
// as leaves
func NestAnds(formulas []Formula) Formula {
	if len(formulas) == 0 { return Tautology{} } 
	if len(formulas) == 1 { return formulas[0] }
	return And{
		A: formulas[0],
		B: NestAnds(formulas[1:len(formulas)]),
	}
}

//Convert a list of Formulas into a tree of nested ORs with the formulas
// as leaves
func NestOrs(formulas []Formula) Formula {
	if len(formulas) == 1 { return formulas[0] }
	return Or{
		A: formulas[0],
		B: NestOrs(formulas[1:len(formulas)]),
	}
}

//Convert a map of key=value pairs to a Formula of nested ANDs terminating
func MapToAnds(m map[string]interface{}) Formula {
	formulas := make([]Formula, 0)
	for k, v := range m {
		formulas = append(formulas, ValueSelection{ Attr: k, Value: v, Op: "=" })
	}
	return NestAnds(formulas)
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
