package gosheet

import (
	"encoding/json"
)

type Constraint struct {
	uniqueColumns []string
	tableColumns  []string
}

func NewConstraint() *Constraint {
	return &Constraint{
		uniqueColumns: make([]string, 0),
		tableColumns:  make([]string, 0),
	}
}

func NewConstraintFromString(str string) *Constraint {
	if len(str) == 0 {
		return nil
	}
	constraintMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(str), &constraintMap)
	if err != nil {
		panic(err)
	}

	constraint := &Constraint{}
	if v, ok := constraintMap["uniqueColumns"]; ok {
		vstring := make([]string, 0)
		for _, str := range v.([]interface{}) {
			vstring = append(vstring, str.(string))
		}
		constraint.uniqueColumns = vstring
	}
	if v, ok := constraintMap["tableColumns"]; ok {
		vstring := make([]string, 0)
		for _, str := range v.([]interface{}) {
			vstring = append(vstring, str.(string))
		}
		constraint.tableColumns = vstring
	}
	return constraint
}

func (c *Constraint) UniqueColumns(columns ...string) *Constraint {
	// Check if valid
	if len(columns) > 0 && len(columns) == len(c.uniqueColumns) {
		allSame := true
		for i := range columns {
			allSame = allSame && (columns[i] == c.uniqueColumns[i])
		}
		if allSame {
			panic("Unique Constraint shouldn't be equal to before")
		}
	}

	c.uniqueColumns = columns
	return c
}

func (c *Constraint) toMap() map[string]interface{} {
	constraintMap := make(map[string]interface{})
	constraintMap["uniqueColumns"] = c.uniqueColumns

	return constraintMap
}
