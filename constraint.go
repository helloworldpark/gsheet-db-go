package gosheet

import (
	"encoding/json"
)

type Constraint struct {
	primaryKey    []string
	autoIncrement bool

	uniqueColumns []string
	tableColumns  []string
}

func NewConstraint() *Constraint {
	return &Constraint{
		primaryKey:    make([]string, 0),
		autoIncrement: false,
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
	if v, ok := constraintMap["primaryKey"]; ok {
		vstring := make([]string, 0)
		for _, str := range v.([]interface{}) {
			vstring = append(vstring, str.(string))
		}
		constraint.primaryKey = vstring
	}
	if v, ok := constraintMap["autoIncrement"]; ok {
		constraint.autoIncrement, ok = v.(bool)
	}
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

func (c *Constraint) PrimaryKey(key string, isAutoIncrement bool) *Constraint {
	if key == "" {
		c.primaryKey = make([]string, 0)
	} else {
		if len(c.primaryKey) == 0 {
			c.primaryKey = make([]string, 1)
		}
		c.primaryKey[0] = key
		c.autoIncrement = isAutoIncrement
	}
	return c
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
	constraintMap["primaryKey"] = c.primaryKey
	constraintMap["autoIncrement"] = c.autoIncrement
	constraintMap["uniqueColumns"] = c.uniqueColumns

	return constraintMap
}
