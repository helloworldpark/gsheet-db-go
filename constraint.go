package gosheet

import "encoding/json"

type Constraint struct {
	primaryKey    []string
	autoIncrement bool

	uniqueColumns []string
}

func NewConstraint() *Constraint {
	return &Constraint{
		primaryKey:    make([]string, 0),
		autoIncrement: false,
		uniqueColumns: make([]string, 0),
	}
}

func NewConstraintFromString(str string) *Constraint {
	constraintMap := make(map[string]interface{})
	err := json.Unmarshal([]byte(str), constraintMap)
	if err != nil {
		panic(err)
	}
	return &Constraint{
		primaryKey:    constraintMap["primaryKey"].([]string),
		autoIncrement: constraintMap["autoIncrement"].(bool),
		uniqueColumns: constraintMap["uniqueColumns"].([]string),
	}
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

func (c *Constraint) ToMap() map[string]interface{} {
	if c == nil {
		panic("Constraiant is nil")
	}

	constraintMap := make(map[string]interface{})
	constraintMap["primarykey"] = c.primaryKey
	constraintMap["autoIncrement"] = c.autoIncrement
	constraintMap["uniqueColumns"] = c.uniqueColumns

	return constraintMap
}
