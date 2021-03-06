package gosheet

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
)

type tableIndex struct {
	primaryIndex map[string]bool    // key: hex of value, value: list of index position
	uniqueIndex  map[string][]int64 // key: hex of value, value: list of index position
}

func newTableIndex() *tableIndex {
	index := &tableIndex{}
	index.primaryIndex = make(map[string]bool, 0)
	index.uniqueIndex = make(map[string][]int64, 0)
	return index
}

// https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
// values: array of values which are splitted to column values
func (index *tableIndex) build(values [][]interface{}, metadata *TableScheme) {
	if metadata.Constraints == nil {
		return
	}
	if len(metadata.Constraints.uniqueColumns) == 0 {
		return
	}

	// clear index
	index.primaryIndex = make(map[string]bool)
	index.uniqueIndex = make(map[string][]int64)

	for i, v := range values {
		uniqueColumns := metadata.columnsToIndices(metadata.Constraints.uniqueColumns)
		uniqueHash := index.hashcode(v, uniqueColumns...)
		bucket, ok := index.uniqueIndex[uniqueHash]
		if ok {
			bucket = append(bucket, int64(i))
			index.uniqueIndex[uniqueHash] = bucket
		} else {
			bucket = make([]int64, 0)
			bucket = append(bucket, int64(i))
			index.uniqueIndex[uniqueHash] = bucket
		}
	}
}

var trueOrFalse = map[bool]string{true: "TRUE", false: "FALSE"}

// value: single struct splitted to column values
func (index *tableIndex) hashcode(value []interface{}, columnIndices ...int64) string {
	reflectedValue := reflect.ValueOf(value)
	testValue := ""
	sort.Slice(columnIndices, func(i, j int) bool {
		return columnIndices[i] < columnIndices[j]
	})

	for _, idx := range columnIndices {
		field := reflectedValue.Index(int(idx))
		// 특별 예외: bool은 대문자로 변환
		var msg string
		if boolValue, ok := field.Interface().(bool); ok {
			msg = fmt.Sprintf("%v%v", idx, trueOrFalse[boolValue])
		} else {
			msg = fmt.Sprintf("%v%v", idx, field.Interface())
		}

		testValue += msg
	}

	hashed := getIndexKey(testValue)
	return hashed
}

// value: struct splitted to column values
// return: bool hasIndex, []int64 indices
func (index *tableIndex) hasIndex(value []interface{}, columnIndices ...int64) (bool, []int64) {
	hashed := index.hashcode(value, columnIndices...)
	bucket, ok := index.uniqueIndex[hashed]
	if ok {
		return true, bucket
	}
	return false, nil
}

func getIndexKey(str string) string {
	k := sha256.Sum256([]byte(str))
	return hex.EncodeToString(k[:])
}
