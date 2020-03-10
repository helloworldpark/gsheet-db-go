package gosheet

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"
	"sort"
)

type TableIndex struct {
	indices map[string][]int64 // key: hash value, value: list of index position
	hashGen func() hash.Hash32
}

func NewTableIndex() *TableIndex {
	tableIndex := &TableIndex{}
	tableIndex.indices = make(map[string][]int64, 0)
	tableIndex.hashGen = fnv.New32
	return tableIndex
}

// https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
// values: array of values which are splitted to column values
func (index *TableIndex) Build(values [][]interface{}, columnIndices ...int64) {
	if len(columnIndices) == 0 {
		return
	}

	// clear index
	index.indices = make(map[string][]int64)

	for i, v := range values {
		hashed := index.Hashcode(v, columnIndices...)
		bucket, ok := index.indices[hashed]
		if ok {
			bucket = append(bucket, int64(i))
			index.indices[hashed] = bucket
		} else {
			bucket = make([]int64, 0)
			bucket = append(bucket, int64(i))
			index.indices[hashed] = bucket
		}
	}
}

var trueOrFalse = map[bool]string{true: "TRUE", false: "FALSE"}

// value: single struct splitted to column values
func (index *TableIndex) Hashcode(value []interface{}, columnIndices ...int64) string {
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
func (index *TableIndex) HasIndex(value []interface{}, columnIndices ...int64) (bool, []int64) {
	hashed := index.Hashcode(value, columnIndices...)
	bucket, ok := index.indices[hashed]
	if ok {
		return true, bucket
	}
	return false, nil
}

func getIndexKey(str string) string {
	k := sha256.Sum256([]byte(str))
	return hex.EncodeToString(k[:])
}

// https://gist.github.com/miguelmota/5bfa2b6ab88f439fe0da0bfb1faca763
func bytesFromInterface(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
