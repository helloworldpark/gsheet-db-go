package gosheet

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash"
	"hash/fnv"
	"reflect"

	dynamicstruct "github.com/ompluscator/dynamic-struct"
)

type TableIndex struct {
	indices map[uint32][]int64 // key: hash value, value: list of index position
	hashGen func() hash.Hash32
}

func NewTableIndex() *TableIndex {
	tableIndex := &TableIndex{}
	tableIndex.indices = make(map[uint32][]int64, 0)
	tableIndex.hashGen = fnv.New32
	return tableIndex
}

// https://stackoverflow.com/questions/13582519/how-to-generate-hash-number-of-a-string-in-go
func (index *TableIndex) Build(values [][]interface{}, columnIndices ...int64) {
	if len(columnIndices) == 0 {
		return
	}

	// clear index
	index.indices = make(map[uint32][]int64)

	for i, v := range values {
		builder := dynamicstruct.NewStruct()
		for _, idx := range columnIndices {
			reflectedValue := reflect.ValueOf(v[idx])
			builder.AddField(fmt.Sprintf("F%d", idx), reflectedValue.Interface(), "")
		}
		rowvalue := builder.Build().New()

		hashed := getIndexKey(index.hashGen, rowvalue)
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

func getIndexKey(hashgen func() hash.Hash32, key interface{}) uint32 {
	hashmaker := hashgen()
	vbytes, err := bytesFromInterface(key)
	if err != nil {
		panic(err)
	}
	hashmaker.Write(vbytes)
	return hashmaker.Sum32()
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
