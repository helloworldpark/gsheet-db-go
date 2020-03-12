package gosheet

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
)

func init() {
	initPrimitiveKind()
}

const leftmostCol = "A"
const rightmostCol = "D"
const defaultRange = "A1D3"

// cellRange Leftmost: from 0, Upmost: from 0
// In excel style, Row 0, Column 0 is A1
// A <-> 0
// Z <-> 25
// endRow, endCol is not included in the range
type cellRange struct {
	sheetName                          string
	startRow, endRow, startCol, endCol int64
}

func newCellRange(sheetName string, startRow, startCol, endRow, endCol int64) cellRange {
	c := cellRange{
		startRow:  startRow,
		endRow:    endRow,
		startCol:  startCol,
		endCol:    endCol,
		sheetName: sheetName,
	}
	if (c.startCol < 0 || c.startCol >= c.endCol) && (c.startRow < 0 || c.startRow >= c.endRow) {
		err := fmt.Sprintf("Invalid cellRange: %+v", c)
		panic(err)
	}
	return c
}

func (c cellRange) String() string {

	leftmost := base26(c.startCol + 1)
	rightmost := base26(c.endCol + 1)

	ranges := fmt.Sprintf("%s%d:%s%d", leftmost, c.startRow+1, rightmost, c.endRow+1)
	ranges = fmt.Sprintf("%s!%s", c.sheetName, ranges)
	return ranges
}

func mininum64(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func maximum64(x, y int64) int64 {
	if x < y {
		return y
	}
	return x
}

func base26(x int64) string {
	if x < 0 {
		panic(fmt.Sprintf("x should not be negative: %d", x))
	}
	if x > 26 {
		panic(fmt.Sprintf("Unsupported number %d", x))
	}
	return string('@' + x)
}

// reflection-related
var primitiveStringToKind = make(map[string]reflect.Kind)
var primitiveKindToString = make(map[reflect.Kind]string)

func initPrimitiveKind() {
	if len(primitiveStringToKind) > 0 {
		return
	}
	for k := reflect.Bool; k <= reflect.Uint64; k++ {
		primitiveStringToKind[k.String()] = k
		primitiveKindToString[k] = k.String()
	}
	for k := reflect.Float32; k <= reflect.Float64; k++ {
		primitiveStringToKind[k.String()] = k
		primitiveKindToString[k] = k.String()
	}
	primitiveStringToKind[reflect.String.String()] = reflect.String
	primitiveKindToString[reflect.String] = reflect.String.String()
}

func isPrimitive(i interface{}) bool {
	_, ok := primitiveKindToString[reflect.TypeOf(i).Kind()]
	return ok
}

func nameOfStruct(i interface{}) string {
	return reflect.TypeOf(i).Name()
}

type structField struct {
	cname  string
	ctype  string
	ckind  reflect.Kind
	cvalue interface{}
}

func (f structField) isBool() bool {
	return f.ckind == reflect.Bool
}

func (f structField) isString() bool {
	return f.ckind == reflect.String
}

func (f structField) isNumeric() bool {
	return reflect.Int8 <= f.ckind && f.ckind <= reflect.Float64
}

func analyseStruct(structInstance interface{}) []structField {
	initPrimitiveKind()

	reflected := reflect.TypeOf(structInstance)
	if reflected.Kind() != reflect.Struct {
		return nil
	}
	reflectedValue := reflect.ValueOf(structInstance)

	n := reflected.NumField()
	fields := make([]structField, n)

	for i := 0; i < n; i++ {
		value := reflectedValue.Field(i)
		valueType := reflectedValue.Type().Field(i)
		valueKind := valueType.Type.Kind()

		typestring, ok := primitiveKindToString[valueKind]
		if !ok {
			fmt.Println("Not a struct: ", value)
			return nil
		}

		fields[i].ctype = typestring
		fields[i].cname = valueType.Name
		fields[i].ckind = valueKind
		fields[i].cvalue = value.Interface()
	}
	return fields
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
