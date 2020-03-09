package gosheet

import (
	"fmt"
	"reflect"
)

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
