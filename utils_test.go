package gosheet

import (
	"fmt"
	"reflect"
	"testing"
)

type TestStruct struct {
	Name1 int16
	Name2 int32
	Name3 int
	Name4 float64
	Name5 string
	Name6 bool
}

// Structs reflection test
func TestStructsReflection(t *testing.T) {

	initPrimitiveKind()
	if isPrimitive(10.1) {
		fmt.Println("Is Primitive")
	} else {
		fmt.Println("Is not Primitive")
	}

	tt := TestStruct{Name1: 1, Name2: 5, Name3: 3, Name4: 100000000.3141592, Name5: ":ADFDE", Name6: true}
	fmt.Println("Name is ", reflect.TypeOf(tt).Name())
	analysed := analyseStruct(tt)
	if len(analysed) == 0 {
		fmt.Println("NONONO")
		return
	}

	for i := 0; i < len(analysed); i++ {
		fmt.Printf("[%d] Name=%s Type=%s Value=%v\n", i, analysed[i].cname, analysed[i].ctype, analysed[i].cvalue)
	}
}

func TestBase26(t *testing.T) {
	for i := 1; i <= 26*26*3+26*2+4; i++ {
		fmt.Printf("[%04d] = %s\n--------------\n", i, base26(int64(i)))
	}
}
