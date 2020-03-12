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
	if isPrimitive(10.1) {
		fmt.Println("10.1 is Primitive")
	} else {
		fmt.Println("10.1 is not Primitive")
	}

	tt := TestStruct{Name1: 1, Name2: 5, Name3: 3, Name4: 100000000.3141592, Name5: ":ADFDE", Name6: true}
	structName := reflect.TypeOf(tt).Name()
	if isPrimitive(tt) {
		fmt.Println(structName, "is Primitive")
	} else {
		fmt.Println(structName, "is not Primitive")
	}

	analysed := analyseStruct(tt)
	if len(analysed) == 0 {
		fmt.Println("Failed to split ")
		return
	}

	for i := 0; i < len(analysed); i++ {
		fmt.Printf("[%d] Name=%s Type=%s Kind=%s Value=%v\n", i, analysed[i].cname, analysed[i].ctype, analysed[i].ckind, analysed[i].cvalue)
	}
}

func TestBase26(t *testing.T) {
	for i := 1; i <= 26*26*3+26*2+4; i++ {
		fmt.Printf("[%04d] = %s\n--------------\n", i, base26(int64(i)))
	}
}
