package gosheet

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func unitBenchmarkUpsertif(table *Table, values []interface{}, appendData bool) {
	table.upsertIf(values, appendData)
}

func createRandomDataMeme() ([]interface{}, int) {
	var values []interface{}
	seed := int(time.Now().Unix())
	for i := 0; i < 10; i++ {
		s := rand.NewSource(time.Now().Unix())
		r := rand.New(s)
		meme := TestStructMeme{
			Name1: int16(r.Int31()),
			Name2: r.Int31(),
			Name3: seed,
			Name4: r.Float64(),
			Name5: fmt.Sprintf("Perfume%d", i),
			Name6: r.Int()%2 == 0,
		}

		values = append(values, meme)
	}
	return values, seed
}

func BenchmarkTest(t *testing.B) {
	analyseStruct(TestStructMeme{})
}

func BenchmarkUpsertif(t *testing.B) {
	// create table
	t.StopTimer()
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	// Find or make table
	table := db.FindTable(TestStructMeme{})
	if table == nil {
		table = db.createTable(TestStructMeme{})
		fmt.Println("Table created\n", table.Name(), table.sheetID())
	} else {
		fmt.Println("Table found\n", table.Name(), table.sheetID())
	}
	tableName := table.Name()
	deleted := table.Drop()
	if deleted {
		fmt.Println("Deleted Table ", tableName)
	} else {
		fmt.Println("Failed drop Table ", tableName)
	}

	table = db.createTable(TestStructMeme{})
	fmt.Println("Table recreated")
	describeTable(table)

	fmt.Println("Starting benchmark")
	for i := 0; i < 20; i++ {
		values, _ := createRandomDataMeme()
		table.manager.enqueueAPIUsage(2, true)
		t.StartTimer()
		table.upsertIf(values, true)
		t.StopTimer()
	}
	now := time.Now().In(time.FixedZone("GMT-7", -7*60*60)).Unix()
	next := ((now / 100) + 1) * 100
	<-time.NewTimer(time.Duration(next-now+1) * time.Second).C
	fmt.Println("Finished benchmark")
}
