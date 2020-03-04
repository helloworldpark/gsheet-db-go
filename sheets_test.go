package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"google.golang.org/api/sheets/v4"
)

type TestStructMeme struct {
	Name1 int16
	Name2 int32
	Name3 int
	Name4 float64
	Name5 string
	Name6 bool
}

// create, get, delete
func TestCreateGetDeleteSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.CreateSpreadsheet("Test First!2")
	fmt.Printf("------Created sheet %p------\n", sheet)
	fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
	fmt.Println("Sheet Name: ", sheet.Properties.Title)
	fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	sheetId := sheet.SpreadsheetId
	manager.DeleteSpreadsheet(sheet.SpreadsheetId)
	fmt.Println("------Deleted sheet ", sheetId, "------")
	sheet = manager.GetSpreadsheet(sheetId)
	fmt.Println("------Get sheet ", sheetId, "------")
	fmt.Println("------    Result ", sheet)
}

// get
func TestGetSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheetID := "1QMUZpqgBCHWEFQ7YEWBwmWwkrtU8yNOTJD0srqt4aFc"
	sheet := manager.GetSpreadsheet(sheetID)
	fmt.Println("------Get sheet ", sheetID, "------")
	fmt.Println("------    Result ", sheet.SpreadsheetId)
}

// create
func TestCreateSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.CreateSpreadsheet("Testing!")
	fmt.Println("------Created sheet------")
	fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
	fmt.Println("Sheet Name: ", sheet.Properties.Title)
	fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
}

// list
func TestListSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheets := manager.ListSpreadsheets()
	for _, s := range sheets {
		sheet := manager.GetSpreadsheet(s)
		fmt.Println("------Listing sheet------")
		fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
		fmt.Println("Sheet Name: ", sheet.Properties.Title)
		fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	}
}

// Delete
func TestDeleteSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.FindSpreadsheet(dbFileStart + "testdb")
	fmt.Println("------Found sheet------")
	fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
	fmt.Println("Sheet Name: ", sheet.Properties.Title)
	fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	sheetId := sheet.SpreadsheetId
	if manager.DeleteSpreadsheet(sheet.SpreadsheetId) {
		fmt.Println("------Deleted sheet ", sheetId, "------")
	}
}

// Find
func TestSpreadsheet005(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.FindSpreadsheet("Test First!")
	if sheet != nil {
		fmt.Println("------Listing sheet------")
		fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
		fmt.Println("Sheet Name: ", sheet.Properties.Title)
		fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	}
}

// Create DB
func TestSpreadsheet006(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.CreateDatabase("testdb")
	if sheet != nil {
		fmt.Println("------Listing sheet------")
		fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
		fmt.Println("Sheet Name: ", sheet.Properties.Title)
		fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	}
}

// Create table
func TestCreateTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.FindDatabase("testdb")
	if sheet != nil {
		fmt.Println("------This database------")
		fmt.Println("Database ID:       ", sheet.SpreadsheetId)
		fmt.Println("Database Name:     ", sheet.Properties.Title)
		fmt.Println("Database Timezone: ", sheet.Properties.TimeZone)
		fmt.Println("------Creating table------")
		manager.CreateEmptyTable(sheet, "MAMA")
		manager.CreateEmptyTable(sheet, "NENE")
		tables := manager.GetTableList(sheet)
		for i := range tables {
			fmt.Printf("Table[%d] Name: %s\n", i, tables[i].Properties.Title)
		}
	}
}

// View table
func TestViewTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.FindDatabase("testdb")
	if sheet != nil {
		fmt.Println("------This database------")
		fmt.Println("Database ID:       ", sheet.SpreadsheetId)
		fmt.Println("Database Name:     ", sheet.Properties.Title)
		fmt.Println("Database Timezone: ", sheet.Properties.TimeZone)
		fmt.Println("------Viewing table------")
		tables := manager.GetTableList(sheet)
		for i := range tables {
			fmt.Printf("Table[%d] Name: %s\n", i, tables[i].Properties.Title)
		}
		table := manager.GetTable(sheet, "NENE")
		fmt.Printf("Table = %s\n", table.Properties.Title)
	}
}

// Delete table
func TestDeleteTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.FindDatabase("testdb")
	if sheet != nil {
		fmt.Println("------This database------")
		fmt.Println("Database ID:       ", sheet.SpreadsheetId)
		fmt.Println("Database Name:     ", sheet.Properties.Title)
		fmt.Println("Database Timezone: ", sheet.Properties.TimeZone)
		fmt.Println("------Viewing table------")

		tableName := "TestStructMeme"
		deleted := manager.DeleteTable(sheet, tableName)
		if deleted {
			fmt.Println("Deleted Table = ", tableName)
		} else {
			fmt.Println("Failed delete Table = ", tableName)
		}

		tables := manager.GetTableList(sheet)
		for i := range tables {
			fmt.Printf("Table[%d] Name: %s\n", i, tables[i].Properties.Title)
		}
	}
}

// Structs reflection test
func TestStructsReflection(t *testing.T) {
	type TestStruct struct {
		Name1 int16
		Name2 int32
		Name3 int
		Name4 float64
		Name5 string
		Name6 bool
	}

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

func TestCreateTableFromStructs(t *testing.T) {
	initPrimitiveKind()

	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	table := manager.CreateTableFromStruct(db, TestStructMeme{})
	fmt.Printf("Table %s[%d] created\n", table.Properties.Title, table.Properties.SheetId)

	tableMeta := manager.ReadTableMetadataFromStruct(db, TestStructMeme{})
	fmt.Printf("Table %s[%d] \nMetadata: %+v\n", table.Properties.Title, table.Properties.SheetId, *tableMeta)
}

func TestBase26(t *testing.T) {
	for i := 1; i <= 26*26*3+26*2+4; i++ {
		fmt.Printf("[%04d] = %s\n--------------\n", i, base26(int64(i)))
	}
}

func TestReadTable(t *testing.T) {
	initPrimitiveKind()

	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	// Find or make table
	table := manager.GetTable(db, "TestStructMeme")
	if table == nil {
		table = manager.CreateTableFromStruct(db, TestStructMeme{})
		fmt.Printf("Table %s[%d] created\n", table.Properties.Title, table.Properties.SheetId)
	} else {
		fmt.Printf("Table %s[%d] found\n", table.Properties.Title, table.Properties.SheetId)
	}

	tableValue := manager.ReadTableDataFromStruct(db, TestStructMeme{}, -1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

func TestReadAndWriteTable(t *testing.T) {
	initPrimitiveKind()

	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	// Find or make table
	table := manager.GetTable(db, "TestStructMeme")
	if table == nil {
		table = manager.CreateTableFromStruct(db, TestStructMeme{})
		fmt.Printf("Table %s[%d] created\n", table.Properties.Title, table.Properties.SheetId)
	} else {
		fmt.Printf("Table %s[%d] found\n", table.Properties.Title, table.Properties.SheetId)
	}

	// Write to table
	var values []interface{}
	for i := 0; i < 10; i++ {
		meme := TestStructMeme{
			Name1: int16(rand.Int31()),
			Name2: rand.Int31(),
			Name3: rand.Int(),
			Name4: rand.Float64(),
			Name5: fmt.Sprintf("Perfume%d", i),
			Name6: rand.Int()%2 == 0,
		}
		reflected := reflect.ValueOf(meme)
		for j := 0; j < reflected.NumField(); j++ {
			field := reflected.Field(j)
			fmt.Printf("W[%d]%s = %v ", i, reflected.Type().Field(j).Name, field.Interface())
		}
		fmt.Printf("\n")

		values = append(values, meme)
	}
	didSuccess := manager.WriteTableData(db, values, true)
	if didSuccess {
		fmt.Printf("Table %s[%d] Success Write %d Data\n", table.Properties.Title, table.Properties.SheetId, len(values))
	} else {
		fmt.Printf("Table %s[%d] Failed  Write %d Data\n", table.Properties.Title, table.Properties.SheetId, len(values))
	}

	tableMeta := manager.ReadTableMetadataFromStruct(db, TestStructMeme{})
	fmt.Printf("Table %s[%d] \nMetadata: %+v\n", table.Properties.Title, table.Properties.SheetId, *tableMeta)

	tableValue := manager.ReadTableDataFromStruct(db, TestStructMeme{}, -1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

func TestReadTableWithFilter(t *testing.T) {
	initPrimitiveKind()

	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	// Find or make table
	table := manager.GetTable(db, "TestStructMeme")
	if table == nil {
		table = manager.CreateTableFromStruct(db, TestStructMeme{})
		fmt.Printf("Table %s[%d] created\n", table.Properties.Title, table.Properties.SheetId)
	} else {
		fmt.Printf("Table %s[%d] found\n", table.Properties.Title, table.Properties.SheetId)
	}

	tableMeta := manager.ReadTableMetadataFromStruct(db, TestStructMeme{})
	fmt.Printf("Table %s[%d] \nMetadata: %+v\n", table.Properties.Title, table.Properties.SheetId, *tableMeta)

	filter := &sheets.FilterCriteria{}
	filter.Condition = &sheets.BooleanCondition{}
	filter.Condition.Type = "NUMBER_GREATER"
	filter.Condition.Values = make([]*sheets.ConditionValue, 1)
	filter.Condition.Values[0] = &sheets.ConditionValue{}
	filter.Condition.Values[0].UserEnteredValue = "0.5"

	filterMap := make(map[string]*sheets.FilterCriteria)
	filterMap["3"] = filter

	tableValue := manager.ReadTableDataWithFilter(db, TestStructMeme{}, filterMap)
	fmt.Println("Raw::::")
	fmt.Println(tableValue)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}
