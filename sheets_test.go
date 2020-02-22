package main

import (
	"fmt"
	"reflect"
	"testing"
)

// create, get, delete
func TestCreateGetDeleteSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.CreateSpreadsheet("Test First!")
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
	type TestStructMeme struct {
		Name1 int16
		Name2 int32
		Name3 int
		Name4 float64
		Name5 string
		Name6 bool
	}
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
