package gosheet

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

type TestStructMeme struct {
	Name1 int16
	Name2 int32
	Name3 int
	Name4 float64
	Name5 string
	Name6 bool
}

type TestStructSmall struct {
	Yes  bool
	Name string
}

func describeDatabase(db *Database) {
	fmt.Println("Sheet ID: ", db.Spreadsheet().SpreadsheetId)
	fmt.Println("Sheet Name: ", db.Spreadsheet().Properties.Title)
	fmt.Println("Sheet Timezone: ", db.Spreadsheet().Properties.TimeZone)
}

// create, get, delete
func TestCreateGetDeleteSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.CreateDatabase("Test First!2")
	fmt.Printf("------Created database %p------\n", db)
	describeDatabase(db)
	sheetID := db.Spreadsheet().SpreadsheetId
	manager.deleteSpreadsheet(sheetID)
	fmt.Println("------Deleted database ", sheetID, "------")
}

// get
func TestGetSpreadsheetByID(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheetID := "1QMUZpqgBCHWEFQ7YEWBwmWwkrtU8yNOTJD0srqt4aFc"
	sheet := manager.getSpreadsheet(sheetID)
	fmt.Println("------Get database ", sheetID, "------")
	fmt.Println("------    Result ", sheet.SpreadsheetId)
}

// create
func TestCreateSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.CreateDatabase("Testing!")
	fmt.Println("------Created database------")
	describeDatabase(database)
}

// list db
func TestListSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheets := manager.listSpreadsheets()
	for _, s := range sheets {
		sheet := manager.getSpreadsheet(s)
		fmt.Println("------Listing sheet------")
		fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
		fmt.Println("Sheet Name: ", sheet.Properties.Title)
		fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	}
}

// Find DB
func TestFindSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.findSpreadsheet("Test First!")
	if sheet != nil {
		fmt.Println("------Listing sheet------")
		fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
		fmt.Println("Sheet Name: ", sheet.Properties.Title)
		fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	}
}

// Delete DB
func TestDeleteDatabase(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet := manager.findSpreadsheet(dbFileStart + "testdb")
	fmt.Println("------Found sheet------")
	fmt.Println("Sheet ID: ", sheet.SpreadsheetId)
	fmt.Println("Sheet Name: ", sheet.Properties.Title)
	fmt.Println("Sheet Timezone: ", sheet.Properties.TimeZone)
	sheetID := sheet.SpreadsheetId
	if manager.deleteSpreadsheet(sheet.SpreadsheetId) {
		fmt.Println("------Deleted sheet ", sheetID, "------")
	}
}

// Create DB
func TestCreateDatabase(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.CreateDatabase("testdb")
	if database == nil {
		t.Fatal("database nil")
	}
	fmt.Println("------Listing sheet------")
	describeDatabase(database)
}

// Create table with index
func TestCreateTableWithIndex(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("database %s is nil", "testdb")
	}

	constraint := NewConstraint()
	constraint.UniqueColumns("Name1", "Name2")
	table := db.CreateTable(TestStructMeme{}, constraint)
	fmt.Printf("DB: %s Table %s[%d] created\n", db.spreadsheet.SpreadsheetId, table.Name(), table.SheetID())

	tableMeta := table.Metadata()
	fmt.Printf("Metadata: \n%+v\n", *tableMeta)

	tableIndex := table.index
	for i, v := range tableIndex.uniqueIndex {
		fmt.Printf("Idx[%s] %v\n", i, v)
	}
}

// List tables
func TestListTables(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.FindDatabase("testdb")
	database.Manager().SynchronizeFromGoogle(database)
	tables := database.ListTables()
	for i := range tables {
		fmt.Printf("DB: %s Table[%d] Name: %s\n", database.spreadsheet.SpreadsheetId, i, tables[i].Name())
	}
}

// Delete table
func TestDeleteTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.FindDatabase("testdb")
	if database == nil {
		t.Fatal("database is nil")
	}
	fmt.Println("------This database------")
	describeDatabase(database)
	fmt.Println("------Viewing table------")

	table := database.FindTable(TestStructMeme{})
	if table == nil {
		t.Fatal("Table deleted")
		return
	}

	tableName := table.Name()
	deleted := table.Drop()
	if deleted {
		fmt.Println("Deleted Table = ", tableName)
	} else {
		fmt.Println("Failed delete Table = ", tableName)
	}

	tables := database.ListTables()
	fmt.Println("------Viewing table------")
	for i := range tables {
		fmt.Printf("Table[%d] Name: %s\n", i, tables[i].Name())
	}
}

func TestResetTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.FindDatabase("testdb")
	if database == nil {
		return
	}

	fmt.Println("------This database------")
	describeDatabase(database)

	tableName := "TestStructMeme"
	table := database.FindTable(TestStructMeme{})
	if table == nil {
		t.Fatal("Table is nil")
	}
	deleted := table.Drop()
	if deleted {
		fmt.Println("Deleted Table = ", tableName)
	} else {
		fmt.Println("Failed delete Table = ", tableName)
	}
	manager.SynchronizeFromGoogle(database)

	table = database.CreateTable(TestStructMeme{})
	fmt.Printf("Table %s[%d] created\n", table.Name(), table.SheetID())
}

func TestReadTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	// Find or make table
	table := db.FindTable(TestStructMeme{})
	if table == nil {
		table = db.CreateTable(TestStructMeme{})
		fmt.Printf("Table %s[%d] created\n", table.Name(), table.SheetID())
	} else {
		fmt.Printf("Table %s[%d] found\n", table.Name(), table.SheetID())
	}

	tableValue, _ := table.Select(-1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

func TestReadAndWriteTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	// Find or make table
	table := db.FindTable(TestStructMeme{})
	if table == nil {
		table = db.CreateTable(TestStructMeme{})
		fmt.Printf("Table %s[%d] created\n", table.Name(), table.SheetID())
	} else {
		fmt.Printf("Table %s[%d] found\n", table.Name(), table.SheetID())
	}

	// Write to table
	var values []interface{}
	for i := 0; i < 10; i++ {
		rand.Seed(time.Now().Unix())
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
	didSuccess := table.UpsertIf(values, true)
	if didSuccess {
		fmt.Printf("Table %s[%d] Success Write %d Data\n", table.Name(), table.SheetID(), len(values))
	} else {
		fmt.Printf("Table %s[%d] Failed  Write %d Data\n", table.Name(), table.SheetID(), len(values))
	}

	tableMeta := table.Metadata()
	fmt.Printf("Metadata: %+v\n", *tableMeta)

	tableValue, _ := table.Select(-1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

func TestReadTableWithFilter(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}

	// Find or make table
	table := db.FindTable(TestStructMeme{})
	if table == nil {
		table = db.CreateTable(TestStructMeme{})
		fmt.Printf("Table %s[%d] created\n", table.Name(), table.SheetID())
	} else {
		fmt.Printf("Table %s[%d] found\n", table.Name(), table.SheetID())
	}

	filter := func(field interface{}) bool {
		return field.(string) == "TRUE"
	}
	filter2 := func(field interface{}) bool {
		v, _ := field.(string)
		v2, _ := strconv.ParseInt(v, 10, 16)
		return int16(v2) < 0
	}

	filterMap := make(map[int]Predicate)
	filterMap[5] = filter
	filterMap[0] = filter2

	tableValue, _ := table.SelectAndFilter(filterMap)
	fmt.Println("Raw::::")
	for i, v := range tableValue {
		fmt.Println(i, v)
	}
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

func TestDeleteRow(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("Sheet %s is nil", "testdb")
	}
	fmt.Println("------1")

	// Find or make table
	table := db.FindTable(TestStructMeme{})
	if table == nil {
		table = db.CreateTable(TestStructMeme{})
		fmt.Printf("Table %s[%d] created\n", table.Name(), table.SheetID())
	} else {
		fmt.Printf("Table %s[%d] found\n", table.Name(), table.SheetID())
	}
	fmt.Println("------2")

	filter0 := func(field interface{}) bool {
		v, _ := field.(string)
		v2, _ := strconv.ParseInt(v, 10, 16)
		return int16(v2) < 0
	}
	// filter5 := func(field interface{}) bool {
	// 	return field.(string) == "TRUE"
	// }

	tmpIdx := 1
	predicate := func(row []interface{}) bool {
		p1 := filter0(row[0])
		// p2 := filter5(row[5])
		if p1 {
			fmt.Println("Row ", tmpIdx, row)
			tmpIdx++
		}
		return p1
	}

	deletedIndex := table.Delete(predicate)
	for i := 0; i < len(deletedIndex); i++ {
		fmt.Println("Deleted: ", deletedIndex[i])
	}
	fmt.Println("------3")

	tableValue, _ := table.Select(-1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
	fmt.Println("------4")
}

func TestConstraintTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("database %s is nil", "testdb")
	}

	table := db.FindTable(TestStructSmall{})
	if table == nil {
		constraint := NewConstraint()
		constraint.UniqueColumns("Yes", "Name")
		table = db.CreateTable(TestStructSmall{}, constraint)
		fmt.Printf("----DB: %s Table %s[%d] created\n", db.spreadsheet.SpreadsheetId, table.Name(), table.SheetID())
	} else {
		fmt.Printf("----DB: %s Table %s[%d] found\n", db.spreadsheet.SpreadsheetId, table.Name(), table.SheetID())
	}

	tableMeta := table.Metadata()
	fmt.Printf("----Metadata: \n%+v\n", *tableMeta)

	bucket := make([]interface{}, 5)
	bucket[0] = TestStructSmall{
		Yes:  true,
		Name: "AAA",
	}
	bucket[1] = TestStructSmall{
		Yes:  true,
		Name: "AAA",
	}
	bucket[2] = TestStructSmall{
		Yes:  false,
		Name: "AAA",
	}
	bucket[3] = TestStructSmall{
		Yes:  true,
		Name: "ABA",
	}
	bucket[4] = TestStructSmall{
		Yes:  false,
		Name: "scdef",
	}
	table.UpsertIf(bucket, true)

	tableIndex := table.index
	for i, v := range tableIndex.uniqueIndex {
		fmt.Printf("----Idx[%s] %v\n", i, v)
	}

	data, tableMeta := table.Select(-1)
	fmt.Println("-----------------------------")
	fmt.Printf("Table name: %s Rows: %d\n", tableMeta.Name, tableMeta.Rows)
	fmt.Printf("      %v\n      %v\n", tableMeta.Types, tableMeta.Columns)
	for i := range data {
		fmt.Printf("%04d", i)
		for j := range data[i] {
			fmt.Printf("  %v", data[i][j])
		}
		fmt.Println()
	}

}
