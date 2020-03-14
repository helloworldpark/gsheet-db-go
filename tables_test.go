package gosheet

import (
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"

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

type TestStructSmall struct {
	Yes  bool
	Name string
}

func describeDatabase(db *Database) {
	if db == nil {
		fmt.Println("describeDatabase: db is nil")
		return
	}
	fmt.Println("-----Database--------------------------")
	fmt.Println("| ID: ", db.Spreadsheet().SpreadsheetId)
	fmt.Println("| Name: ", db.Spreadsheet().Properties.Title)
	fmt.Println("| Timezone: ", db.Spreadsheet().Properties.TimeZone)
	fmt.Println("---------------------------------------")
}

func describeSpreadsheet(sheet *sheets.Spreadsheet) {
	if sheet == nil {
		fmt.Println("describeDatabase: sheet is nil")
		return
	}
	fmt.Println("-----Spreadsheet-----------------------")
	fmt.Println("| ID: ", sheet.SpreadsheetId)
	fmt.Println("| Name: ", sheet.Properties.Title)
	fmt.Println("| Timezone: ", sheet.Properties.TimeZone)
	fmt.Println("---------------------------------------")
}

func describeTable(table *Table) {
	if table == nil {
		fmt.Println("describeTable: table is nil")
		return
	}
	if table.scheme == nil {
		fmt.Println("describeTable: table.scheme is nil")
		return
	}
	fmt.Println("-----Table-----------------------------")
	fmt.Println("| Name: ", table.scheme.Name)
	fmt.Println("| Rows: ", table.scheme.Rows)
	// fmt.Println("---------------------------------------")
	fmt.Println("| Scheme")
	fmt.Printf("| |")
	for i := 0; i < len(table.scheme.Columns); i++ {
		padding := len(table.scheme.Columns[i])
		if padding < len(table.scheme.Types[i].String()) {
			padding = len(table.scheme.Types[i].String())
		}
		padding -= len(table.scheme.Columns[i])
		padded := ""
		for padding > 0 {
			padded += " "
			padding--
		}
		fmt.Printf(" %s%s|", table.scheme.Columns[i], padded)
	}
	fmt.Printf("\n")
	fmt.Printf("| |")
	for i := 0; i < len(table.scheme.Columns); i++ {
		padding := len(table.scheme.Columns[i])
		if padding < len(table.scheme.Types[i].String()) {
			padding = len(table.scheme.Types[i].String())
		}
		padding -= len(table.scheme.Types[i].String())
		padded := ""
		for padding > 0 {
			padded += " "
			padding--
		}
		fmt.Printf(" %s%s|", table.scheme.Types[i].String(), padded)
	}
	fmt.Printf("\n")
	// fmt.Println("---------------------------------------")
	if table.scheme.Constraints != nil {
		fmt.Println("| Constraints: ", table.scheme.Constraints.uniqueColumns)
	}
	fmt.Println("---------------------------------------")
}

// database: create, get, delete
func TestCreateGetDeleteDatabase(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.CreateDatabase("Test First!2")
	fmt.Printf("------Created database %p------\n", db)
	describeDatabase(db)
	sheetID := db.Spreadsheet().SpreadsheetId
	manager.deleteSpreadsheet(sheetID)
	fmt.Println("------Deleted database ", sheetID, "------")
}

// spreadsheet: get
func TestGetSpreadsheetByID(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheetID := "1QMUZpqgBCHWEFQ7YEWBwmWwkrtU8yNOTJD0srqt4aFc"
	spreadsheet := manager.getSpreadsheet(sheetID)
	describeSpreadsheet(spreadsheet)
}

// spreadsheet: list
func TestListSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheets := manager.listSpreadsheets()
	fmt.Println("------Listing sheets------")
	for _, s := range sheets {
		sheet := manager.getSpreadsheet(s)
		describeSpreadsheet(sheet)
	}
}

// spreadsheet: find
func TestFindSpreadsheet(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet, _ := manager.findSpreadsheet("Test First!")
	fmt.Println("------Listing sheet------")
	describeSpreadsheet(sheet)
}

// spreadsheet: find + delete
func TestDeleteDatabase(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	sheet, _ := manager.findSpreadsheet(dbFileStart + "testdb")
	fmt.Println("------Find sheet------")
	describeSpreadsheet(sheet)
	sheetID := sheet.SpreadsheetId
	if manager.deleteSpreadsheet(sheet.SpreadsheetId) {
		fmt.Println("------Delete sheet ", sheetID, "------")
	}
}

// db: create, no index
func TestCreateDatabase(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.CreateDatabase("testdb")
	if database == nil {
		t.Fatal("database nil")
	}
	fmt.Println("------Create sheet------")
	describeDatabase(database)
}

// table: create, index
func TestCreateTableWithIndex(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("database %s is nil", "testdb")
	}

	constraint := NewConstraint()
	constraint.SetUniqueColumns("Name1", "Name2")
	table := db.createTable(TestStructMeme{}, constraint)
	fmt.Printf("DB: %s Table %s[%d] created\n", db.spreadsheet.SpreadsheetId, table.Name(), table.sheetID())

	tableMeta := table.header()
	fmt.Printf("Metadata: \n%+v\n", *tableMeta)

	for i, v := range table.index.uniqueIndex {
		fmt.Printf("Idx[%s] %v\n", i, v)
	}
}

// table: list
func TestListTables(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.FindDatabase("testdb")
	database.Manager().synchronizeFromGoogle(database)
	tables := database.ListTables()
	for i := range tables {
		describeTable(tables[i])
	}
}

// table: drop
func TestDropTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.FindDatabase("testdb")
	if database == nil {
		t.Fatal("database is nil")
	}
	describeDatabase(database)
	table := database.FindTable(TestStructMeme{})
	if table == nil {
		t.Fatal("table deleted")
	}

	tableName := table.Name()
	dropped := table.Drop()
	if dropped {
		fmt.Println("Dropped table = ", tableName)
	} else {
		fmt.Println("Failed drop table = ", tableName)
	}

	tables := database.ListTables()
	for i := range tables {
		describeTable(tables[i])
	}
}

// table: reset(convenicence, delete + create)
func TestResetTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	database := manager.FindDatabase("testdb")
	if database == nil {
		return
	}

	describeDatabase(database)

	table := database.FindTable(TestStructMeme{})
	if table == nil {
		t.Fatal("Table is nil")
	}
	tableName := table.Name()
	deleted := table.Drop()
	if deleted {
		fmt.Println("Deleted Table ", tableName)
	} else {
		fmt.Println("Failed drop Table ", tableName)
	}

	table = database.createTable(TestStructMeme{})
	fmt.Println("Table created")
	describeTable(table)
}

// table: read
func TestReadTable(t *testing.T) {
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
	describeTable(table)

	tableValue, _ := table.selectData(-1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

// table: read, write
func TestReadAndWriteTable(t *testing.T) {
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
	describeTable(table)

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
			fmt.Printf("W[%d]%s = %v", i, reflected.Type().Field(j).Name, field.Interface())
		}
		fmt.Printf("\n")

		values = append(values, meme)
	}
	fmt.Println("Table before")
	describeTable(table)
	didSuccess := table.upsertIf(values, true)
	if didSuccess {
		fmt.Printf("Table %s[%d] Success Write %d Data\n", table.Name(), table.sheetID(), len(values))
	} else {
		fmt.Printf("Table %s[%d] Failed  Write %d Data\n", table.Name(), table.sheetID(), len(values))
	}
	fmt.Println("Table after")
	describeTable(table)

	tableValue, _ := table.selectData(-1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

// table: read, filter
func TestReadTableWithFilter(t *testing.T) {
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
	describeTable(table)

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

	tableValue, _ := table.selectAndFilter(filterMap)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
}

// table: delete row
func TestDeleteRow(t *testing.T) {
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
	describeTable(table)

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

	deletedIndex := table.delete(predicate)
	for i := 0; i < len(deletedIndex); i++ {
		fmt.Println("Deleted: ", deletedIndex[i])
	}
	fmt.Println("------3")

	tableValue, _ := table.selectData(-1)
	for i := 0; i < len(tableValue); i++ {
		for j := 0; j < len(tableValue[i]); j++ {
			fmt.Printf("V[%d][%d] = %v ", i, j, tableValue[i][j])
		}
		fmt.Printf("\n")
	}
	fmt.Println("------4")
}

// table: create + constraint
func TestConstraintTable(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("database %s is nil", "testdb")
	}

	table := db.FindTable(TestStructSmall{})
	if table == nil {
		constraint := NewConstraint()
		constraint.SetUniqueColumns("Yes", "Name")
		table = db.createTable(TestStructSmall{}, constraint)
		fmt.Println("Table created\n", table.Name(), table.sheetID())
	} else {
		fmt.Println("Table found\n", table.Name(), table.sheetID())
	}
	describeTable(table)

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
	table.upsertIf(bucket, true)

	data, tableMeta := table.selectData(-1)
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

// table: validation
func TestInvalidSchemeValue(t *testing.T) {
	manager := NewSheetManager(jsonPath)
	db := manager.FindDatabase("testdb")
	if db == nil {
		t.Fatalf("database %s is nil", "testdb")
	}

	table := db.FindTable(TestStructSmall{})
	if table == nil {
		constraint := NewConstraint()
		constraint.SetUniqueColumns("Yes", "Name")
		table = db.createTable(TestStructSmall{}, constraint)
		fmt.Println("Table created\n", table.Name(), table.sheetID())
	} else {
		fmt.Println("Table found\n", table.Name(), table.sheetID())
	}
	describeTable(table)

	bucket := make([]interface{}, 5)
	bucket[0] = struct {
		floating float64
		testing  string
	}{
		floating: 1.23,
		testing:  "ssdfod",
	}

	table.upsertIf(bucket, true)

	for i, v := range table.index.uniqueIndex {
		fmt.Printf("----Idx[%s] %v\n", i, v)
	}

	data, tableMeta := table.selectData(-1)
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
