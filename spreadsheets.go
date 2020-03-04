package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"

	"golang.org/x/oauth2"
	"google.golang.org/api/sheets/v4"
)

const dbFileStart = "database_file_"

// SheetManager Manage OAuth2 token lifecycle
type SheetManager struct {
	client  *http.Client
	service *sheets.Service

	token          *oauth2.Token
	credentialJSON string
}

// NewSheetManager Create new SheetManager
func NewSheetManager(jsonPath string) *SheetManager {
	client := http.DefaultClient
	service, _ := sheets.New(client)
	token := CreateJWTToken(jsonPath)

	m := &SheetManager{
		client:         client,
		token:          token,
		service:        service,
		credentialJSON: jsonPath,
	}
	return m
}

// RefreshToken refreshes token if not valid
func (m *SheetManager) RefreshToken() {
	if !m.token.Valid() {
		token := CreateJWTToken(m.credentialJSON)
		m.token = token
	}
	return
}

// CreateSpreadsheet creates a single spreadsheet file
func (m *SheetManager) CreateSpreadsheet(title string) *sheets.Spreadsheet {
	// check duplicated title
	if m.FindSpreadsheet(title) != nil {
		return nil
	}

	return newSpreadsheetCreateRequest(m, title).Do()
}

// GetSpreadsheet gets a single spreadsheet file with id, if exists.
func (m *SheetManager) GetSpreadsheet(spreadsheetID string) *sheets.Spreadsheet {
	req := m.service.Spreadsheets.Get(spreadsheetID).IncludeGridData(true)
	req.Header().Add("Authorization", "Bearer "+m.token.AccessToken)
	resp, err := req.Do()
	if err != nil {
		panic(err)
	}
	return resp
}

// DeleteSpreadsheet deletes spreadsheet file with `spreadsheetId`
// Returns true if deleted(status code 20X)
//         false if else
// https://stackoverflow.com/questions/46836393/how-do-i-delete-a-spreadsheet-file-using-google-spreadsheets-api
// https://stackoverflow.com/questions/46310113/consume-a-delete-endpoint-from-golang
func (m *SheetManager) DeleteSpreadsheet(spreadsheetID string) bool {
	resp := newURLRequest(m, delete, fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s", spreadsheetID)).Do()
	return resp.StatusCode/100 == 2
}

// ListSpreadsheets lists spreadsheets' id by []string
func (m *SheetManager) ListSpreadsheets() []string {
	req := newURLRequest(m, get, "https://www.googleapis.com/drive/v3/files")
	// https://stackoverflow.com/questions/30652577/go-doing-a-get-request-and-building-the-querystring
	// https://developers.google.com/drive/api/v3/mime-types
	req.AddQuery("q", "mimeType='application/vnd.google-apps.spreadsheet'")
	resp := req.Do()
	read, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		panic(err)
	}

	var readJSON interface{}
	json.Unmarshal(read, &readJSON)

	mapJSON, ok := readJSON.(map[string]interface{})
	if !ok {
		return nil
	}

	files, ok := mapJSON["files"]
	if !ok {
		return nil
	}
	fileArr, ok := files.([]interface{})
	if !ok {
		return nil
	}

	var sheetArr []string
	for _, f := range fileArr {
		fmap, ok := f.(map[string]interface{})
		if !ok {
			continue
		}
		trashed, ok := fmap["trashed"].(bool)
		if ok && trashed {
			continue
		}
		fileID, ok := fmap["id"].(string)
		if !ok {
			continue
		}
		sheetArr = append(sheetArr, fileID)

	}
	return sheetArr
}

// FindSpreadsheet finds a spreadsheet with `title`
func (m *SheetManager) FindSpreadsheet(title string) *sheets.Spreadsheet {
	sheetIDs := m.ListSpreadsheets()
	for _, sheetID := range sheetIDs {
		s := m.GetSpreadsheet(sheetID)
		if s.Properties.Title == title {
			return s
		}
	}
	return nil
}

/*
 * Database alias api
 */

// CreateDatabase creates a new database with the given database_file_`title`.
// Be careful, 'database_file_' tag is on the start of the title.
func (m *SheetManager) CreateDatabase(title string) *sheets.Spreadsheet {
	return m.CreateSpreadsheet(dbFileStart + title)
}

// FindDatabase gets a new database with the given database_file_`title`, if exists.
// If not existing, it will return nil.
// Be careful, 'database_file_' tag is on the start of the title finding for.
func (m *SheetManager) FindDatabase(title string) *sheets.Spreadsheet {
	return m.FindSpreadsheet(dbFileStart + title)
}

// DeleteDatabase deletes a database with the given database_file_`title`, if exists.
// If not existing, or failed to delete, it will log and return false.
// Be careful, 'database_file_' tag is implicitly on the start of the title finding for.
func (m *SheetManager) DeleteDatabase(title string) bool {
	db := m.FindDatabase(title)
	if db == nil {
		return false
	}
	return m.DeleteSpreadsheet(db.SpreadsheetId)
}

func (m *SheetManager) SyncDatabaseFromGoogle(db *sheets.Spreadsheet) *sheets.Spreadsheet {
	if db == nil {
		return nil
	}

	db = m.GetSpreadsheet(db.SpreadsheetId)
	if db == nil {
		fmt.Printf("[SheetManager]SyncDatabaseToGoogle: DB is nil")
		return nil
	}
	return db
}

type httpRequest interface {
	Header() http.Header
}

type httpMethod string

const (
	get    httpMethod = "GET"
	post   httpMethod = "POST"
	delete httpMethod = "DELETE"
	put    httpMethod = "PUT"
)

type httpURLRequest struct {
	req     *http.Request
	manager *SheetManager
}

func newURLRequest(manager *SheetManager, method httpMethod, url string) *httpURLRequest {
	req, err := http.NewRequest(string(method), url, nil)
	if err != nil {
		panic(err)
	}
	return &httpURLRequest{
		req:     req,
		manager: manager,
	}
}

// interface httpRequest
func (r *httpURLRequest) Header() http.Header {
	return r.req.Header
}

func (r *httpURLRequest) AddQuery(key, value string) *httpURLRequest {
	values := r.req.URL.Query()
	values.Add(key, value)
	r.req.URL.RawQuery = values.Encode()
	return r
}

func (r *httpURLRequest) Do() *http.Response {
	r.manager.RefreshToken()
	r.req.Header.Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	resp, err := r.manager.client.Do(r.req)
	if err != nil {
		panic(err)
	}
	return resp
}

type httpSpreadsheetCreateRequest struct {
	req     *sheets.SpreadsheetsCreateCall
	manager *SheetManager
}

func newSpreadsheetCreateRequest(manager *SheetManager, title string) *httpSpreadsheetCreateRequest {
	rb := &sheets.Spreadsheet{
		Properties: &sheets.SpreadsheetProperties{
			Title:      title,
			TimeZone:   "Asia/Seoul",
			AutoRecalc: "ON_CHANGE",
		},
	}
	req := manager.service.Spreadsheets.Create(rb)
	return &httpSpreadsheetCreateRequest{
		req:     req,
		manager: manager,
	}
}

// interface httpSpreadsheetCreateRequest
func (r *httpSpreadsheetCreateRequest) Header() http.Header {
	return r.req.Header()
}

func (r *httpSpreadsheetCreateRequest) Do() *sheets.Spreadsheet {
	r.manager.RefreshToken()
	r.req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	resp, err := r.req.Do()
	if err != nil {
		panic(err)
	}
	return resp
}

type httpBatchUpdateRequest struct {
	batchRequest  *sheets.BatchUpdateSpreadsheetRequest
	manager       *SheetManager
	spreadsheetID string
}

func newSpreadsheetBatchUpdateRequest(manager *SheetManager, spreadsheetID string, requests ...*sheets.Request) *httpBatchUpdateRequest {
	batchRequest := &sheets.BatchUpdateSpreadsheetRequest{}
	batchRequest.IncludeSpreadsheetInResponse = true
	batchRequest.Requests = requests
	return &httpBatchUpdateRequest{
		batchRequest:  batchRequest,
		manager:       manager,
		spreadsheetID: spreadsheetID,
	}
}

func (r *httpBatchUpdateRequest) updateRequest(req *sheets.Request) {
	if req == nil {
		return
	}
	r.batchRequest.Requests = append(r.batchRequest.Requests, req)
}

func (r *httpBatchUpdateRequest) Do() (*sheets.Spreadsheet, []*sheets.Response, int) {
	r.manager.RefreshToken()
	req := r.manager.service.Spreadsheets.BatchUpdate(r.spreadsheetID, r.batchRequest)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	resp, err := req.Do()
	if err != nil {
		panic(err)
	}
	return resp.UpdatedSpreadsheet, resp.Replies, resp.HTTPStatusCode
}

type httpValueRangeRequest struct {
	manager       *SheetManager
	ranges        string
	spreadsheetID string
}

const leftmostCol = "A"
const rightmostCol = "D"
const defaultRange = "A1C3"

func newSpreadsheetValuesRequest(manager *SheetManager, spreadsheetID, tableName string) *httpValueRangeRequest {
	return &httpValueRangeRequest{
		manager:       manager,
		ranges:        defaultRange,
		spreadsheetID: spreadsheetID,
	}
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

// updateRange includes end
func (r *httpValueRangeRequest) updateRange(tablename string, startRow, startCol, endRow, endCol int64) bool {
	if startRow < 0 || startRow >= endRow {
		return false
	}

	if startCol < 0 || startCol >= endCol {
		return false
	}

	r.manager.RefreshToken()

	var ranges string
	leftmost := base26(startCol + 1)
	rightmost := rightmostCol
	if endCol >= 2 {
		rightmost = base26(endCol + 1)
	}
	startRow++
	endRow++

	ranges = fmt.Sprintf("%s%d:%s%d", leftmost, startRow, rightmost, endRow)
	ranges = fmt.Sprintf("%s!%s", tablename, ranges)

	fmt.Println("Ranges: ", ranges)

	r.ranges = ranges
	return true
}

func (r *httpValueRangeRequest) Do() *sheets.ValueRange {
	r.manager.RefreshToken()
	req := r.manager.service.Spreadsheets.Values.Get(r.spreadsheetID, r.ranges)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	valueRange, err := req.Do()
	if err != nil {
		panic(err)
	}
	return valueRange
}

type httpUpdateValuesRequest struct {
	manager        *SheetManager
	ranges         string
	spreadsheetID  string
	updatingValues [][]interface{}
}

func newSpreadsheetValuesUpdateRequest(manager *SheetManager, spreadsheetID, tableName string) *httpUpdateValuesRequest {
	return &httpUpdateValuesRequest{
		manager:       manager,
		ranges:        defaultRange,
		spreadsheetID: spreadsheetID,
	}
}

func (r *httpUpdateValuesRequest) updateRange(metadata *TableMetadata, values []interface{}, appendData bool) bool {
	if len(values) == 0 {
		return false
	}
	startRow := 4
	if appendData {
		startRow += int(metadata.Rows)
	}
	endRow := startRow + len(values) - 1
	const startCol = 0
	endCol := startCol + len(metadata.Columns) - 1

	for i := range values {
		r.updatingValues = append(r.updatingValues, make([]interface{}, 0))
		reflected := reflect.ValueOf(values[i])
		for j := 0; j < len(metadata.Columns); j++ {
			r.updatingValues[i] = append(r.updatingValues[i], reflected.FieldByName(metadata.Columns[j]).Interface())
		}
	}

	var ranges string
	leftmost := base26(startCol + 1)
	rightmost := rightmostCol
	if endCol > 2 {
		rightmost = base26(int64(endCol + 1))
	}
	startRow++
	endRow++

	ranges = fmt.Sprintf("%s%d:%s%d", leftmost, startRow, rightmost, endRow)
	ranges = fmt.Sprintf("%s!%s", metadata.Name, ranges)

	r.ranges = ranges
	return true
}

func (r *httpUpdateValuesRequest) updateRows(metadata *TableMetadata, adding int) bool {
	unpackedValues := make([][]interface{}, 1)
	unpackedValues[0] = make([]interface{}, 1)
	unpackedValues[0][0] = metadata.Rows + int64(adding)

	r.ranges = fmt.Sprintf("%s!A3", metadata.Name)
	r.updatingValues = unpackedValues
	return true
}

func (r *httpUpdateValuesRequest) Do() int {
	r.manager.RefreshToken()
	rangeValues := &sheets.ValueRange{}
	rangeValues.Range = r.ranges
	rangeValues.Values = r.updatingValues
	req := r.manager.service.Spreadsheets.Values.Update(r.spreadsheetID, r.ranges, rangeValues)
	req.ValueInputOption("RAW")
	req.IncludeValuesInResponse(true)
	req.Header().Add("Authorization", "Bearer "+r.manager.token.AccessToken)
	updatedRange, err := req.Do()
	if err != nil {
		fmt.Println(r)
		panic(err)
	}
	return updatedRange.HTTPStatusCode
}
