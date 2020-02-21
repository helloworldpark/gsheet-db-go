package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

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
func (m *SheetManager) RefreshToken() bool {
	if m.token.Valid() {
		return false
	}
	token := CreateJWTToken(m.credentialJSON)
	m.token = token
	return true
}

// CreateSpreadsheet creates a single spreadsheet file
func (m *SheetManager) CreateSpreadsheet(title string) *sheets.Spreadsheet {
	// check duplicated title
	if m.FindSpreadsheet(title) != nil {
		return nil
	}

	rb := &sheets.Spreadsheet{
		Properties: &sheets.SpreadsheetProperties{
			Title:      title,
			TimeZone:   "Asia/Seoul",
			AutoRecalc: "ON_CHANGE",
		},
	}
	req := m.service.Spreadsheets.Create(rb)
	req.Header().Add("Authorization", "Bearer "+m.token.AccessToken)
	resp, err := req.Do()
	if err != nil {
		panic(err)
	}

	return resp
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
	req, err := http.NewRequest("DELETE", fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s", spreadsheetID), nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add("Authorization", "Bearer "+m.token.AccessToken)
	resp, err := m.client.Do(req)
	if err != nil {
		panic(err)
	}
	return resp.StatusCode/100 == 2
}

// ListSpreadsheets lists spreadsheets' id by []string
func (m *SheetManager) ListSpreadsheets() []string {
	req, err := http.NewRequest("GET", "https://www.googleapis.com/drive/v3/files", nil)
	if err != nil {
		panic(err)
	}
	req.Header.Add("Authorization", "Bearer "+m.token.AccessToken)
	// https://stackoverflow.com/questions/30652577/go-doing-a-get-request-and-building-the-querystring
	// https://developers.google.com/drive/api/v3/mime-types
	shouldBe := "mimeType='application/vnd.google-apps.spreadsheet'"
	values := req.URL.Query()
	values.Add("q", shouldBe)
	req.URL.RawQuery = values.Encode()

	resp, err := m.client.Do(req)
	if err != nil {
		panic(err)
	}
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