package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

func ListSharedDrives() []string {
	resp, err := http.DefaultClient.Get("https://www.googleapis.com/drive/v3/drives")
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	var jsonBody interface{}
	json.Unmarshal(body, &jsonBody)

	keyJson, ok := jsonBody.(map[string]interface{})
	if !ok {
		return nil
	}
	if _, ok = keyJson["drives"]; !ok {
		return nil
	}
	rawDrives, ok := keyJson["drives"].([]interface{})
	if !ok {
		return nil
	}

	var driveName []string
	for _, drive := range rawDrives {
		formattedDrive, ok := drive.(map[string]interface{})
		if !ok {
			continue
		}
		driveName = append(driveName, formattedDrive["name"].(string))
	}

	return driveName
}
