// gcloud functions deploy main \
// --runtime go121 \
// --trigger-http \
// --entry-point=gdeltetl \
// --memory=128MB \
// --service-account=firebase-adminsdk-e7n2g@gdelt-433201.iam.gserviceaccount.com
package gdeltetl
// package main

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"sync"

	"google.golang.org/api/drive/v3"
	"google.golang.org/api/sheets/v4"
	"google.golang.org/api/option"
	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
)

func init() {
	functions.HTTP("gdeltetl", gdeltetlhttp)
 }
 
// helloHTTP is an HTTP Cloud Function with a request parameter.
// func helloHTTP(w http.ResponseWriter, r *http.Request) {
// var d struct {
// 	Name string `json:"name"`
// }
// if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
// 	fmt.Fprint(w, "Hello, World! Running GDELT ETL...")
// 	gdeltetl()
// 	return
// }
// if d.Name == "" {
// 	fmt.Fprint(w, "Hello, World! Running GDELT ETL...")
// 	gdeltetl()
// 	return
// }
// fmt.Fprintf(w, "Hello, %s!", html.EscapeString(d.Name), "Running GDELT ETL...")
// }

const (
	gdriveFolderID = "1QBYcTh8b3n0XBbPyr_VOrEbnTMQUtZC2"
	tmpPath        = "/tmp"
	// svcAcctPath    = "/Users/FYE7200/Documents/Personal/gdelt/gdelt-433201-351ecf8fcad7.json"
	// svcAcctPath    = "/projects/348469365843/secrets/gdelt-2"
	svcAcctPath    = "gdelt-2"
	projectID	   = "gdelt-433201"
	secretID	   = "348469365843"
	versionID	   = "1"
)

func accessSecretVersion(secretID string, versionID string) (string, error) {
	ctx := context.Background()

	// Create the Secret Manager client
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to create secretmanager client: %v", err)
	}
	defer client.Close()

	// Build the request
	name := fmt.Sprintf("projects/%s/secrets/%s/versions/%s", projectID, secretID, versionID)
	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: name,
	}

	// Access the secret
	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		return "", fmt.Errorf("failed to access secret version: %v", err)
	}

	// Return the secret payload
	secretData := string(result.Payload.Data)
	return secretData, nil
}

func downloadLatestGKGFile() ([][]string, error) {
	updateURL := "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

	// Get the URL of the latest file
	resp, err := http.Get(updateURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var latestFileURL string
	scanner := bufio.NewScanner(resp.Body)
	for i := 0; scanner.Scan(); i++ {
		if i == 2 {
			latestFileURL = strings.Split(scanner.Text(), " ")[2]
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}

	localZipPath := filepath.Join(tmpPath, "latest_gkg.zip")

	// Download the file
	out, err := os.Create(localZipPath)
	if err != nil {
		return nil, err
	}
	defer out.Close()

	resp, err = http.Get(latestFileURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	_, err = io.Copy(out, resp.Body)
	if err != nil {
		return nil, err
	}

	// Extract the zip file
	r, err := zip.OpenReader(localZipPath)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var csvFilePath string
	for _, f := range r.File {
		if strings.HasSuffix(strings.ToLower(f.Name), ".gkg.csv") {
			csvFilePath = filepath.Join(tmpPath, f.Name)
			outFile, err := os.OpenFile(csvFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return nil, err
			}
			rc, err := f.Open()
			if err != nil {
				return nil, err
			}
			_, err = io.Copy(outFile, rc)
			outFile.Close()
			rc.Close()
			if err != nil {
				return nil, err
			}
		}
	}

	// Read the CSV file
	file, err := os.Open(csvFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(bufio.NewReader(file))
	reader.LazyQuotes = true // Allow more lenient parsing of quotes
	reader.Comma = '\t'      // Change delimiter to tab
	data, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return data, nil
}

func findFileInGDrive(service *drive.Service, fileName string) (*drive.File, error) {
	query := fmt.Sprintf("name = '%s' and '%s' in parents and trashed = false", fileName, gdriveFolderID)
	fileList, err := service.Files.List().Q(query).Do()
	if err != nil {
		return nil, err
	}
	if len(fileList.Files) > 0 {
		return fileList.Files[0], nil
	}
	return nil, nil
}

func clearSheet(service *sheets.Service, spreadsheetID string) error {
	clearRange := &sheets.ClearValuesRequest{}
	_, err := service.Spreadsheets.Values.Clear(spreadsheetID, "Sheet1", clearRange).Do()
	if err != nil {
		return err
	}
	return nil
}

func uploadToGSheets(service *sheets.Service, spreadsheetID string, data [][]string) error {
	valueRange := &sheets.ValueRange{
		Values: [][]interface{}{},
	}

	for _, row := range data {
		rowData := []interface{}{}
		for _, cell := range row {
			rowData = append(rowData, cell)
		}
		valueRange.Values = append(valueRange.Values, rowData)
	}

	_, err := service.Spreadsheets.Values.Update(spreadsheetID, "Sheet1", valueRange).ValueInputOption("RAW").Do()
	return err
}

func uploadOrUpdateGDrive(serviceDrive *drive.Service, serviceSheets *sheets.Service, data [][]string, fileName string) error {
	existingFile, err := findFileInGDrive(serviceDrive, fileName)
	if err != nil {
		return err
	}

	if existingFile != nil {
		err = clearSheet(serviceSheets, existingFile.Id)
		if err != nil {
			return err
		}
		err = uploadToGSheets(serviceSheets, existingFile.Id, data)
		if err != nil {
			return err
		}
		fmt.Printf("Updated file: %s, File ID: %s\n", fileName, existingFile.Id)
	} else {
		file := &drive.File{
			Name:     fileName,
			MimeType: "application/vnd.google-apps.spreadsheet",
			Parents:  []string{gdriveFolderID},
		}
		createdFile, err := serviceDrive.Files.Create(file).Do()
		if err != nil {
			return err
		}
		err = uploadToGSheets(serviceSheets, createdFile.Id, data)
		if err != nil {
			return err
		}
		fmt.Printf("Uploaded new file: %s, File ID: %s\n", fileName, createdFile.Id)
	}

	return nil
}

func dropDuplicates(data [][]string) [][]string {
	uniqueMap := make(map[string]bool)
	var uniqueData [][]string

	for _, row := range data {
		// Join row into a string to use as a map key
		rowKey := fmt.Sprintf("%v", row)
		if _, exists := uniqueMap[rowKey]; !exists {
			uniqueMap[rowKey] = true
			uniqueData = append(uniqueData, row)
		}
	}

	return uniqueData
}

func dropNa(data [][]string) [][]string {
	var cleanedData [][]string

	for _, row := range data {
		keepRow := true
		for _, value := range row {
			if value == "" { // Check for empty strings (or nil if working with pointers)
				keepRow = false
				break
			}
		}
		if keepRow {
			cleanedData = append(cleanedData, row)
		}
	}

	return cleanedData
}

func fillNa(data [][]string) [][]string {
	for i, row := range data {
		for j, value := range row {
			if value == "" {
				data[i][j] = "Null"
			}
		}
	}
	return data
}


// Function to process and filter the data into different tables (like format_tone, format_col in Python)
func processAndFilterData(data [][]string) ([][]string, [][]string, [][]string, [][]string, [][]string) {
	var gdeltMain, gdeltLocs, gdeltPersons, gdeltOrgs, gdeltThemes [][]string

	// Add headers
	gdeltMain = append(gdeltMain, []string{"GKGRecordID", "Date", "SourceID", "SourceCommonName", "DocumentID", "V2SharingImage", "V15Tone", "OverallTone", "PosTone", "NegTone", "TonePolarity", "ToneActivityRefDensity", "ToneSelfGroupRefDensity", "ToneWordCount"})
	gdeltLocs = append(gdeltLocs, []string{"GKGRecordID", "V2Locations", "LocationTypeCode", "LocationFullName", "LocationCountryCode", "LocationADM1Code1", "LocationADM1Code2", "LocationLatitude", "LocationLongitude", "LocationFeatureID", "TextLocation"})
	gdeltPersons = append(gdeltPersons, []string{"GKGRecordID", "Person"})
	gdeltOrgs = append(gdeltOrgs, []string{"GKGRecordID", "Organization"})
	gdeltThemes = append(gdeltThemes, []string{"GKGRecordID", "Theme"})

	// Process rows and split into respective slices
	for _, row := range data[1:] { // Skip header row
		// Process the main table
		tones := strings.Split(row[15], ",")
		gdeltMain = append(gdeltMain, []string{
			row[0], row[1], row[2], row[3], row[4], row[18], row[15], 
			tones[0], tones[1], tones[2], tones[3], tones[4], tones[5], tones[6]})

		// Split fields that use ';' and '#'
		if len(row[10]) > 1 {
			locations := strings.Split(row[10], ";")
			for _, loc := range locations {
				locs := strings.Split(loc, "#")
				gdeltLocs = append(gdeltLocs, []string{
					row[0], row[10],
					locs[0], locs[1], locs[2],locs[3],locs[4],locs[5],locs[6],locs[7],locs[8]})
			}
		}
		
		if len(row[12]) > 1 {
			persons := strings.Split(row[12], ";")
			for _, person := range persons {
				person1 := strings.Split(person, ",")
				gdeltPersons = append(gdeltPersons, []string{row[0], person1[0]})
			}
		}

		if len(row[14]) > 1 {
			orgs := strings.Split(row[14], ";")
			for _, org := range orgs {
				org1 := strings.Split(org, ",")
				gdeltOrgs = append(gdeltOrgs, []string{row[0], org1[0]})
			}
		}

		if len(row[8]) > 1 {
			themes := strings.Split(row[8], ";")
			for _, theme := range themes {
				theme1 := strings.Split(theme, ",")
				gdeltThemes = append(gdeltThemes, []string{row[0], theme1[0]})
			}
		}
	}

	return gdeltMain, gdeltLocs, gdeltPersons, gdeltOrgs, gdeltThemes
}

func gdeltetlhttp(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "ETL process started")
	gdeltetl()
}

func gdeltetl() {
	// fmt.Println("ETL process started")
	start := time.Now()
	ctx := context.Background()
	secret, err := accessSecretVersion(secretID, versionID)
	clientOption := option.WithCredentialsFile(secret)

	driveService, err := drive.NewService(ctx, clientOption)
	if err != nil {
		fmt.Println("Error creating Drive client:", err)
		return
	}

	sheetsService, err := sheets.NewService(ctx, clientOption)
	if err != nil {
		fmt.Println("Error creating Sheets client:", err)
		return
	}

	// Download and process the latest GKG file
	data, err := downloadLatestGKGFile()
	if err != nil {
		fmt.Println("Error downloading or processing GKG file:", err)
		return
	}


	// Split data into different tables
	gdeltMain, gdeltLocs, gdeltPersons, gdeltOrgs, gdeltThemes := processAndFilterData(data)

	var wg sync.WaitGroup
	errChan := make(chan error, 5)

	uploadFuncs := []struct {
		data     [][]string
		fileName string
	}{
		{dropNa(gdeltMain), "gdelt_main"},
		{dropDuplicates(fillNa(gdeltLocs)), "gdelt_locs"},
		{dropDuplicates(gdeltPersons), "gdelt_persons"},
		{dropDuplicates(gdeltOrgs), "gdelt_orgs"},
		{dropDuplicates(dropNa(gdeltThemes)), "gdelt_themes"},
	}

	for _, uf := range uploadFuncs {
		wg.Add(1)
		go func(uf struct {
			data     [][]string
			fileName string
		}) {
			defer wg.Done()
			if err := uploadOrUpdateGDrive(driveService, sheetsService, uf.data, uf.fileName); err != nil {
				errChan <- err
			}
		}(uf)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			fmt.Println("Error during upload:", err)
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("Total execution time: %s\n", elapsed)
	return
}
