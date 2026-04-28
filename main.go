package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/open-runtimes/types-for-go/v4/openruntimes"
)

// Constants
const (
	DatabaseId         = "69cbdded00392d03962c"
	MaintenanceCol     = "maintenance" // Collection where tasks are created
	PrintersCollection = "printers"
	ControlCollection  = "no" // The collection holding the "no" column
)

// ---------------------------------------------------------
// PURE API IMPLEMENTATION TO AVOID ALL SDK VERSION CRASHES
// ---------------------------------------------------------
type AppwriteAPI struct {
	Endpoint  string
	ProjectID string
	APIKey    string
	Client    *http.Client
}

func NewAppwriteAPI() *AppwriteAPI {
	return &AppwriteAPI{
		Endpoint:  os.Getenv("APPWRITE_ENDPOINT"),
		ProjectID: os.Getenv("APPWRITE_PROJECT_ID"),
		APIKey:    os.Getenv("APPWRITE_API_KEY"),
		Client:    &http.Client{Timeout: 15 * time.Second},
	}
}

func (api *AppwriteAPI) req(method, path string, body interface{}) ([]byte, error) {
	var bodyReader io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		bodyReader = bytes.NewReader(b)
	}

	url := fmt.Sprintf("%s%s", api.Endpoint, path)
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Appwrite-Project", api.ProjectID)
	req.Header.Set("X-Appwrite-Key", api.APIKey)

	res, err := api.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		respBody, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("appwrite api error: %d - %s", res.StatusCode, string(respBody))
	}

	return io.ReadAll(res.Body)
}

func (api *AppwriteAPI) ListDocuments(dbId, colId string) ([]map[string]interface{}, error) {
	path := fmt.Sprintf("/databases/%s/collections/%s/documents", dbId, colId)
	b, err := api.req("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var res map[string]interface{}
	json.Unmarshal(b, &res)

	docsRaw, ok := res["documents"].([]interface{})
	if !ok {
		return nil, nil
	}

	var docs []map[string]interface{}
	for _, raw := range docsRaw {
		if m, ok := raw.(map[string]interface{}); ok {
			docs = append(docs, m)
		}
	}
	return docs, nil
}

func (api *AppwriteAPI) CreateDocument(dbId, colId, docId string, data map[string]interface{}) (map[string]interface{}, error) {
	path := fmt.Sprintf("/databases/%s/collections/%s/documents", dbId, colId)
	payload := map[string]interface{}{
		"documentId": docId,
		"data":       data,
	}
	b, err := api.req("POST", path, payload)
	if err != nil {
		return nil, err
	}

	var res map[string]interface{}
	json.Unmarshal(b, &res)
	return res, nil
}

// ---------------------------------------------------------
// GENERATOR LOGIC
// ---------------------------------------------------------

func GenerateRandomTask(Context openruntimes.Context, api *AppwriteAPI) {
	// 1. Get Printers
	printers, err := api.ListDocuments(DatabaseId, PrintersCollection)
	if err != nil || len(printers) == 0 {
		Context.Log("Failed to fetch printers or no printers available.")
		return
	}

	// 2. Pick a random printer
	rand.Seed(time.Now().UnixNano())
	p := printers[rand.Intn(len(printers))]
	printerId, ok := p["$id"].(string)
	if !ok {
		return
	}

	// 3. Pick a random error type (only "No something" as requested)
	errors := []string{
		"No Paper",
		"No toner ink",
		"No power",
		"No connection",
		"No response",
	}
	errType := errors[rand.Intn(len(errors))]

	// 4. Create Task mapping exactly to the CSV structure
	taskData := map[string]interface{}{
		"printer_id":   printerId,
		"error_type":   errType,
		"startTime":    time.Now().UTC().Format(time.RFC3339),
		"printerFixed": false,
	}

	_, createErr := api.CreateDocument(DatabaseId, MaintenanceCol, "unique()", taskData)
	if createErr != nil {
		Context.Log("Failed to create mock task: " + createErr.Error())
	} else {
		Context.Log(fmt.Sprintf("Created new mock task: [%s] for printer [%s]", errType, printerId))
	}
}

// ---------------------------------------------------------
// ENTRY POINT
// ---------------------------------------------------------

func Main(Context openruntimes.Context) openruntimes.Response {
	api := NewAppwriteAPI()
	Context.Log("Starting Mock Task Generator Backend...")

	for {
		// 1. Fetch the interval from the control collection
		docs, err := api.ListDocuments(DatabaseId, ControlCollection)

		interval := 0
		if err == nil && len(docs) > 0 {
			// Get the number from the column
			if val, ok := docs[0]["no"].(float64); ok {
				interval = int(val)
			} else if val, ok := docs[0]["no"].(int); ok {
				interval = val
			}
		}

		if interval <= 0 {
			// If 0, missing, or deleted -> Pause generation silently to avoid log flooding
			time.Sleep(10 * time.Second)
			continue
		}

		// 2. Sleep for the specified interval (e.g., 30 seconds)
		time.Sleep(time.Duration(interval) * time.Second)

		// 3. Generate a new task
		GenerateRandomTask(Context, api)
	}

	return Context.Res.Json(map[string]interface{}{"status": "Loop ended"})
}
