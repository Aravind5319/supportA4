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
	ControlCollection  = "history_collection" // The collection holding the "no" column
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
		return
	}

	// 2. Pick a random printer safely
	p := printers[rand.Intn(len(printers))]
	printerId, ok := p["$id"].(string)
	if !ok || printerId == "" {
		return
	}

	// 3. Define the specific error types
	errorTypes := []string{
		"No paper",
		"Service Requested",
		"Jammed",
		"Paper Jam",
		"Door Opened",
		"No toner ink",
		"No Toner",
		"Printer Offline",
		"Offline",
		"Low paper",
	}

	selectedError := errorTypes[rand.Intn(len(errorTypes))]

	// 4. Create Task mapping - EXACTLY as requested (no color, employee, priority, etc)
	taskData := map[string]interface{}{
		"printer_id":   printerId,
		"error_type":   selectedError,
		"printerFixed": false,
	}

	_, createErr := api.CreateDocument(DatabaseId, MaintenanceCol, "unique()", taskData)
	if createErr == nil {
		Context.Log(fmt.Sprintf("Created new mock task: [%s] for printer [%s]", selectedError, printerId))
	} else {
		Context.Log(fmt.Sprintf("API Error while creating task: %v", createErr))
	}
}

// ---------------------------------------------------------
// ENTRY POINT
// ---------------------------------------------------------

func Main(Context openruntimes.Context) openruntimes.Response {
	api := NewAppwriteAPI()
	Context.Log("Starting Mock Task Generator Backend...")

	// Prevent any unhandled panics
	defer func() {
		if r := recover(); r != nil {
			Context.Log(fmt.Sprintf("Recovered from panic: %v", r))
		}
	}()

	for {
		// 1. Read the DB to get the live number (interval)
		docs, err := api.ListDocuments(DatabaseId, ControlCollection)

		interval := 0
		if err == nil && len(docs) > 0 {
			doc := docs[0]
			var val interface{}
			
			// Check for 'NO.' or 'no'
			if v, ok := doc["NO."]; ok { val = v } else 
			if v, ok := doc["no"]; ok { val = v } else 
			if v, ok := doc["No"]; ok { val = v } else 
			if v, ok := doc["NO"]; ok { val = v } else 
			if v, ok := doc["interval"]; ok { val = v }

			if val != nil {
				if v, ok := val.(float64); ok { interval = int(v) } else 
				if v, ok := val.(int); ok { interval = v } else 
				if vStr, ok := val.(string); ok { fmt.Sscanf(vStr, "%d", &interval) }
			}
		}

		// 2. If interval is 0 or missing, just PAUSE and check again soon
		if interval <= 0 {
			Context.Log("Interval is 0 (or not found). Pausing for 5 seconds and checking DB again...")
			time.Sleep(5 * time.Second)
			continue
		}

		// 3. Create ONE random task and save it in maintenance DB
		GenerateRandomTask(Context, api)

		// 4. Wait for the exact seconds defined in the DB
		Context.Log(fmt.Sprintf("Task created! Waiting for %d seconds...", interval))
		time.Sleep(time.Duration(interval) * time.Second)
		
		// 5. Continues the infinite loop!
	}

	return Context.Res.Json(map[string]interface{}{"status": "Success", "message": "Done."})
}
