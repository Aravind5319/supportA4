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

type TaskDef struct {
	Type     string
	Priority int
	Label    string
	Color    string
}

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

	// 3. Define the specific tasks requested by the user
	taskDefs := []TaskDef{
		{Type: "No paper", Priority: 1, Label: "🚨 CRITICAL", Color: "High"},
		{Type: "Service Requested", Priority: 2, Label: "⚠️ HIGH", Color: "High"},
		{Type: "Jammed", Priority: 3, Label: "🟠 JAMMED", Color: "Orange"},
		{Type: "Paper Jam", Priority: 3, Label: "🟠 JAMMED", Color: "Orange"},
		{Type: "Door Opened", Priority: 4, Label: "⚡ IMMEDIATE", Color: "Orange"},
		{Type: "No toner ink", Priority: 5, Label: "🔵 CRITICAL", Color: "Blue"},
		{Type: "No Toner", Priority: 5, Label: "🔵 CRITICAL", Color: "Blue"},
		{Type: "Printer Offline", Priority: 5, Label: "🔵 CRITICAL", Color: "Blue"},
		{Type: "Offline", Priority: 5, Label: "🔵 CRITICAL", Color: "Blue"},
		{Type: "Low paper", Priority: 6, Label: "✅ READY", Color: "Yellow"},
	}

	selected := taskDefs[rand.Intn(len(taskDefs))]

	// 4. Create Task mapping
	taskData := map[string]interface{}{
		"printer_id":   printerId,
		"error_type":   selected.Type,
		"startTime":    time.Now().UTC().Format(time.RFC3339),
		"printerFixed": false,
		"priority":     selected.Priority,
		"label":        selected.Label,
		"color":        selected.Color,
	}

	_, createErr := api.CreateDocument(DatabaseId, MaintenanceCol, "unique()", taskData)
	if createErr == nil {
		Context.Log(fmt.Sprintf("Created new mock task: [%s] for printer [%s]", selected.Type, printerId))
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

	// Prevent any unhandled panics from crashing the function
	defer func() {
		if r := recover(); r != nil {
			Context.Log(fmt.Sprintf("Recovered from panic: %v", r))
		}
	}()

	for {
		// 1. Fetch the interval from the control collection
		docs, err := api.ListDocuments(DatabaseId, ControlCollection)

		interval := 0
		if err == nil && len(docs) > 0 {
			// Safely extract the number
			if val, ok := docs[0]["NO."].(float64); ok {
				interval = int(val)
			} else if val, ok := docs[0]["NO."].(int); ok {
				interval = val
			} else if valStr, ok := docs[0]["NO."].(string); ok {
				fmt.Sscanf(valStr, "%d", &interval)
			}
		}

		// If the user sets it to 0, EXIT the function gracefully
		if interval <= 0 {
			Context.Log("Interval is 0. Exiting generator.")
			break
		}

		// 2. Sleep for the specified interval (e.g., 60 seconds)
		time.Sleep(time.Duration(interval) * time.Second)

		// 3. Generate exactly ONE task
		GenerateRandomTask(Context, api)
	}

	return Context.Res.Json(map[string]interface{}{"status": "Success", "message": "Generator stopped because interval was set to 0."})
}
