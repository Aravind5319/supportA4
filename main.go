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
		Context.Log("Waiting for printers to be available...")
		return
	}

	// 2. Pick a random printer safely
	p := printers[rand.Intn(len(printers))]
	printerId, ok := p["$id"].(string)
	if !ok || printerId == "" {
		return
	}

	// Exact error types provided by the user
	errorsList := []string{
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
	errType := errorsList[rand.Intn(len(errorsList))]

	// 4. Create Task mapping exactly to the CSV structure
	taskData := map[string]interface{}{
		"printer_id":   printerId,
		"error_type":   errType,
		"startTime":    time.Now().UTC().Format(time.RFC3339),
		"printerFixed": false,
	}

	_, createErr := api.CreateDocument(DatabaseId, MaintenanceCol, "unique()", taskData)
	if createErr == nil {
		Context.Log(fmt.Sprintf("Created task: [%s] for printer [%s]", errType, printerId))
	}
}

// ---------------------------------------------------------
// ENTRY POINT
// ---------------------------------------------------------

func Main(Context openruntimes.Context) openruntimes.Response {
	api := NewAppwriteAPI()
	Context.Log("Starting continuous task generator...")

	// Prevent any unhandled panics from crashing the function
	defer func() {
		if r := recover(); r != nil {
			Context.Log(fmt.Sprintf("Recovered from panic: %v", r))
		}
	}()

	// Track when the function started
	startTime := time.Now()

	for {
		// Appwrite hard-kills functions at 15 minutes (900 seconds).
		// We gracefully exit at 14.5 minutes (870 seconds) so you get a Green "Completed" status!
		if time.Since(startTime).Seconds() > 870 {
			Context.Log("Reached 14.5 minutes. Exiting cleanly to avoid a red Timeout error.")
			break
		}

		// 1. Fetch the interval from the control collection
		docs, err := api.ListDocuments(DatabaseId, ControlCollection)
		
		interval := 0
		if err == nil && len(docs) > 0 {
			if val, ok := docs[0]["NO."].(float64); ok {
				interval = int(val)
			} else if val, ok := docs[0]["NO."].(int); ok {
				interval = val
			} else if valStr, ok := docs[0]["NO."].(string); ok {
				fmt.Sscanf(valStr, "%d", &interval)
			}
		}

		if interval <= 0 {
			// If missing or 0, just wait 5 seconds and check again
			time.Sleep(5 * time.Second)
			continue
		}

		// 2. Sleep for the specified interval (e.g., 60 seconds)
		time.Sleep(time.Duration(interval) * time.Second)

		// 3. Generate a new task
		GenerateRandomTask(Context, api)
	}

	return Context.Res.Json(map[string]interface{}{"status": "Success", "message": "Run finished cleanly."})
}
