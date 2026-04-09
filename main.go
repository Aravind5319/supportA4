package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/open-runtimes/types-for-go/v4/openruntimes"
)

// Constants
const (
	DatabaseId         = "69cbdded00392d03962c"
	TasksCollection    = "tasks_collection"
	PrintersCollection = "printers"
	UsersCollection    = "users_collection"
	HistoryCollection  = "history_collection"
)

// Data Structures
type Task struct {
	Id         string   `json:"$id,omitempty"`
	PrinterId  string   `json:"printerId"`
	EmployeeId string   `json:"employeeId"`
	Issues     []string `json:"issues"`
	Priority   string   `json:"priority"`
	CreatedAt  string   `json:"createdAt"`
	Deadline   string   `json:"deadline"`
	Status     string   `json:"status"`
	Shared     bool     `json:"shared"`
}

type User struct {
	Id              string  `json:"$id,omitempty"`
	Name            string  `json:"name"`
	Email           string  `json:"email"`
	Role            string  `json:"role"`
	FCMToken        string  `json:"fcmToken,omitempty"`
	TotalTasks      int     `json:"totalTasks"`
	AvgResponseTime int     `json:"avgResponseTime"`
	SuccessRate     float64 `json:"successRate"`
}

// Logic Helpers
func DeterminePriority(issues []string) string {
	for _, issue := range issues {
		if issue == "Paper Jam" || issue == "No Paper" {
			return "HIGH"
		}
	}
	for _, issue := range issues {
		if issue == "Ink Low" || issue == "Low Paper" {
			return "MEDIUM"
		}
	}
	return "LOW"
}

func DetermineDeadline(priority string, from time.Time) string {
	var deadline time.Time
	switch priority {
	case "HIGH":
		deadline = from.Add(5 * time.Minute)
	case "MEDIUM":
		deadline = from.Add(15 * time.Minute)
	case "LOW":
		deadline = from.Add(30 * time.Minute)
	default:
		deadline = from.Add(30 * time.Minute)
	}
	return deadline.UTC().Format(time.RFC3339)
}

func SortTasks(tasks []Task) {
	priorityWeight := map[string]int{
		"HIGH":   3,
		"MEDIUM": 2,
		"LOW":    1,
	}

	sort.Slice(tasks, func(i, j int) bool {
		pi := priorityWeight[tasks[i].Priority]
		pj := priorityWeight[tasks[j].Priority]
		if pi != pj {
			return pi > pj
		}

		di, _ := time.Parse(time.RFC3339, tasks[i].Deadline)
		dj, _ := time.Parse(time.RFC3339, tasks[j].Deadline)
		if !di.Equal(dj) {
			return di.Before(dj)
		}

		ci, _ := time.Parse(time.RFC3339, tasks[i].CreatedAt)
		cj, _ := time.Parse(time.RFC3339, tasks[j].CreatedAt)
		return ci.Before(cj)
	})
}

func ParseBody(body interface{}, target interface{}) error {
	if body == nil {
		return fmt.Errorf("request body is empty")
	}

	// Handle case where body is a getter function (common in some runtimes)
	if getter, ok := body.(func() interface{}); ok {
		body = getter()
	}

	var bodyBytes []byte
	var err error

	switch v := body.(type) {
	case string:
		bodyBytes = []byte(v)
	case []byte:
		bodyBytes = v
	default:
		// Attempt to marshal maps or other objects back to JSON
		bodyBytes, err = json.Marshal(v)
		if err != nil {
			return fmt.Errorf("failed to marshal body object: %v", err)
		}
	}

	err = json.Unmarshal(bodyBytes, target)
	if err != nil {
		return fmt.Errorf("failed to unmarshal JSON: %v (Raw: %s)", err, string(bodyBytes))
	}
	return nil
}

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

func (api *AppwriteAPI) GetDocument(dbId, colId, docId string) (map[string]interface{}, error) {
	path := fmt.Sprintf("/databases/%s/collections/%s/documents/%s", dbId, colId, docId)
	b, err := api.req("GET", path, nil)
	if err != nil {
		return nil, err
	}

	var res map[string]interface{}
	json.Unmarshal(b, &res)
	return res, nil
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

func (api *AppwriteAPI) UpdateDocument(dbId, colId, docId string, data map[string]interface{}) (map[string]interface{}, error) {
	path := fmt.Sprintf("/databases/%s/collections/%s/documents/%s", dbId, colId, docId)
	payload := map[string]interface{}{
		"data": data,
	}
	b, err := api.req("PATCH", path, payload)
	if err != nil {
		return nil, err
	}

	var res map[string]interface{}
	json.Unmarshal(b, &res)
	return res, nil
}

// target registration (Required for Appwrite Messaging Push)
func (api *AppwriteAPI) CreateTarget(userId, providerId, identifier string) error {
	path := fmt.Sprintf("/users/%s/targets", userId)
	payload := map[string]interface{}{
		"targetId":     "unique()",
		"providerId":   providerId,
		"providerType": "push",
		"identifier":   identifier,
	}
	_, err := api.req("POST", path, payload)
	return err
}

func (api *AppwriteAPI) SendPushNotification(userIds []string, title, body string) error {
	if len(userIds) == 0 {
		return nil
	}
	path := "/messaging/messages/push"
	payload := map[string]interface{}{
		"messageId": "unique()",
		"title":     title,
		"body":      body,    // Compatibility
		"content":   body,    // Appwrite 1.5+
		"users":     userIds,
	}
	_, err := api.req("POST", path, payload)
	return err
}

// ---------------------------------------------------------
// ROUTE HANDLERS
// ---------------------------------------------------------

func GetTasks(Context openruntimes.Context, api *AppwriteAPI) openruntimes.Response {
	response, err := api.ListDocuments(DatabaseId, TasksCollection)
	if err != nil {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": err.Error()})
	}

	var tasks []Task
	for _, rawDoc := range response {
		b, _ := json.Marshal(rawDoc)
		var t Task
		json.Unmarshal(b, &t)
		if t.Status == "ACTIVE" || t.Status == "ongoing" {
			tasks = append(tasks, t)
		}
	}
	SortTasks(tasks)
	return Context.Res.Json(map[string]interface{}{"success": true, "data": tasks})
}

func CreateTask(Context openruntimes.Context, api *AppwriteAPI) openruntimes.Response {
	var payload struct {
		PrinterId string   `json:"printerId"`
		Issues    []string `json:"issues"`
	}
	
	if err := ParseBody(Context.Req.Body, &payload); err != nil {
		Context.Log("ParseBody error: " + err.Error())
		return Context.Res.Json(map[string]interface{}{"success": false, "error": "Invalid request body: " + err.Error()})
	}

	now := time.Now()
	priority := DeterminePriority(payload.Issues)
	deadline := DetermineDeadline(priority, now)

	// Combine issues into a single string to match Appwrite schema (String, not array)
	combinedIssues := strings.Join(payload.Issues, ", ")

	taskData := map[string]interface{}{
		"printerId":  payload.PrinterId,
		"issues":     combinedIssues,
		"priority":   priority,
		"createdAt":  now.UTC().Format(time.RFC3339),
		"deadline":   deadline,
		"status":     "ACTIVE",
		"shared":     false,
	}

	doc, err := api.CreateDocument(DatabaseId, TasksCollection, "unique()", taskData)
	if err != nil {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": err.Error()})
	}

	// FIRE ALERT IF HIGH PRIORITY
	if priority == "HIGH" {
		Context.Log("High priority task detected! Sending notifications...")
		usersRaw, _ := api.ListDocuments(DatabaseId, UsersCollection)
		var targetIds []string
		for _, u := range usersRaw {
			// Now that the Appwrite ID is successfully bound to the token target, we harvest the Auth Account ID!
			if tok, ok := u["$id"].(string); ok && tok != "" && tok != "NULL" {
				// We actually need the Appwrite Account ID which we just mapped into the database!
				// Wait! We passed session.userId to `saveToken`!
				// So `saveToken` in Go Backend literally created the Document in `users_collection` using the `session.userId` as the Document ID!!
				targetIds = append(targetIds, tok)
			}
		}
		
		if len(targetIds) > 0 {
			err := api.SendPushNotification(targetIds, "🚨 High Priority Alarm", "Urgent printer issue: " + combinedIssues)
			if err != nil {
				Context.Log("PUSH ERROR (CreateTask): " + err.Error())
			} else {
				Context.Log(fmt.Sprintf("PUSH SUCCESS! Broadcasted alert to %d potential technicians.", len(targetIds)))
			}
		}
	}

	return Context.Res.Json(map[string]interface{}{"success": true, "data": doc})
}

func SaveToken(Context openruntimes.Context, api *AppwriteAPI) openruntimes.Response {
	var payload struct {
		UserId   string `json:"userId"`
		FCMToken string `json:"fcmToken"`
	}
	if err := ParseBody(Context.Req.Body, &payload); err != nil {
		Context.Log("ParseBody error: " + err.Error())
		return Context.Res.Json(map[string]interface{}{"success": false, "error": "Invalid body: " + err.Error()})
	}

	if payload.UserId == "" || payload.FCMToken == "" {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": "userId and fcmToken are required"})
	}

	// 1. Register as Appwrite Target using the Legit Provider ID from Dashboard
	err := api.CreateTarget(payload.UserId, "69d4d2ce0027660c1fe2", payload.FCMToken)
	if err != nil {
		Context.Log("Target Registration Warning: " + err.Error())
	}

	// 2. Store in Database (Upsert: Update if exists, Create if not)
	updateData := map[string]interface{}{
		"fcmToken": payload.FCMToken,
	}

	_, err = api.UpdateDocument(DatabaseId, UsersCollection, payload.UserId, updateData)
	if err != nil {
		Context.Log("User document not found or error, attempting to create: " + err.Error())
		
		// If update fails, try creating the document (using the same UserId as Document ID)
		createData := map[string]interface{}{
			"fcmToken": payload.FCMToken,
			"role": "technician", // Default role for new auto-created technicians
		}
		_, err = api.CreateDocument(DatabaseId, UsersCollection, payload.UserId, createData)
		if err != nil {
			Context.Log("Critical database Error: " + err.Error())
			return Context.Res.Json(map[string]interface{}{"success": false, "error": "Failed to save token to database: " + err.Error()})
		}
		Context.Log("Successfully created new user document with token")
	} else {
		Context.Log("Successfully updated existing user document with token")
	}

	return Context.Res.Json(map[string]interface{}{"success": true})
}

func CompleteTaskByPath(Context openruntimes.Context, api *AppwriteAPI, taskId string) openruntimes.Response {
    var payload struct {
        EmployeeId string `json:"employeeId"`
    }
    _ = ParseBody(Context.Req.Body, &payload) 

    taskMap, err := api.GetDocument(DatabaseId, TasksCollection, taskId)
    if err != nil {
        return Context.Res.Json(map[string]interface{}{"success": false, "error": "Task not found"})
    }

    var task Task
    b, _ := json.Marshal(taskMap)
    json.Unmarshal(b, &task)

    createdAt, _ := time.Parse(time.RFC3339, task.CreatedAt)
    resolvedAt := time.Now()
    timeTaken := int(resolvedAt.Sub(createdAt).Minutes())

    historyData := map[string]interface{}{
        "taskId":     task.Id,
        "employeeId": payload.EmployeeId,
        "printerId":  task.PrinterId,
        "issues":     task.Issues,
        "resolvedAt": resolvedAt.UTC().Format(time.RFC3339),
        "timeTaken":  timeTaken,
    }
    
    api.CreateDocument(DatabaseId, HistoryCollection, "unique()", historyData)
    _, err = api.UpdateDocument(DatabaseId, TasksCollection, task.Id, map[string]interface{}{"status": "DONE"})
    
    if err != nil {
        return Context.Res.Json(map[string]interface{}{"success": false, "error": "Failed to update task status"})
    }

    if payload.EmployeeId != "" {
        userMap, err := api.GetDocument(DatabaseId, UsersCollection, payload.EmployeeId)
        if err == nil {
            var user User
            b, _ := json.Marshal(userMap)
            json.Unmarshal(b, &user)

            newTotal := user.TotalTasks + 1
            newAvg := ((user.AvgResponseTime * user.TotalTasks) + timeTaken) / newTotal

            api.UpdateDocument(DatabaseId, UsersCollection, payload.EmployeeId, map[string]interface{}{
                "totalTasks":      newTotal,
                "avgResponseTime": newAvg,
            })
        }
    }

    return Context.Res.Json(map[string]interface{}{"success": true, "message": "Task completed"})
}

func UpdatePrinter(Context openruntimes.Context, api *AppwriteAPI) openruntimes.Response {
	var payload struct {
		PrinterId    string `json:"printerId"`
		CurrentPaper int    `json:"currentPaper"`
		QueueCount   int    `json:"queueCount"`
		Status       string `json:"status"`
	}
	
	if err := ParseBody(Context.Req.Body, &payload); err != nil {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": "Invalid request body"})
	}

	updateData := map[string]interface{}{
		"currentPaper": payload.CurrentPaper,
		"queueCount":   payload.QueueCount,
		"status":       payload.Status,
		"lastUpdated":  time.Now().UTC().Format(time.RFC3339),
	}

	_, err := api.UpdateDocument(DatabaseId, PrintersCollection, payload.PrinterId, updateData)
	if err != nil {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": err.Error()})
	}
	return Context.Res.Json(map[string]interface{}{"success": true, "message": "Printer updated successfully"})
}

func GetPrinters(Context openruntimes.Context, api *AppwriteAPI) openruntimes.Response {
	docs, err := api.ListDocuments(DatabaseId, PrintersCollection)
	if err != nil {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": err.Error()})
	}
	return Context.Res.Json(map[string]interface{}{"success": true, "data": docs})
}

func GetUserStats(Context openruntimes.Context, api *AppwriteAPI) openruntimes.Response {
	var payload struct {
		EmployeeId string `json:"employeeId"`
	}
	
	if err := ParseBody(Context.Req.Body, &payload); err != nil {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": "Invalid request body"})
	}

	userMap, err := api.GetDocument(DatabaseId, UsersCollection, payload.EmployeeId)
	if err != nil {
		return Context.Res.Json(map[string]interface{}{"success": false, "error": err.Error()})
	}

	b, _ := json.Marshal(userMap)
	var user map[string]interface{}
	json.Unmarshal(b, &user)

	stats := map[string]interface{}{
		"totalTasks":      user["totalTasks"],
		"avgResponseTime": user["avgResponseTime"],
		"successRate":     user["successRate"],
	}
	return Context.Res.Json(map[string]interface{}{"success": true, "data": stats})
}

// ---------------------------------------------------------
// ENTRY POINT
// ---------------------------------------------------------

func Main(Context openruntimes.Context) openruntimes.Response {
	if Context.Req.Method == "OPTIONS" {
		return Context.Res.Json(map[string]interface{}{})
	}

	api := NewAppwriteAPI()

	path := Context.Req.Path
	method := Context.Req.Method
	
	// --- DATABASE EVENT WEBHOOK INTERCEPTION ---
	appwriteEvent := Context.Req.Headers["x-appwrite-event"]
	if appwriteEvent != "" {
		Context.Log("RADAR TRAPPED EVENT: " + appwriteEvent)
		
		// If it's a document/row update in our tasks collection
		if strings.Contains(appwriteEvent, "documents") || strings.Contains(appwriteEvent, "rows") {
			var eventDoc map[string]interface{}
			_ = ParseBody(Context.Req.Body, &eventDoc)
			
			priority, _ := eventDoc["priority"].(string)
			employeeId, _ := eventDoc["employeeId"].(string)
			
			if priority == "HIGH" && employeeId != "" {
				userMap, err := api.GetDocument(DatabaseId, UsersCollection, employeeId)
				if err == nil {
					if fcmToken, ok := userMap["fcmToken"].(string); ok && fcmToken != "" && fcmToken != "NULL" {
						Context.Log("Event Triggered Push! Sending to Tech: " + employeeId)
						err := api.SendPushNotification([]string{employeeId}, "⚠️ URGENT: High Priority Task!", "New high priority task assigned to you. Please attend immediately!")
						if err != nil {
							Context.Log("PUSH ERROR (Event): " + err.Error())
						} else {
							Context.Log("PUSH SUCCESS! Direct alert delivered.")
						}
					}
				}
			}
			return Context.Res.Json(map[string]interface{}{"success": true, "event": appwriteEvent})
		}
	}
	// -------------------------------------------

	Context.Log("REQUEST: " + method + " " + path)

	if path == "/tasks" && method == "GET" {
		return GetTasks(Context, api)
	}
	if path == "/tasks" && method == "POST" {
		return CreateTask(Context, api)
	}
    if path == "/saveToken" && method == "POST" {
        return SaveToken(Context, api)
    }
	if path == "/printers" && method == "GET" {
		return GetPrinters(Context, api)
	}
	if path == "/printers" && method == "PUT" {
		return UpdatePrinter(Context, api)
	}
    if path == "/users/stats" && method == "POST" {
        return GetUserStats(Context, api) 
    }
	if strings.HasPrefix(path, "/complete/") && method == "PUT" {
		taskId := strings.TrimPrefix(path, "/complete/")
		return CompleteTaskByPath(Context, api, taskId)
	}

	return Context.Res.Json(map[string]interface{}{
		"success": false,
		"error":   "Route not found: " + path,
	})
}
