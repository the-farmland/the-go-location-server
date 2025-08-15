package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Global database connection pool
var (
	dbpool *pgxpool.Pool
	once   sync.Once
)

// Location struct matches the database schema
type Location struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Country     string  `json:"country"`
	State       *string `json:"state"`
	Description *string `json:"description"`
	SVGLink     *string `json:"svg_link"`
	Rating      float64 `json:"rating"`
}

// JSON-RPC 2.0 structs
type JsonRpcRequest struct {
	Jsonrpc string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
	ID      interface{}     `json:"id,omitempty"`
}

type JsonRpcResponse struct {
	Jsonrpc string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RpcError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type RpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data,omitempty"`
}

func NewRpcError(code int, message, data string, id interface{}) *JsonRpcResponse {
	return &JsonRpcResponse{
		Jsonrpc: "2.0",
		Error: &RpcError{
			Code:    code,
			Message: message,
			Data:    data,
		},
		ID: id,
	}
}

// establishConnection initializes the database connection pool
func establishConnection() error {
	var err error
	once.Do(func() {
		connStr := os.Getenv("DB_CONN_STRING")
		if connStr == "" {
			err = fmt.Errorf("DB_CONN_STRING environment variable not set")
			return
		}

		config, errParse := pgxpool.ParseConfig(connStr)
		if errParse != nil {
			err = fmt.Errorf("unable to parse connection string: %v", errParse)
			return
		}

		config.MaxConns = 1
		config.MinConns = 0
		config.MaxConnIdleTime = 10 * time.Second

		dbpool, err = pgxpool.NewWithConfig(context.Background(), config)
		if err != nil {
			err = fmt.Errorf("unable to create connection pool: %v", err)
			return
		}

		log.Println("Database connection pool established.")
	})
	return err
}

// corsMiddleware adds CORS headers and handles preflight (OPTIONS)
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Get allowed origin from environment, fallback to * if not set
		allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
		if allowedOrigin == "" {
			allowedOrigin = "*"
		}

		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400")
		w.Header().Set("Content-Type", "application/json")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Call the next handler
		next.ServeHTTP(w, r)
	})
}

// writeJSON writes a JSON response with proper headers
func writeJSON(w http.ResponseWriter, status int, response *JsonRpcResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error writing JSON response: %v", err)
	}
}

// Handler is the main entry point for Vercel
func Handler(w http.ResponseWriter, r *http.Request) {
	// Ensure CORS headers are applied even during panics or early errors
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic recovered: %v", r)
			response := NewRpcError(-32603, "Internal error", "server panic", nil)
			writeJSON(w, http.StatusInternalServerError, response)
		}
	}()

	if err := establishConnection(); err != nil {
		log.Printf("Database connection failed: %v", err)
		response := NewRpcError(-32603, "Internal error", "failed to connect to database", nil)
		writeJSON(w, http.StatusInternalServerError, response)
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", rpcHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, NewRpcError(-32600, "Not Found", "Endpoint not found", nil))
	})

	handler := corsMiddleware(mux)
	handler.ServeHTTP(w, r)
}

func rpcHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, NewRpcError(-32600, "Invalid method", "Only POST allowed", nil))
		return
	}

	var req JsonRpcRequest
	var id interface{}

	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, NewRpcError(-32700, "Parse error", "Invalid JSON", nil))
		return
	}

	if req.Jsonrpc != "2.0" {
		writeJSON(w, http.StatusBadRequest, NewRpcError(-32600, "Invalid Request", "jsonrpc must be '2.0'", nil))
		return
	}

	id = req.ID

	// Extract userid if present
	var userid string
	if req.Params != nil {
		var paramsMap map[string]interface{}
		if json.Unmarshal(req.Params, &paramsMap) == nil {
			if uid, ok := paramsMap["userid"].(string); ok && uid != "" {
				userid = uid
			}
		}
	}

	// Rate limiting check
	if userid != "" {
		blocked, err := isUserBlocked(r.Context(), userid)
		if err != nil {
			log.Printf("Rate limit check failed: %v", err)
			writeJSON(w, http.StatusInternalServerError, NewRpcError(-32603, "Internal error", "rate limit check failed", id))
			return
		}
		if blocked {
			writeJSON(w, http.StatusTooManyRequests, NewRpcError(-32001, "Rate limit exceeded", "Too many requests", id))
			return
		}
		if err := logUserRequest(r.Context(), userid); err != nil {
			log.Printf("Failed to log request: %v", err)
		}
	}

	// Dispatch method
	var result interface{}
	var rpcError *RpcError

	switch req.Method {
	case "getTopLocations":
		result, rpcError = handleGetTopLocations(r.Context(), req.Params)
	case "getLocationById":
		result, rpcError = handleGetLocationById(r.Context(), req.Params)
	case "searchLocations":
		result, rpcError = handleSearchLocations(r.Context(), req.Params)
	default:
		rpcError = &RpcError{Code: -32601, Message: "Method not found", Data: req.Method}
	}

	// Build response
	var response *JsonRpcResponse
	if rpcError != nil {
		response = &JsonRpcResponse{
			Jsonrpc: "2.0",
			Error:   rpcError,
			ID:      id,
		}
	} else {
		response = &JsonRpcResponse{
			Jsonrpc: "2.0",
			Result:  result,
			ID:      id,
		}
	}

	// Log successful response
	if userid != "" && rpcError == nil {
		if err := logUserResponse(r.Context(), userid); err != nil {
			log.Printf("Failed to log response: %v", err)
		}
	}

	writeJSON(w, http.StatusOK, response)
}

// Handler functions
func handleGetTopLocations(ctx context.Context, params json.RawMessage) (interface{}, *RpcError) {
	var p struct{ Limit int `json:"limit"` }
	p.Limit = 10
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, &RpcError{Code: -32602, Message: "Invalid params", Data: "limit must be a number"}
	}
	if p.Limit < 1 || p.Limit > 100 {
		p.Limit = 10
	}

	rows, err := dbpool.Query(ctx, "SELECT * FROM get_top_locations($1);", p.Limit)
	if err != nil {
		log.Printf("DB query error in getTopLocations: %v", err)
		return nil, &RpcError{Code: -32603, Message: "Internal error", Data: "failed to fetch locations"}
	}
	defer rows.Close()

	var locations []Location
	for rows.Next() {
		var loc Location
		if err := rows.Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating); err != nil {
			return nil, &RpcError{Code: -32603, Message: "Internal error", Data: "failed to parse location"}
		}
		locations = append(locations, loc)
	}
	return locations, nil
}

func handleGetLocationById(ctx context.Context, params json.RawMessage) (interface{}, *RpcError) {
	var p struct{ ID string `json:"id"` }
	if err := json.Unmarshal(params, &p); err != nil || p.ID == "" {
		return nil, &RpcError{Code: -32602, Message: "Invalid params", Data: "missing or invalid id"}
	}

	var loc Location
	err := dbpool.QueryRow(ctx, "SELECT * FROM get_location_by_id($1);", p.ID).Scan(
		&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating,
	)
	if err != nil {
		return nil, &RpcError{Code: -32000, Message: "Location not found", Data: err.Error()}
	}
	return loc, nil
}

func handleSearchLocations(ctx context.Context, params json.RawMessage) (interface{}, *RpcError) {
	var p struct{ Query string `json:"query"` }
	if err := json.Unmarshal(params, &p); err != nil || p.Query == "" {
		return nil, &RpcError{Code: -32602, Message: "Invalid params", Data: "query is required"}
	}

	rows, err := dbpool.Query(ctx, "SELECT * FROM search_locations($1);", p.Query)
	if err != nil {
		return nil, &RpcError{Code: -32603, Message: "Internal error", Data: "search failed"}
	}
	defer rows.Close()

	var locations []Location
	for rows.Next() {
		var loc Location
		if err := rows.Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating); err != nil {
			return nil, &RpcError{Code: -32603, Message: "Internal error", Data: "failed to parse search result"}
		}
		locations = append(locations, loc)
	}
	return locations, nil
}

// DB utility functions
func logUserRequest(ctx context.Context, userid string) error {
	_, err := dbpool.Exec(ctx, "SELECT log_user_request($1);", userid)
	return err
}

func logUserResponse(ctx context.Context, userid string) error {
	_, err := dbpool.Exec(ctx, "SELECT log_user_response($1);", userid)
	return err
}

func isUserBlocked(ctx context.Context, userid string) (bool, error) {
	var blocked bool
	err := dbpool.QueryRow(ctx, "SELECT is_user_blocked($1);", userid).Scan(&blocked)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, err
	}
	return blocked, nil
}