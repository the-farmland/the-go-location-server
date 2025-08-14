// File: the-go-location-server/api/index.go
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

// Location struct matches the database schema.
type Location struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Country     string  `json:"country"`
	State       *string `json:"state"` // Use pointer for nullable fields
	Description *string `json:"description"`
	SVGLink     *string `json:"svg_link"`
	Rating      float64 `json:"rating"`
}

// Structs for JSON-RPC requests and responses
type RpcRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type RpcResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// establishConnection initializes the database connection pool.
func establishConnection() {
	once.Do(func() {
		connStr := os.Getenv("DB_CONN_STRING")
		if connStr == "" {
			log.Fatal("FATAL: DB_CONN_STRING environment variable not set.")
		}

		config, err := pgxpool.ParseConfig(connStr)
		if err != nil {
			log.Fatalf("Unable to parse connection string: %v\n", err)
		}

		// Vercel specific settings for serverless functions
		config.MaxConns = 1
		config.MinConns = 0
		config.MaxConnIdleTime = 10 * time.Second

		dbpool, err = pgxpool.NewWithConfig(context.Background(), config)
		if err != nil {
			log.Fatalf("Unable to create connection pool: %v\n", err)
		}
		log.Println("Database connection pool established.")
	})
}

// --- Database Interaction Functions ---

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
		// If no rows are found, the user isn't blocked.
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, err
	}
	return blocked, nil
}

// --- RPC Method Handlers ---

func getTopLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		Limit int `json:"limit"`
	}
	// Set a default limit
	p.Limit = 10
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid params for getTopLocations: %w", err)
	}

	rows, err := dbpool.Query(ctx, "SELECT * FROM get_top_locations($1);", p.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []Location
	for rows.Next() {
		var loc Location
		if err := rows.Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating); err != nil {
			return nil, err
		}
		locations = append(locations, loc)
	}
	return locations, nil
}

func getLocationById(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.ID == "" {
		return nil, fmt.Errorf("invalid or missing 'id' in params")
	}

	var loc Location
	err := dbpool.QueryRow(ctx, "SELECT * FROM get_location_by_id($1);", p.ID).Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating)
	if err != nil {
		if err.Error() == "no rows in result set" {
			return nil, fmt.Errorf("location not found")
		}
		return nil, err
	}
	return loc, nil
}

func searchLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct {
		Query string `json:"query"`
	}
	if err := json.Unmarshal(params, &p); err != nil || p.Query == "" {
		return nil, fmt.Errorf("invalid or missing 'query' in params")
	}

	rows, err := dbpool.Query(ctx, "SELECT * FROM search_locations($1);", p.Query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var locations []Location
	for rows.Next() {
		var loc Location
		if err := rows.Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating); err != nil {
			return nil, err
		}
		locations = append(locations, loc)
	}
	return locations, nil
}

// rpcDispatcher calls the correct function based on the method name.
func rpcDispatcher(ctx context.Context, req RpcRequest) RpcResponse {
	var data interface{}
	var err error

	switch req.Method {
	case "getTopLocations":
		data, err = getTopLocations(ctx, req.Params)
	case "getLocationById":
		data, err = getLocationById(ctx, req.Params)
	case "searchLocations":
		data, err = searchLocations(ctx, req.Params)
	default:
		err = fmt.Errorf("method '%s' not found", req.Method)
	}

	if err != nil {
		return RpcResponse{Success: false, Error: err.Error()}
	}
	return RpcResponse{Success: true, Data: data}
}

// rpcHandler processes the incoming RPC requests.
func rpcHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var req RpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RpcResponse{Success: false, Error: "Invalid JSON"})
		return
	}

	// Extract userid for rate limiting
	var paramsWithUserid struct {
		Userid string `json:"userid"`
	}
	userid := ""
	if err := json.Unmarshal(req.Params, &paramsWithUserid); err == nil && paramsWithUserid.Userid != "" {
		userid = paramsWithUserid.Userid
	}

	// Rate limiting logic
	if userid != "" {
		blocked, err := isUserBlocked(r.Context(), userid)
		if err != nil {
			log.Printf("Error checking if user is blocked: %v", err)
			writeJSON(w, http.StatusInternalServerError, RpcResponse{Success: false, Error: "Internal server error"})
			return
		}
		if blocked {
			writeJSON(w, http.StatusTooManyRequests, RpcResponse{Success: false, Error: "You have exceeded the rate limit"})
			return
		}
		if err := logUserRequest(r.Context(), userid); err != nil {
			log.Printf("Error logging user request: %v", err) // Log and continue
		}
	}

	// Dispatch the RPC call
	response := rpcDispatcher(r.Context(), req)

	if userid != "" && response.Success {
		if err := logUserResponse(r.Context(), userid); err != nil {
			log.Printf("Error logging user response: %v", err) // Log and continue
		}
	}

	writeJSON(w, http.StatusOK, response)
}

// writeJSON is a helper to write JSON responses.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error writing JSON response: %v", err)
	}
}

// corsMiddleware adds the necessary CORS headers.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set your frontend's Vercel URL here or use "*" for development
		// For production, it's better to get this from an environment variable.
		allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
		if allowedOrigin == "" {
			allowedOrigin = "*" // Fallback for local dev
		}
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Max-Age", "86400")

		// Handle preflight requests
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Handler is the main entry point for Vercel.
func Handler(w http.ResponseWriter, r *http.Request) {
	// Ensure the database connection is ready
	establishConnection()

	// Simple router
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/rpc", rpcHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})

	// Wrap the router with CORS middleware
	handler := corsMiddleware(mux)
	handler.ServeHTTP(w, r)
}
