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

// Location struct
type Location struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Country     string  `json:"country"`
	State       *string `json:"state"`
	Description *string `json:"description"`
	SVGLink     *string `json:"svg_link"`
	Rating      float64 `json:"rating"`
}

// RPC structs
type RpcRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

type RpcResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

// writeJSON with guaranteed CORS headers
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	// 🔥 Always set CORS headers, even on 500 or startup errors
	setCORSHeaders(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error writing JSON response: %v", err)
	}
}

// setCORSHeaders ensures CORS is applied on every response
func setCORSHeaders(w http.ResponseWriter) {
	allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
	if allowedOrigin == "" {
		allowedOrigin = "*" // fallback
	}
	w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "86400")
}

// corsMiddleware handles OPTIONS and wraps requests
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// establishConnection with robust error handling
func establishConnection() error {
	var err error
	once.Do(func() {
		connStr := os.Getenv("DB_CONN_STRING")
		if connStr == "" {
			err = fmt.Errorf("DB_CONN_STRING environment variable is not set")
			log.Printf("❌ Error: %v", err)
			return
		}

		// 🔧 Fix: Ensure SSL mode is required and connection params are correct
		connStr += "?sslmode=require&connect_timeout=10&pool_max_conns=1"

		config, parseErr := pgxpool.ParseConfig(connStr)
		if parseErr != nil {
			err = fmt.Errorf("failed to parse connection string: %v", parseErr)
			log.Printf("❌ Error: %v", err)
			return
		}

		// Set connection limits
		config.MaxConns = 1
		config.MinConns = 0
		config.MaxConnLifetime = 5 * time.Minute
		config.MaxConnIdleTime = 30 * time.Second
		config.HealthCheckPeriod = 1 * time.Minute

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		dbpool, err = pgxpool.NewWithConfig(ctx, config)
		if err != nil {
			err = fmt.Errorf("failed to create connection pool: %v", err)
			log.Printf("❌ Error: %v", err)
			return
		}

		// Test the connection
		if err = dbpool.Ping(ctx); err != nil {
			err = fmt.Errorf("failed to ping database: %v", err)
			log.Printf("❌ Error: %v", err)
			dbpool.Close()
			dbpool = nil
			return
		}

		log.Println("✅ Database connection pool established and tested.")
	})
	return err
}

// Handler - main entry point
func Handler(w http.ResponseWriter, r *http.Request) {
	// 🛡️ Always set CORS headers — even if panic or DB fails
	setCORSHeaders(w)

	// Handle OPTIONS early
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Defer recovery to prevent crashes from killing response
	defer func() {
		if rec := recover(); rec != nil {
			log.Printf("⚠️ Panic recovered: %v", rec)
			writeJSON(w, http.StatusInternalServerError, RpcResponse{
				Success: false,
				Error:   "Internal server error",
			})
		}
	}()

	// Connect to DB
	if err := establishConnection(); err != nil {
		log.Printf("❌ DB connection failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, RpcResponse{
			Success: false,
			Error:   "Failed to connect to database. Please try again later.",
		})
		return
	}

	// Routes
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", rpcHandler)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		setCORSHeaders(w)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, RpcResponse{Success: false, Error: "Not Found"})
	})

	// Apply CORS
	handler := corsMiddleware(mux)
	handler.ServeHTTP(w, r)
}

func rpcHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, RpcResponse{Success: false, Error: "Method not allowed"})
		return
	}

	var req RpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, RpcResponse{Success: false, Error: "Invalid JSON"})
		return
	}

	var userid string
	var paramsWithUserid struct {
		Userid string `json:"userid"`
	}
	if err := json.Unmarshal(req.Params, &paramsWithUserid); err == nil && paramsWithUserid.Userid != "" {
		userid = paramsWithUserid.Userid
	}

	if userid != "" {
		blocked, err := isUserBlocked(r.Context(), userid)
		if err != nil {
			log.Printf("Rate limit check failed: %v", err)
			writeJSON(w, http.StatusInternalServerError, RpcResponse{Success: false, Error: "Internal error"})
			return
		}
		if blocked {
			writeJSON(w, http.StatusTooManyRequests, RpcResponse{Success: false, Error: "Rate limit exceeded"})
			return
		}
		if err := logUserRequest(r.Context(), userid); err != nil {
			log.Printf("Failed to log request: %v", err)
		}
	}

	response := rpcDispatcher(r.Context(), req)

	if userid != "" && response.Success {
		if err := logUserResponse(r.Context(), userid); err != nil {
			log.Printf("Failed to log response: %v", err)
		}
	}

	writeJSON(w, http.StatusOK, response)
}

func rpcDispatcher(ctx context.Context, req RpcRequest) RpcResponse {
	switch req.Method {
	case "getTopLocations":
		data, err := getTopLocations(ctx, req.Params)
		if err != nil {
			return RpcResponse{Success: false, Error: err.Error()}
		}
		return RpcResponse{Success: true, Data: data}
	case "getLocationById":
		data, err := getLocationById(ctx, req.Params)
		if err != nil {
			return RpcResponse{Success: false, Error: err.Error()}
		}
		return RpcResponse{Success: true, Data: data}
	case "searchLocations":
		data, err := searchLocations(ctx, req.Params)
		if err != nil {
			return RpcResponse{Success: false, Error: err.Error()}
		}
		return RpcResponse{Success: true, Data: data}
	default:
		return RpcResponse{Success: false, Error: "Method not found"}
	}
}

// Database Handlers
func getTopLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct{ Limit int `json:"limit"` }
	p.Limit = 10
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid limit")
	}
	if p.Limit < 1 || p.Limit > 100 {
		p.Limit = 10
	}

	rows, err := dbpool.Query(ctx, "SELECT * FROM get_top_locations($1);", p.Limit)
	if err != nil {
		return nil, fmt.Errorf("database query failed: %v", err)
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
	var p struct{ ID string `json:"id"` }
	if err := json.Unmarshal(params, &p); err != nil || p.ID == "" {
		return nil, fmt.Errorf("missing or invalid id")
	}

	var loc Location
	err := dbpool.QueryRow(ctx, "SELECT * FROM get_location_by_id($1);", p.ID).Scan(
		&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating,
	)
	if err != nil {
		return nil, fmt.Errorf("location not found")
	}
	return loc, nil
}

func searchLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct{ Query string `json:"query"` }
	if err := json.Unmarshal(params, &p); err != nil || p.Query == "" {
		return nil, fmt.Errorf("query is required")
	}

	rows, err := dbpool.Query(ctx, "SELECT * FROM search_locations($1);", p.Query)
	if err != nil {
		return nil, fmt.Errorf("search failed: %v", err)
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