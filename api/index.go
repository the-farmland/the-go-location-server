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

// establishConnection initializes the database connection pool using environment variables.
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

		dbpool, errParse = pgxpool.NewWithConfig(context.Background(), config)
		if errParse != nil {
			err = fmt.Errorf("unable to create connection pool: %v", errParse)
			return
		}
		log.Println("Database connection pool established.")
	})
	return err
}

// writeJSON is a helper to write JSON responses.
func writeJSON(w http.ResponseWriter, status int, data interface{}) {
    allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
    if allowedOrigin == "" {
        allowedOrigin = "*" 
    }
    w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    if err := json.NewEncoder(w).Encode(data); err != nil {
        log.Printf("Error writing JSON response: %v", err)
    }
}

// corsMiddleware adds the necessary CORS headers.
func corsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
        if allowedOrigin == "" {
            allowedOrigin = "*" 
        }

        if r.Method == http.MethodOptions {
            w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
            w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
            w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
            w.Header().Set("Access-Control-Max-Age", "86400")
            w.WriteHeader(http.StatusNoContent)
            return
        }

        w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
        next.ServeHTTP(w, r)
    })
}


// Handler is the main entry point for Vercel.
func Handler(w http.ResponseWriter, r *http.Request) {
    allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
    if allowedOrigin == "" {
        allowedOrigin = "*" 
    }
    w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)

    if r.Method == http.MethodOptions {
        w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
        w.Header().Set("Access-Control-Max-Age", "86400")
        w.WriteHeader(http.StatusNoContent)
        return
    }

    if err := establishConnection(); err != nil {
        log.Printf("Database connection error: %v", err)
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(http.StatusInternalServerError)
        json.NewEncoder(w).Encode(RpcResponse{
            Success: false,
            Error:   "Internal server error - database connection failed",
        })
        return
    }

    mux := http.NewServeMux()
    mux.HandleFunc("/rpc", rpcHandler)
    mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
        w.WriteHeader(http.StatusOK)
        w.Write([]byte("OK"))
    })
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        json.NewEncoder(w).Encode(RpcResponse{Success: false, Error: "Not Found"})
    })

    handler := http.Handler(mux)
    handler.ServeHTTP(w, r)
}



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

	var paramsWithUserid struct {
		Userid string `json:"userid"`
	}
	userid := ""
	if err := json.Unmarshal(req.Params, &paramsWithUserid); err == nil && paramsWithUserid.Userid != "" {
		userid = paramsWithUserid.Userid
	}

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
			log.Printf("Error logging user request: %v", err)
		}
	}

	response := rpcDispatcher(r.Context(), req)

	if userid != "" && response.Success {
		if err := logUserResponse(r.Context(), userid); err != nil {
			log.Printf("Error logging user response: %v", err)
		}
	}

	writeJSON(w, http.StatusOK, response)
}

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

func getTopLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct{ Limit int `json:"limit"` }
	p.Limit = 10
	if err := json.Unmarshal(params, &p); err != nil { return nil, err }
	rows, err := dbpool.Query(ctx, "SELECT * FROM get_top_locations($1);", p.Limit)
	if err != nil { return nil, err }
	defer rows.Close()
	var locations []Location
	for rows.Next() {
		var loc Location
		if err := rows.Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating); err != nil { return nil, err }
		locations = append(locations, loc)
	}
	return locations, nil
}

func getLocationById(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct{ ID string `json:"id"` }
	if err := json.Unmarshal(params, &p); err != nil || p.ID == "" { return nil, fmt.Errorf("invalid or missing 'id'") }
	var loc Location
	err := dbpool.QueryRow(ctx, "SELECT * FROM get_location_by_id($1);", p.ID).Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating)
	if err != nil { return nil, fmt.Errorf("location not found") }
	return loc, nil
}

func searchLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct{ Query string `json:"query"` }
	if err := json.Unmarshal(params, &p); err != nil || p.Query == "" { return nil, fmt.Errorf("invalid or missing 'query'") }
	rows, err := dbpool.Query(ctx, "SELECT * FROM search_locations($1);", p.Query)
	if err != nil { return nil, err }
	defer rows.Close()
	var locations []Location
	for rows.Next() {
		var loc Location
		if err := rows.Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating); err != nil { return nil, err }
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
		if err.Error() == "no rows in result set" { return false, nil }
		return false, err
	}
	return blocked, nil
}