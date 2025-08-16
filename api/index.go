// File: the-go-location-server-main/api/index.go
package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Global database connection pool, initialized once.
var dbpool *pgxpool.Pool

// Location struct matches the database schema.
type Location struct {
	ID          string  `json:"id"`
	Name        string  `json:"name"`
	Country     string  `json:"country"`
	State       *string `json:"state"`
	Description *string `json:"description"`
	SVGLink     *string `json:"svg_link"`
	Rating      float64 `json:"rating"`
}

// RpcRequest struct for JSON-RPC requests.
type RpcRequest struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

// init runs once when the serverless function is initialized.
// It establishes the database connection.
func init() {
	connStr := os.Getenv("DB_CONN_STRING")
	if connStr == "" {
		log.Fatal("FATAL: DB_CONN_STRING environment variable not set")
	}

	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("FATAL: Unable to parse connection string: %v", err)
	}

	config.MaxConns = 2 // A small pool is fine for serverless
	config.MinConns = 0
	config.MaxConnIdleTime = 30 * time.Second

	dbpool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalf("FATAL: Unable to create connection pool: %v", err)
	}

	log.Println("Database connection pool established successfully.")
}

// Handler is the main entry point for Vercel. It sets up the Gin engine.
func Handler(w http.ResponseWriter, r *http.Request) {
	// Set Gin to release mode for production on Vercel
	gin.SetMode(gin.ReleaseMode)

	// Create a new Gin engine
	router := gin.New()

	// Use Gin's logger and recovery middleware
	router.Use(gin.Logger())
	router.Use(gin.Recovery())

	// --- Bulletproof CORS Configuration ---
	// This middleware is now the first thing to execute on any request.
	allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
	if allowedOrigin == "" {
		// Fallback to a restrictive default if not set, to avoid accidental open CORS.
		// For development, you can set this to "http://localhost:3000" in your .env file.
		// For production, set it to your actual frontend URL.
		allowedOrigin = "null" // Or a specific domain
		log.Println("WARNING: ALLOWED_ORIGIN not set. CORS will likely fail.")
	}

	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{allowedOrigin},
		AllowMethods:     []string{"POST", "GET", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Authorization", "Accept"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// --- API Routes ---
	router.POST("/rpc", rpcHandler)
	router.GET("/health", func(c *gin.Context) {
		c.String(http.StatusOK, "OK")
	})

	// --- Handle 404 Not Found ---
	router.NoRoute(func(c *gin.Context) {
		c.JSON(http.StatusNotFound, gin.H{"success": false, "error": "Not Found"})
	})

	// Let Gin handle the request
	router.ServeHTTP(w, r)
}

// rpcHandler processes all JSON-RPC requests.
func rpcHandler(c *gin.Context) {
	// Ensure the database pool is available.
	if dbpool == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "Internal server error: database connection not available"})
		return
	}

	var req RpcRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "Invalid JSON payload"})
		return
	}

	// --- Rate Limiting Logic ---
	var paramsWithUserid struct {
		Userid string `json:"userid"`
	}
	userid := ""
	if err := json.Unmarshal(req.Params, &paramsWithUserid); err == nil && paramsWithUserid.Userid != "" {
		userid = paramsWithUserid.Userid
	}

	if userid != "" {
		blocked, err := isUserBlocked(c.Request.Context(), userid)
		if err != nil {
			log.Printf("Error checking if user is blocked: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": "Internal server error"})
			return
		}
		if blocked {
			c.JSON(http.StatusTooManyRequests, gin.H{"success": false, "error": "You have exceeded the rate limit"})
			return
		}
		if err := logUserRequest(c.Request.Context(), userid); err != nil {
			log.Printf("Error logging user request: %v", err)
		}
	}
	// --- End Rate Limiting ---

	// Dispatch to the correct function based on the method
	data, err := rpcDispatcher(c.Request.Context(), req)

	if err != nil {
		// Log the internal error details
		log.Printf("RPC Dispatch Error for method '%s': %v", req.Method, err)
		// Return a generic error to the client
		c.JSON(http.StatusOK, gin.H{"success": false, "error": err.Error()})
		return
	}

	// Log successful response if user is identified
	if userid != "" {
		if err := logUserResponse(c.Request.Context(), userid); err != nil {
			log.Printf("Error logging user response: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": data})
}

// rpcDispatcher calls the appropriate data-fetching function.
func rpcDispatcher(ctx context.Context, req RpcRequest) (interface{}, error) {
	switch req.Method {
	case "getTopLocations":
		return getTopLocations(ctx, req.Params)
	case "getLocationById":
		return getLocationById(ctx, req.Params)
	case "searchLocations":
		return searchLocations(ctx, req.Params)
	default:
		return nil, fmt.Errorf("method '%s' not found", req.Method)
	}
}

// --- Database Functions (Unchanged Logic) ---

func getTopLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct{ Limit int `json:"limit"` }
	p.Limit = 10 // Default limit
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("invalid parameters for getTopLocations: %w", err)
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
	var p struct{ ID string `json:"id"` }
	if err := json.Unmarshal(params, &p); err != nil || p.ID == "" {
		return nil, fmt.Errorf("invalid or missing 'id' parameter")
	}
	var loc Location
	err := dbpool.QueryRow(ctx, "SELECT * FROM get_location_by_id($1);", p.ID).Scan(&loc.ID, &loc.Name, &loc.Country, &loc.State, &loc.Description, &loc.SVGLink, &loc.Rating)
	if err != nil {
		return nil, fmt.Errorf("location not found")
	}
	return loc, nil
}

func searchLocations(ctx context.Context, params json.RawMessage) (interface{}, error) {
	var p struct{ Query string `json:"query"` }
	if err := json.Unmarshal(params, &p); err != nil || p.Query == "" {
		return nil, fmt.Errorf("invalid or missing 'query' parameter")
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

func logUserRequest(ctx context.Context, userid string) error {
	_, err := dbpool.Exec(ctx, "SELECT log_user_request($1);", userid)
	return err
}

// THIS IS THE CORRECTED FUNCTION
func logUserResponse(ctx context.Context, userid string) error {
	_, err := dbpool.Exec(ctx, "SELECT log_user_response($1);", userid)
	return err
}

func isUserBlocked(ctx context.Context, userid string) (bool, error) {
	var blocked bool
	err := dbpool.QueryRow(ctx, "SELECT is_user_blocked($1);", userid).Scan(&blocked)
	if err != nil {
		// pgx.ErrNoRows is a common case if the user is not in the log table yet
		if err.Error() == "no rows in result set" {
			return false, nil
		}
		return false, err
	}
	return blocked, nil
}