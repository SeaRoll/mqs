package mqs

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// App represents the whole application from start to finish.
// It contains the database pool and the methods to start a web application.
type App struct {
	pool *pgxpool.Pool
}

// InitApp initializes the application by creating a connection pool to the database.
// Make sure to call Close() when the application is done to release resources.
func InitApp(ctx context.Context, url string) (*App, error) {
	dbpool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := RunMigrations(ctx, dbpool); err != nil {
		dbpool.Close()
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	app := &App{
		pool: dbpool,
	}
	return app, nil
}

//go:embed migrations/*.sql
var migrations embed.FS

// RunMigrations runs database migrations through migration files that exist
func RunMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	// Create a transaction
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create migration table
	_, err = tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS migrations (
			name TEXT PRIMARY KEY
	)`)
	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	// Get the list of migration files
	files, err := migrations.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Sort files by name
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name()
	})

	// Apply each migration only if their name is not in the migrations table
	for _, file := range files {
		name := file.Name()

		var exists bool
		err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM migrations WHERE name = $1)", name).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check if migration %s exists: %w", name, err)
		}
		if exists {
			slog.Info("migration already applied", "name", name)
			continue
		}

		data, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("failed to read migration file %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", name, err)
		}

		if _, err := tx.Exec(ctx, "INSERT INTO migrations (name) VALUES ($1)", name); err != nil {
			return fmt.Errorf("failed to insert migration %s into migrations table: %w", name, err)
		}

		slog.Info("applied migration", "name", name)
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Close releases the resources held by the application.
func (app *App) Close() {
	if app.pool != nil {
		app.pool.Close()
	}
}

func (app *App) RunServer(ctx context.Context, port int) error {
	mux := app.initRoutes()

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	errChan := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errChan <- err
		}
	}()

	slog.Info("server started", "address", srv.Addr)

	select {
	case err := <-errChan:
		return fmt.Errorf("server error: %w", err)
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
	}

	return nil
}

// initRoutes initializes the HTTP routes for the application.
func (app *App) initRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /health", app.handleGetHealth())
	mux.Handle("GET /docs", handleGetOpenAPIHTML())
	mux.Handle("GET /openapi.yaml", handleGetOpenAPI())
	mux.HandleFunc("POST /topics", app.createTopic)
	mux.HandleFunc("GET /topics", app.getTopics)
	mux.HandleFunc("DELETE /topics/{slug}", app.deleteTopic)

	mux.HandleFunc("GET /topics/{slug}/webhooks", app.getWebhooksForTopic)
	mux.HandleFunc("POST /topics/{slug}/webhooks", app.createWebhook)
	mux.HandleFunc("DELETE /topics/{slug}/webhooks/{webhookId}", app.deleteWebhook)

	mux.HandleFunc("POST /topics/{slug}/messages", app.createMessage)

	handler := accesslog(mux)
	handler = recovery(handler)
	return handler
}

//go:embed openapi.yaml
var openAPI []byte

// handleGetOpenAPI returns an [http.HandlerFunc] that serves the OpenAPI specification YAML file.
// The file is embedded in the binary using the go:embed directive.
func handleGetOpenAPI() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		w.WriteHeader(http.StatusOK)
		w.Write(openAPI)
	}
}

//go:embed openapi.html
var openAPIHTML []byte

// handleGetOpenAPIHTML returns an [http.HandlerFunc] that serves the OpenAPI HTML documentation page.
func handleGetOpenAPIHTML() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		w.Write(openAPIHTML)
	}
}

// handleGetHealth returns an [http.HandlerFunc] for health check.
func (app *App) handleGetHealth() http.HandlerFunc {
	type responseBody struct {
		Uptime         string    `json:"uptime"`
		LastCommitHash string    `json:"lastCommitHash"`
		LastCommitTime time.Time `json:"lastCommitTime"`
		DirtyBuild     bool      `json:"dirtyBuild"`
		ConnectedToDB  bool      `json:"connectedToDB"`
	}

	res := responseBody{}
	buildInfo, _ := debug.ReadBuildInfo()
	for _, kv := range buildInfo.Settings {
		if kv.Value == "" {
			continue
		}
		switch kv.Key {
		case "vcs.revision":
			res.LastCommitHash = kv.Value
		case "vcs.time":
			res.LastCommitTime, _ = time.Parse(time.RFC3339, kv.Value)
		case "vcs.modified":
			res.DirtyBuild = kv.Value == "true"
		}
	}

	up := time.Now()
	return func(w http.ResponseWriter, r *http.Request) {
		if app.pool != nil {
			if err := app.pool.Ping(r.Context()); err != nil {
				slog.Error("failed to connect to database", "error", err)
				res.ConnectedToDB = false
			} else {
				res.ConnectedToDB = true
			}
		} else {
			slog.Warn("no database connection pool available")
			res.ConnectedToDB = false
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		res.Uptime = time.Since(up).String()
		if err := json.NewEncoder(w).Encode(res); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

// accesslog is a middleware that logs request and response details,
// including latency, method, path, query parameters, IP address, response status, and bytes sent.
func accesslog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wr := responseRecorder{ResponseWriter: w}

		next.ServeHTTP(&wr, r)

		slog.InfoContext(r.Context(), "accessed",
			slog.String("latency", time.Since(start).String()),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.String("query", r.URL.RawQuery),
			slog.String("ip", r.RemoteAddr),
			slog.Int("status", wr.status),
			slog.Int("bytes", wr.numBytes))
	})
}

// recovery is a middleware that recovers from panics during HTTP handler execution and logs the error details.
// It must be the last middleware in the chain to ensure it captures all panics.
func recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wr := responseRecorder{ResponseWriter: w}
		defer func() {
			err := recover()
			if err == nil {
				return
			}

			if err, ok := err.(error); ok && errors.Is(err, http.ErrAbortHandler) {
				// Handle the abort gracefully
				return
			}

			stack := make([]byte, 1024)
			n := runtime.Stack(stack, true)

			slog.ErrorContext(r.Context(), "panic!",
				slog.Any("error", err),
				slog.String("stack", string(stack[:n])),
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.String("query", r.URL.RawQuery),
				slog.String("ip", r.RemoteAddr))

			if wr.status > 0 {
				// response was already sent, nothing we can do
				return
			}

			// send error response
			http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
		}()
		next.ServeHTTP(&wr, r)
	})
}

// PerformJSONQuery executes a SQL query that returns a single JSON result.
// It uses the provided context and transaction to execute the query,
// scans the result into a variable of type T, and unmarshals the JSON string into that variable.
// The function returns the result of type T and an error if any occurs during the process.
// It is a generic function that can handle any type T that can be unmarshaled from JSON.
func PerformJSONQuery[T any](ctx context.Context, dbtx pgx.Tx, query string, args ...any) (T, error) {
	var result T
	rows, err := dbtx.Query(ctx, query, args...)
	if err != nil {
		return result, fmt.Errorf("failed to query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var resp string
		err := rows.Scan(&resp)
		if err != nil {
			return result, fmt.Errorf("failed to scan: %w", err)
		}
		if err := json.Unmarshal([]byte(resp), &result); err != nil {
			return result, fmt.Errorf("failed to unmarshal: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return result, fmt.Errorf("failed to iterate over rows: %w", err)
	}

	return result, nil
}

// responseRecorder is a wrapper around [http.ResponseWriter] that records the status and bytes written during the response.
// It implements the [http.ResponseWriter] interface by embedding the original ResponseWriter.
type responseRecorder struct {
	http.ResponseWriter
	status   int
	numBytes int
}

// Header implements the [http.ResponseWriter] interface.
func (re *responseRecorder) Header() http.Header {
	return re.ResponseWriter.Header()
}

// Write implements the [http.ResponseWriter] interface.
func (re *responseRecorder) Write(b []byte) (int, error) {
	re.numBytes += len(b)
	return re.ResponseWriter.Write(b)
}

// WriteHeader implements the [http.ResponseWriter] interface.
func (re *responseRecorder) WriteHeader(statusCode int) {
	re.status = statusCode
	re.ResponseWriter.WriteHeader(statusCode)
}

// createTopic handles HTTP requests to create a new topic.
// It expects a JSON request body with a "slug" field representing the topic's unique identifier.
// The function validates the input, starts a database transaction, inserts the new topic into the "topics" table,
// and commits the transaction. If successful, it responds with HTTP 201 Created and a JSON success message.
// On error, it responds with an appropriate HTTP error code and message.
func (app *App) createTopic(w http.ResponseWriter, r *http.Request) {
	type requestBody struct {
		Slug string `json:"slug"`
	}

	var req requestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if req.Slug == "" {
		http.Error(w, "topic slug cannot be empty", http.StatusBadRequest)
		return
	}

	tx, err := app.pool.Begin(r.Context())
	if err != nil {
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	// Create the topic in the database
	_, err = tx.Exec(r.Context(), "INSERT INTO topics (slug) VALUES ($1)", req.Slug)
	if err != nil {
		slog.Error("failed to create topic", "error", err)
		http.Error(w, "failed to create topic", http.StatusInternalServerError)
		return
	}
	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit transaction", "error", err)
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	response := map[string]bool{"success": true}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

// getTopics handles HTTP requests to retrieve a paginated list of topics.
// It accepts optional "offset" and "limit" query parameters to control pagination,
// with "limit" defaulting to 10 and capped between 1 and 100, and "offset" defaulting to 0.
// The handler queries the database for topics ordered by creation date (descending),
// and returns the results as a JSON array under the "data" key.
// Responds with appropriate HTTP error codes for invalid parameters or internal errors.
func (app *App) getTopics(w http.ResponseWriter, r *http.Request) {
	type reqQueryParams struct {
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
	}
	params := reqQueryParams{
		Offset: 0,
		Limit:  10,
	}
	if r.URL.Query().Get("offset") != "" {
		if parsedOffset, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil {
			params.Offset = parsedOffset
		}
	}
	if r.URL.Query().Get("limit") != "" {
		if parsedLimit, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil {
			params.Limit = parsedLimit
		}
	}
	if params.Limit <= 0 || params.Limit > 100 {
		http.Error(w, "limit must be between 1 and 100", http.StatusBadRequest)
		return
	}
	if params.Offset < 0 {
		http.Error(w, "offset cannot be negative", http.StatusBadRequest)
		return
	}

	tx, err := app.pool.BeginTx(r.Context(), pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	type queryResponse struct {
		Slug      string    `json:"slug"`
		CreatedAt time.Time `json:"createdAt"`
	}
	const query = `
SELECT COALESCE(json_agg (
	json_build_object(
		'slug', t.slug,
		'createdAt', t.created_at
	)
), '[]'::json)
FROM (
  SELECT *
  FROM topics
  ORDER BY created_at DESC
  LIMIT $1
  OFFSET $2
) t;
`

	resp, err := PerformJSONQuery[[]queryResponse](r.Context(), tx, query, params.Limit, params.Offset)
	if err != nil {
		slog.Error("failed to get topics", "error", err)
		http.Error(w, "failed to get topics", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit transaction", "error", err)
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(map[string]any{
		"data": resp,
	}); err != nil {
		slog.Error("failed to encode response", "error", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}

// deleteTopic handles HTTP DELETE requests to remove a topic identified by its slug.
// It expects the "slug" parameter to be present in the URL path. The function begins a database
// transaction, attempts to delete the topic from the "topics" table, and commits the transaction
// if successful. If the topic does not exist, it responds with a 404 Not Found error. On successful
// deletion, it returns a 202 Accepted status with a JSON response indicating success. Any errors
// during the process are logged and result in appropriate HTTP error responses.
func (app *App) deleteTopic(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if slug == "" {
		http.Error(w, "slug must be provided in the URL path", http.StatusBadRequest)
		return
	}

	tx, err := app.pool.Begin(r.Context())
	if err != nil {
		slog.Error("failed to begin transaction", "error", err)
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	// Delete the topic from the database
	cmdTag, err := tx.Exec(r.Context(), "DELETE FROM topics WHERE slug = $1", slug)
	if err != nil {
		slog.Error("failed to delete topic", "slug", slug, "error", err)
		http.Error(w, "failed to delete topic", http.StatusInternalServerError)
		return
	}

	// Check if any row was actually deleted
	if cmdTag.RowsAffected() == 0 {
		http.Error(w, "topic not found", http.StatusNotFound)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit transaction", "error", err)
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	// Per requirement, return 202 Accepted for successful deletion.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := map[string]bool{"success": true}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		// The header is already sent, so we can't send a new error code.
		// We can log the error.
		slog.Error("failed to encode success response for delete", "error", err)
	}
}

// createWebhook handles HTTP requests to create a new webhook for a given topic slug.
// It expects the topic slug as a URL path parameter and a JSON request body containing the webhook URL.
// The function validates the input, ensures the topic exists, and inserts the webhook into the database.
// On success, it returns a 201 Created response with a JSON payload indicating success.
// Possible error responses include 400 Bad Request for invalid input, 404 Not Found if the topic does not exist,
// and 500 Internal Server Error for database or transaction failures.
func (app *App) createWebhook(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if slug == "" {
		http.Error(w, "topic slug must be provided in the URL path", http.StatusBadRequest)
		return
	}

	type requestBody struct {
		URL string `json:"url"`
	}

	var req requestBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if _, err := url.ParseRequestURI(req.URL); err != nil {
		http.Error(w, "invalid url format", http.StatusBadRequest)
		return
	}

	tx, err := app.pool.Begin(r.Context())
	if err != nil {
		slog.Error("failed to begin transaction", "error", err)
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	const query = "INSERT INTO webhooks (topic, url) VALUES ($1, $2)"
	_, err = tx.Exec(r.Context(), query, slug, req.URL)
	if err != nil {
		var pgErr *pgconn.PgError
		// Check if the error is a PostgreSQL foreign key violation (code 23503),
		// which means the topic slug does not exist.
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			http.Error(w, "topic not found", http.StatusNotFound)
			return
		}
		slog.Error("failed to create webhook", "error", err, "slug", slug, "url", req.URL)
		http.Error(w, "failed to create webhook", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit transaction", "error", err)
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	response := map[string]bool{"success": true}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("failed to encode success response for webhook creation", "error", err)
	}
}

// getWebhooksForTopic handles HTTP requests to retrieve a paginated list of webhooks for a specific topic.
// The topic is identified by the "slug" path parameter. Supports optional "offset" and "limit" query parameters
// for pagination, with defaults of 0 and 10 respectively. The "limit" must be between 1 and 100.
// Responds with a JSON object containing an array of webhooks, each with "id", "url", and "createdAt" fields.
// Returns HTTP 400 for invalid parameters, 500 for internal errors.
func (app *App) getWebhooksForTopic(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if slug == "" {
		http.Error(w, "topic slug must be provided in the URL path", http.StatusBadRequest)
		return
	}

	// --- Parameter Parsing & Validation ---
	type reqQueryParams struct {
		Offset int `json:"offset"`
		Limit  int `json:"limit"`
	}
	params := reqQueryParams{
		Offset: 0,
		Limit:  10, // Default limit
	}
	if r.URL.Query().Get("offset") != "" {
		if parsedOffset, err := strconv.Atoi(r.URL.Query().Get("offset")); err == nil {
			params.Offset = parsedOffset
		}
	}
	if r.URL.Query().Get("limit") != "" {
		if parsedLimit, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil {
			params.Limit = parsedLimit
		}
	}
	if params.Limit <= 0 || params.Limit > 100 {
		http.Error(w, "limit must be between 1 and 100", http.StatusBadRequest)
		return
	}
	if params.Offset < 0 {
		http.Error(w, "offset cannot be negative", http.StatusBadRequest)
		return
	}

	// --- Database Operation ---
	tx, err := app.pool.BeginTx(r.Context(), pgx.TxOptions{
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		slog.Error("failed to begin read-only transaction", "error", err)
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	// This query now aggregates results into a single JSON string,
	// matching the expectation of the PerformJSONQuery function.
	const query = `
	SELECT COALESCE(json_agg(
		json_build_object(
			'id', w.id,
			'url', w.url,
			'createdAt', w.created_at
		)
	), '[]'::json)
	FROM (
		SELECT id, url, created_at
		FROM webhooks
		WHERE topic = $1
		ORDER BY created_at DESC
		LIMIT $2
		OFFSET $3
	) AS w;`

	type webhookResponse struct {
		ID        int       `json:"id"`
		URL       string    `json:"url"`
		CreatedAt time.Time `json:"createdAt"`
	}

	// Use the generic query function to get the results.
	webhooks, err := PerformJSONQuery[[]webhookResponse](r.Context(), tx, query, slug, params.Limit, params.Offset)
	if err != nil {
		slog.Error("failed to get webhooks", "error", err, "slug", slug)
		http.Error(w, "failed to get webhooks", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit read-only transaction", "error", err)
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	// --- HTTP Response ---
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(map[string]any{
		"data": webhooks,
	}); err != nil {
		slog.Error("failed to encode response", "error", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

// deleteWebhook handles HTTP DELETE requests to remove a webhook associated with a specific topic.
// It expects the topic slug and webhook ID to be provided as URL path parameters.
// The function validates the input, ensures the webhook belongs to the specified topic, and deletes it from the database.
// If the deletion is successful, it returns a 202 Accepted response with a JSON body indicating success.
// If the webhook does not exist or does not belong to the topic, it returns a 404 Not Found.
// On input validation errors or database failures, it returns appropriate HTTP error responses.
func (app *App) deleteWebhook(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if slug == "" {
		http.Error(w, "topic slug must be provided in the URL path", http.StatusBadRequest)
		return
	}

	webhookIdStr := r.PathValue("webhookId")
	if webhookIdStr == "" {
		http.Error(w, "webhook ID must be provided in the URL path", http.StatusBadRequest)
		return
	}

	webhookId, err := strconv.Atoi(webhookIdStr)
	if err != nil {
		http.Error(w, "invalid webhook ID format, must be an integer", http.StatusBadRequest)
		return
	}

	tx, err := app.pool.Begin(r.Context())
	if err != nil {
		slog.Error("failed to begin transaction", "error", err)
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	// Delete the webhook from the database, ensuring it belongs to the correct topic.
	// This prevents deleting a webhook from another topic by just knowing its ID.
	cmdTag, err := tx.Exec(r.Context(), "DELETE FROM webhooks WHERE id = $1 AND topic = $2", webhookId, slug)
	if err != nil {
		slog.Error("failed to delete webhook", "webhookId", webhookId, "slug", slug, "error", err)
		http.Error(w, "failed to delete webhook", http.StatusInternalServerError)
		return
	}

	// Check if any row was actually deleted. If not, the webhook ID either
	// didn't exist or didn't belong to the specified topic.
	if cmdTag.RowsAffected() == 0 {
		http.Error(w, "webhook not found for the given topic", http.StatusNotFound)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit transaction", "error", err)
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	// Per previous requirements, return 202 Accepted for successful deletion.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := map[string]bool{"success": true}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("failed to encode success response for webhook deletion", "error", err)
	}
}

// createMessage handles HTTP POST requests to publish a new message to a specific topic.
// It expects the topic slug as a URL path parameter and a JSON object as the request body.
// The function validates the presence of the topic slug and ensures the payload is a non-empty JSON object.
// The payload is stored in the database as a JSON string associated with the specified topic.
// If the topic does not exist, it returns a 404 Not Found error. On success, it responds with 202 Accepted.
// Any internal errors are logged and result in appropriate HTTP error responses.
func (app *App) createMessage(w http.ResponseWriter, r *http.Request) {
	slug := r.PathValue("slug")
	if slug == "" {
		http.Error(w, "topic slug must be provided in the URL path", http.StatusBadRequest)
		return
	}

	// Decode the JSON payload from the request body into a map.
	// This allows for accepting any valid JSON object as the payload.
	var payload map[string]any
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	// The payload must not be empty.
	if len(payload) == 0 {
		http.Error(w, "message payload cannot be empty", http.StatusBadRequest)
		return
	}

	// Marshal the payload back into a JSON string for database storage.
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		slog.Error("failed to marshal payload for database", "error", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	tx, err := app.pool.Begin(r.Context())
	if err != nil {
		slog.Error("failed to begin transaction", "error", err)
		http.Error(w, "failed to begin transaction", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(r.Context())

	// Insert the message into the database.
	const query = "INSERT INTO messages (topic, payload) VALUES ($1, $2)"
	_, err = tx.Exec(r.Context(), query, slug, payloadBytes)
	if err != nil {
		var pgErr *pgconn.PgError
		// Check for a foreign key violation, which indicates the topic does not exist.
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			http.Error(w, "topic not found", http.StatusNotFound)
			return
		}
		slog.Error("failed to insert message", "error", err, "slug", slug)
		http.Error(w, "failed to publish message", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		slog.Error("failed to commit transaction", "error", err)
		http.Error(w, "failed to commit transaction", http.StatusInternalServerError)
		return
	}

	// Return 202 Accepted to indicate the message has been received for processing.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	response := map[string]bool{"success": true}
	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("failed to encode success response for message publishing", "error", err)
	}
}

type TopicData struct {
	Topic        string    `json:"topic"`
	MessagesList []Message `json:"messages_list"`
	Webhooks     []Webhook `json:"webhooks"`
}

type Message struct {
	ID        int64           `json:"id"`
	Payload   json.RawMessage `json:"payload"` // Use json.RawMessage for arbitrary JSON
	CreatedAt time.Time       `json:"created_at"`
}

type Webhook struct {
	URL string `json:"url"`
}

const fetchPendingMessagesQuery = `
WITH selected_batch AS (
    -- This is the exact query you provided
    SELECT
        id,
        topic,
        payload,
        created_at
    FROM messages
    ORDER BY created_at ASC
    LIMIT 100
    FOR UPDATE SKIP LOCKED
),
grouped_by_topics as (
    SELECT
        topic,
        -- This aggregates all rows for a given topic_id into a single JSON array
        json_agg(
            -- This builds a JSON object for each individual message
            json_build_object(
                'id', id,
                'payload', payload,
                'created_at', created_at
            )
            -- This ensures the messages inside the JSON array are also ordered
            ORDER BY created_at ASC
        ) AS messages_list
    FROM selected_batch
    GROUP BY topic
),
webhooks_grouped_by_topics as (
    SELECT 
        w.topic, 
        json_agg(
            json_build_object(
                'url', w.url
            )
        ) as webhooks
    FROM webhooks w
    WHERE w.topic in (
        select
            topic
        from grouped_by_topics
    )
    group by w.topic
),
resp as (
	select
	    gbt.topic,
	    gbt.messages_list,
	    COALESCE(wgbt.webhooks, '[]'::JSON) as webhooks
	from grouped_by_topics gbt
	left join webhooks_grouped_by_topics wgbt
	on wgbt.topic = gbt.topic
)
select coalesce(json_agg(w), '[]'::JSON)
from resp w;
`

// PublishMesssages periodically fetches pending messages from the database,
// sends them to their respective webhooks, and deletes successfully processed
// messages. It runs in a loop, triggered by a ticker every second, and manages
// database transactions for each iteration. If the context is cancelled, the
// function gracefully shuts down. Errors during processing, transaction
// management, or webhook delivery are logged appropriately.
func (app *App) PublishMesssages(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := processPendingMessages(ctx, app); err != nil {
				slog.Error("failed to process messages", "error", err)
			}
		case <-ctx.Done():
			slog.Info("shutting down message publishing goroutine")
			return
		}
	}
}

func processPendingMessages(ctx context.Context, app *App) error {
	tx, err := app.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			slog.Error("failed to rollback transaction", "error", rollbackErr)
		}
	}()

	// Fetch pending messages and webhooks for each topic
	resp, err := PerformJSONQuery[[]TopicData](ctx, tx, fetchPendingMessagesQuery)
	if err != nil {
		return fmt.Errorf("failed to fetch pending messages: %w", err)
	}

	if len(resp) > 0 {
		processedIds := []int64{}

		for _, td := range resp {
			newProcessedIds, err := app.sendToWebhook(ctx, td)
			if err != nil {
				slog.Error("failed to send messages to webhook", "error", err, "topic", td.Topic)
				continue
			}
			processedIds = append(processedIds, newProcessedIds...)
		}

		// Delete processed messages from the database
		if len(processedIds) > 0 {
			deleteQuery := "DELETE FROM messages WHERE id = ANY($1)"
			if _, err := tx.Exec(ctx, deleteQuery, processedIds); err != nil {
				return fmt.Errorf("failed to delete processed messages: %w", err)
			}
			slog.Info("deleted processed messages", "count", len(processedIds))
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// sendToWebhook sends the list of messages for a given topic to all configured webhooks associated with that topic.
// It iterates through each webhook, constructs a JSON payload containing the topic and messages, and sends it via HTTP POST.
// If any webhook URL is empty, or if there is an error during marshaling, request creation, or sending, the function returns an error.
// If all webhooks respond with a 2xx status code, it returns a slice of IDs of the processed messages.
// Parameters:
//   - ctx: The context for controlling cancellation and timeouts.
//   - td:  The TopicData containing the topic, messages, and associated webhooks.
//
// Returns:
//   - A slice of int64 IDs representing the processed messages.
//   - An error if any step fails.
func (app *App) sendToWebhook(ctx context.Context, td TopicData) ([]int64, error) {
	processedIds := []int64{}

	// loop through each webhook for the topic and send the message list
	for _, webhook := range td.Webhooks {
		if webhook.URL == "" {
			return nil, fmt.Errorf("webhook URL cannot be empty for topic %s", td.Topic)
		}

		// Prepare the payload to send
		payload := map[string]any{
			"topic":    td.Topic,
			"messages": td.MessagesList,
		}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal payload for webhook %s: %w", webhook.URL, err)
		}

		// Send the payload to the webhook URL
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook.URL, bytes.NewBuffer(payloadBytes))
		if err != nil {
			return nil, fmt.Errorf("failed to create request for webhook %s: %w", webhook.URL, err)
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request to webhook %s: %w", webhook.URL, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("webhook %s returned non-2xx status code: %d", webhook.URL, resp.StatusCode)
		}

		slog.Info("successfully sent messages to webhook", "webhook", webhook.URL, "topic", td.Topic)
	}

	// If we reach here, it means we have processed all messages for the topic.
	// Return the IDs of the processed messages.
	for _, message := range td.MessagesList {
		processedIds = append(processedIds, message.ID)
	}

	return processedIds, nil
}
