package server

import (
	"log/slog"
	"net/http"
	"time"
)

// loggingMiddleware logs HTTP requests with method, path, status code and
// duration
func loggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			recorder := &responseRecorder{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}
			next.ServeHTTP(recorder, r)
			duration := time.Since(start)
			logger.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", recorder.statusCode,
				"duration", duration,
				"remote_addr", r.RemoteAddr,
			)
		})
	}
}

// responseRecorder wraps http.ResponseWriter to capture status code
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (r *responseRecorder) Unwrap() http.ResponseWriter {
	return r.ResponseWriter
}

func (r *responseRecorder) WriteHeader(statusCode int) {
	r.statusCode = statusCode
	r.ResponseWriter.WriteHeader(statusCode)
}
