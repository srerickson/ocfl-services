// Package oauth provides OAuth2 authentication middleware for Google login.
package oauth

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// Config holds OAuth configuration.
type Config struct {
	ClientID     string
	ClientSecret string
	RedirectURL  string // e.g., "http://localhost:8283/auth/callback"
	SessionKey   []byte // Key for HMAC signing session cookies (if nil, derives from ClientSecret)
}

// User represents an authenticated user.
type User struct {
	ID      string `json:"id"`
	Email   string `json:"email"`
	Name    string `json:"name"`
	Picture string `json:"picture"`
}

type contextKey string

const userContextKey contextKey = "oauth_user"

// UserFromContext retrieves the authenticated user from the request context.
// Returns nil if no user is authenticated.
func UserFromContext(ctx context.Context) *User {
	user, _ := ctx.Value(userContextKey).(*User)
	return user
}

// Middleware provides OAuth2 authentication.
type Middleware struct {
	oauthConfig  *oauth2.Config
	sessionKey   []byte
	cookieName   string
	cookieMaxAge int
}

// NewMiddleware creates a new OAuth middleware.
func NewMiddleware(cfg Config) *Middleware {
	oauthConfig := &oauth2.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		RedirectURL:  cfg.RedirectURL,
		Scopes: []string{
			"https://www.googleapis.com/auth/userinfo.email",
			"https://www.googleapis.com/auth/userinfo.profile",
		},
		Endpoint: google.Endpoint,
	}

	// Use provided session key or derive from client secret
	sessionKey := cfg.SessionKey
	if sessionKey == nil {
		h := sha256.Sum256([]byte(cfg.ClientSecret + "_session_key"))
		sessionKey = h[:]
	}

	return &Middleware{
		oauthConfig:  oauthConfig,
		sessionKey:   sessionKey,
		cookieName:   "ocfl_session",
		cookieMaxAge: 86400 * 7, // 7 days
	}
}

// RegisterHandlers registers OAuth-related HTTP handlers.
func (m *Middleware) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /auth/login", m.handleLogin)
	mux.HandleFunc("GET /auth/callback", m.handleCallback)
	mux.HandleFunc("GET /auth/logout", m.handleLogout)
}

// Wrap wraps a handler to extract user info from session cookie.
// It does NOT enforce authentication - it just populates context with user if present.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := m.getUserFromCookie(r)
		if user != nil {
			ctx := context.WithValue(r.Context(), userContextKey, user)
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	})
}

// RequireAuth returns middleware that enforces authentication.
// Unauthenticated requests receive a 401 Unauthorized response.
func (m *Middleware) RequireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := UserFromContext(r.Context())
		if user == nil {
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (m *Middleware) handleLogin(w http.ResponseWriter, r *http.Request) {
	// Generate random state
	state := generateState()
	
	// Store state in cookie for validation
	http.SetCookie(w, &http.Cookie{
		Name:     "oauth_state",
		Value:    state,
		Path:     "/",
		MaxAge:   300, // 5 minutes
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteLaxMode,
	})

	// Store the return URL if provided
	if returnURL := r.URL.Query().Get("return"); returnURL != "" {
		http.SetCookie(w, &http.Cookie{
			Name:     "oauth_return",
			Value:    returnURL,
			Path:     "/",
			MaxAge:   300,
			HttpOnly: true,
			Secure:   r.TLS != nil,
			SameSite: http.SameSiteLaxMode,
		})
	}

	url := m.oauthConfig.AuthCodeURL(state, oauth2.AccessTypeOffline)
	http.Redirect(w, r, url, http.StatusTemporaryRedirect)
}

func (m *Middleware) handleCallback(w http.ResponseWriter, r *http.Request) {
	// Verify state
	stateCookie, err := r.Cookie("oauth_state")
	if err != nil {
		http.Error(w, "Missing state cookie", http.StatusBadRequest)
		return
	}
	if r.URL.Query().Get("state") != stateCookie.Value {
		http.Error(w, "Invalid state", http.StatusBadRequest)
		return
	}

	// Clear state cookie
	http.SetCookie(w, &http.Cookie{
		Name:   "oauth_state",
		Path:   "/",
		MaxAge: -1,
	})

	// Exchange code for token
	code := r.URL.Query().Get("code")
	token, err := m.oauthConfig.Exchange(r.Context(), code)
	if err != nil {
		http.Error(w, "Failed to exchange token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Fetch user info
	user, err := m.fetchUserInfo(r.Context(), token)
	if err != nil {
		http.Error(w, "Failed to fetch user info: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Store user in session cookie
	m.setUserCookie(w, r, user)

	// Redirect to return URL or home
	returnURL := "/"
	if returnCookie, err := r.Cookie("oauth_return"); err == nil {
		returnURL = returnCookie.Value
		// Clear return cookie
		http.SetCookie(w, &http.Cookie{
			Name:   "oauth_return",
			Path:   "/",
			MaxAge: -1,
		})
	}

	http.Redirect(w, r, returnURL, http.StatusTemporaryRedirect)
}

func (m *Middleware) handleLogout(w http.ResponseWriter, r *http.Request) {
	// Clear session cookie
	http.SetCookie(w, &http.Cookie{
		Name:   m.cookieName,
		Path:   "/",
		MaxAge: -1,
	})

	// Redirect to home
	http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
}

func (m *Middleware) fetchUserInfo(ctx context.Context, token *oauth2.Token) (*User, error) {
	client := m.oauthConfig.Client(ctx, token)
	resp, err := client.Get("https://www.googleapis.com/oauth2/v2/userinfo")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var user User
	if err := json.NewDecoder(resp.Body).Decode(&user); err != nil {
		return nil, err
	}
	return &user, nil
}

func (m *Middleware) setUserCookie(w http.ResponseWriter, r *http.Request, user *User) {
	data, _ := json.Marshal(user)
	encoded := base64.URLEncoding.EncodeToString(data)
	signed := m.signValue(encoded)

	http.SetCookie(w, &http.Cookie{
		Name:     m.cookieName,
		Value:    signed,
		Path:     "/",
		MaxAge:   m.cookieMaxAge,
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteLaxMode,
	})
}

func (m *Middleware) getUserFromCookie(r *http.Request) *User {
	cookie, err := r.Cookie(m.cookieName)
	if err != nil {
		return nil
	}

	// Verify signature and extract payload
	payload, ok := m.verifyValue(cookie.Value)
	if !ok {
		return nil
	}

	data, err := base64.URLEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}

	var user User
	if err := json.Unmarshal(data, &user); err != nil {
		return nil
	}
	return &user
}

// signValue returns "payload.signature" where signature is HMAC-SHA256.
func (m *Middleware) signValue(payload string) string {
	mac := hmac.New(sha256.New, m.sessionKey)
	mac.Write([]byte(payload))
	sig := base64.URLEncoding.EncodeToString(mac.Sum(nil))
	return payload + "." + sig
}

// verifyValue checks the signature and returns the payload if valid.
func (m *Middleware) verifyValue(signed string) (string, bool) {
	parts := strings.SplitN(signed, ".", 2)
	if len(parts) != 2 {
		return "", false
	}
	payload, sig := parts[0], parts[1]

	// Recompute expected signature
	mac := hmac.New(sha256.New, m.sessionKey)
	mac.Write([]byte(payload))
	expectedSig := base64.URLEncoding.EncodeToString(mac.Sum(nil))

	// Constant-time comparison to prevent timing attacks
	if !hmac.Equal([]byte(sig), []byte(expectedSig)) {
		return "", false
	}
	return payload, true
}

func generateState() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%d", b, time.Now().Unix())
}
