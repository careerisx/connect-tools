package server

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"connect-tools/internal/api"
)

const (
	loginTicketLimit     = 32
	loginTicketTTL       = 60 * time.Second
	browserSessionLimit  = 32
	browserSessionTTL    = 12 * time.Hour
	browserSessionMaxAge = int(browserSessionTTL / time.Second)
	loginIDBytes         = 32
)

type loginManager struct {
	mu       sync.Mutex
	address  string
	tickets  map[string]time.Time
	sessions map[string]time.Time
}

func newLoginManager(address string) *loginManager {
	return &loginManager{
		address:  address,
		tickets:  make(map[string]time.Time),
		sessions: make(map[string]time.Time),
	}
}

// CreateBrowserLoginURL creates a cryptographically random, single-use login
// URL that expires after 60 seconds.
func (m *loginManager) CreateBrowserLoginURL() (string, error) {
	now := time.Now()

	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeExpiredTickets(now)
	if len(m.tickets) >= loginTicketLimit {
		return "", api.ErrBrowserLoginCapacity
	}

	ticket, err := m.uniqueID(m.tickets)
	if err != nil {
		return "", fmt.Errorf("generate browser login ticket: %w", err)
	}
	m.tickets[ticket] = now.Add(loginTicketTTL)

	loginURL := url.URL{
		Scheme: "http",
		Host:   m.address,
		Path:   "/login",
	}
	query := loginURL.Query()
	query.Set("ticket", ticket)
	loginURL.RawQuery = query.Encode()
	return loginURL.String(), nil
}

// ValidBrowserSession reports whether sessionID was issued by a successful
// login exchange in this process.
func (m *loginManager) ValidBrowserSession(sessionID string) bool {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removeExpiredSessions(now)
	expiresAt, ok := m.sessions[sessionID]
	return sessionID != "" && ok && now.Before(expiresAt)
}

func (m *loginManager) handler() http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Referrer-Policy", "no-referrer")
		if request.Method != http.MethodGet {
			response.Header().Set("Allow", http.MethodGet)
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if request.Host != m.address {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}

		sessionID, sessionExpiresAt, err := m.consumeTicket(request.URL.Query().Get("ticket"), time.Now())
		if err != nil {
			http.Error(response, "internal server error", http.StatusInternalServerError)
			return
		}
		if sessionID == "" {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}

		http.SetCookie(response, &http.Cookie{
			Name:     api.BrowserSessionCookieName,
			Value:    sessionID,
			Path:     "/",
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			MaxAge:   browserSessionMaxAge,
			Expires:  sessionExpiresAt,
		})
		http.Redirect(response, request, "/", http.StatusSeeOther)
	})
}

func (m *loginManager) browserOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Host != m.address {
			http.Error(response, "forbidden", http.StatusForbidden)
			return
		}
		sessionID, ok := singleBrowserSessionCookie(request)
		if !ok || !m.ValidBrowserSession(sessionID) {
			http.Error(response, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(response, request)
	})
}

func (m *loginManager) consumeTicket(ticket string, now time.Time) (string, time.Time, error) {
	if ticket == "" {
		return "", time.Time{}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	expiresAt, ok := m.tickets[ticket]
	if !ok {
		return "", time.Time{}, nil
	}
	delete(m.tickets, ticket)
	if !now.Before(expiresAt) {
		return "", time.Time{}, nil
	}

	sessionID, err := m.uniqueSessionID()
	if err != nil {
		return "", time.Time{}, fmt.Errorf("generate browser session: %w", err)
	}
	m.removeExpiredSessions(now)
	if len(m.sessions) >= browserSessionLimit {
		m.removeEarliestSession()
	}
	sessionExpiresAt := now.Add(browserSessionTTL)
	m.sessions[sessionID] = sessionExpiresAt
	return sessionID, sessionExpiresAt, nil
}

func (m *loginManager) removeExpiredTickets(now time.Time) {
	for ticket, expiresAt := range m.tickets {
		if !now.Before(expiresAt) {
			delete(m.tickets, ticket)
		}
	}
}

func (m *loginManager) removeExpiredSessions(now time.Time) {
	for sessionID, expiresAt := range m.sessions {
		if !now.Before(expiresAt) {
			delete(m.sessions, sessionID)
		}
	}
}

func (m *loginManager) removeEarliestSession() {
	var earliestID string
	var earliestExpiry time.Time
	for sessionID, expiresAt := range m.sessions {
		if earliestID == "" || expiresAt.Before(earliestExpiry) ||
			(expiresAt.Equal(earliestExpiry) && sessionID < earliestID) {
			earliestID = sessionID
			earliestExpiry = expiresAt
		}
	}
	if earliestID != "" {
		delete(m.sessions, earliestID)
	}
}

func (m *loginManager) uniqueID(existing map[string]time.Time) (string, error) {
	for {
		value, err := randomLoginID()
		if err != nil {
			return "", err
		}
		if _, found := existing[value]; !found {
			return value, nil
		}
	}
}

func (m *loginManager) uniqueSessionID() (string, error) {
	for {
		value, err := randomLoginID()
		if err != nil {
			return "", err
		}
		if _, found := m.sessions[value]; !found {
			return value, nil
		}
	}
}

func singleBrowserSessionCookie(request *http.Request) (string, bool) {
	var sessionID string
	found := false
	for _, cookie := range request.Cookies() {
		if cookie.Name != api.BrowserSessionCookieName {
			continue
		}
		if found {
			return "", false
		}
		sessionID = cookie.Value
		found = true
	}
	return sessionID, found
}

func randomLoginID() (string, error) {
	raw := make([]byte, loginIDBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
