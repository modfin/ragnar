package web

import (
	"context"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/modfin/ragnar/internal/auth"
	"github.com/modfin/ragnar/internal/dao"
)

var GetRequestID = middleware.GetReqID

const AuthContextKey = "authorization_bearer"

// Authoization flow
// Bearer is extracted and added to the context
// Bearer may be a AccessKey (rag_xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
// Bearer may be a JWT token that is signed with a the AccessKey.
//        It so the AccessKey is added to the context once the JWT is verified

// AddAuthorizationBearer is a middleware that adds the value of the Authorization
// header with the prefix "Bearer " to the context of each request
func AddAuthorizationBearer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if strings.HasPrefix(authHeader, "Bearer ") {
			bearer := strings.TrimPrefix(authHeader, "Bearer ")
			r = r.WithContext(context.WithValue(r.Context(), AuthContextKey, bearer))
		}
		next.ServeHTTP(w, r)
	})

}

// AddAccessKey is a middleware that extracts the AccessKey from the Authorization Bearer
func AddAccessKey(log *slog.Logger, db *dao.DAO) func(next http.Handler) http.Handler {
	var accessTokenRegex = regexp.MustCompile("^(rag_)[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
	isAccessToken := accessTokenRegex.MatchString

	getAuthorizationBearer := func(ctx context.Context) (string, bool) {
		s, ok := ctx.Value(AuthContextKey).(string)
		return s, ok
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bearer, ok := getAuthorizationBearer(r.Context())
			if !ok {
				next.ServeHTTP(w, r)
				return
			}

			// This is an access token format. Lets add it to the context and validate it in the next middleware
			if len(bearer) == 4+36 && strings.HasPrefix(bearer, "rag_") && isAccessToken(bearer) {
				r = r.WithContext(context.WithValue(r.Context(), auth.ACCESS_KEY, bearer))
				next.ServeHTTP(w, r)
				return
			}

			header, payload, err := auth.UnmarshalJWT(bearer)
			if err != nil {
				log.Warn("failed to unmarshal JWT", "error", err)
				next.ServeHTTP(w, r)
				return
			}
			// Check if kid is present
			if header.Kid == "" {
				log.Warn("kid not found in JWT header", "header", header)
				next.ServeHTTP(w, r)
				return
			}
			now := time.Now().Unix()
			// Check if access token is valid
			if payload.IssuedAt-10 > now { // 10 seconds slack for clock drift
				log.Warn("access token is not yet valid", "kid", header.Kid, "iat", time.Unix(payload.IssuedAt, 0))
				next.ServeHTTP(w, r)
			}
			// Check if access token is expired
			if payload.ExpiresAt < now {
				log.Warn("access token is expired", "kid", header.Kid, "exp", time.Unix(payload.ExpiresAt, 0))
				next.ServeHTTP(w, r)
				return
			}

			token, err := db.GetAccessTokenFromKeyID(r.Context(), header.Kid)
			if err != nil {
				log.Warn("failed to get access token", "kid", header.Kid, "error", err)
				next.ServeHTTP(w, r)
				return
			}
			if token == nil {
				log.Warn("access token not found", "kid", header.Kid)
				next.ServeHTTP(w, r)
				return
			}

			// Verify JWT signature using the access token as HMAC key
			err = auth.VerifyJWT(token.AccessKey, bearer)
			if err != nil {
				log.Warn("JWT signature verification failed", "kid", header.Kid, "error", err)
				next.ServeHTTP(w, r)
				return
			}

			r = r.WithContext(context.WithValue(r.Context(), auth.ACCESS_KEY, token.AccessKey))
			next.ServeHTTP(w, r)
		})
	}
}

func QueryParam(name string) func(*http.Request) string {
	return func(r *http.Request) string {
		return r.URL.Query().Get(name)
	}
}
func PathParam(name string) func(*http.Request) string {
	return func(r *http.Request) string {
		return chi.URLParam(r, name)
	}
}

func AuthenticateTubAccess(log *slog.Logger, db *dao.DAO, getTubName func(*http.Request) string, operation ...auth.ACLOperation) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			tub := getTubName(r)
			if tub == "" {
				http.Error(w, "no tub was provided", http.StatusUnauthorized)
				return
			}

			_, ok := auth.GetAccessKey(r.Context())
			if !ok {
				http.Error(w, "no access token was provided", http.StatusUnauthorized)
				return
			}

			err := db.AllowedTubOperation(r.Context(), tub, operation...)
			if err != nil {
				log.Warn("access key is not allowed", "tub", tub, "error", err)
				http.Error(w, "access key is not allowed", http.StatusUnauthorized)
				return
			}

			r = r.WithContext(context.WithValue(r.Context(), "authenticated_tub", tub))

			next.ServeHTTP(w, r)

		})
	}
}

func AuthenticateAccess(log *slog.Logger, db *dao.DAO, operation ...auth.ACLOperation) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, ok := auth.GetAccessKey(r.Context())
			if !ok {
				http.Error(w, "no access token was provided", http.StatusUnauthorized)
				return
			}

			err := db.AllowedOperation(r.Context(), operation...)
			if err != nil {
				log.Warn("access key is not allowed", "error", err)
				http.Error(w, "access key is not allowed", http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
