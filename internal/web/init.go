package web

import (
	"context"
	"errors"
	"fmt"
	"github.com/modfin/ragnar/internal/ai"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/auth"
	"github.com/modfin/ragnar/internal/dao"
	"github.com/modfin/ragnar/internal/dao/docket"
	"github.com/modfin/ragnar/internal/storage"
	"github.com/modfin/strut"
	"github.com/modfin/strut/with"
	slogchi "github.com/samber/slog-chi"
)

type Config struct {
	HttpPort        int    `cli:"http-port"`
	HttpURI         string `cli:"http-uri"`
	HttpUploadLimit int64  `cli:"http-upload-limit"`
}

// Web holds application-wide dependencies, like the database connection pool.
type Web struct {
	cfg    Config
	db     *dao.DAO
	srv    *http.Server
	stor   *storage.Storage
	log    *slog.Logger
	docket *docket.Docket
	ai     *ai.AI
}

func (web *Web) Name() string {
	return "web"
}

func (web *Web) Close(ctx context.Context) error {
	return web.srv.Shutdown(ctx)
}

func New(log *slog.Logger, db *dao.DAO, stor *storage.Storage, docket *docket.Docket, ai *ai.AI, cfg Config) *Web {
	// Create a new chi router
	r := chi.NewRouter()
	web := &Web{
		cfg:    cfg,
		db:     db,
		stor:   stor,
		docket: docket,
		ai:     ai,
		log:    log,
	}

	// Create strut instance with logger and router
	s := strut.New(slog.Default(), r).
		Title("Ragnar API").
		Description("RAG API for retrieving internal information for AI use").
		Version("1.0.0").
		AddServer(web.cfg.HttpURI, "Development server")

	//Logging
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID, slogchi.NewWithFilters(web.log, slogchi.IgnorePath("/ping", "/ping/db")))
	r.Use(AddAuthorizationBearer, AddAccessKey(web.log, web.db))

	strut.Get(
		s.With(AuthenticateAccess(log, db, auth.ALLOW_READ)),
		"/tubs",
		web.ListTubs,
		with.OperationId("list-tubs"),
		with.Description("List tubs"),
		with.ResponseDescription(200, "contains a list of available tubs for the access token"),
	)
	strut.Post(
		s.With(AuthenticateAccess(log, db, auth.ALLOW_CREATE)),
		"/tubs",
		web.CreateTub,
		with.OperationId("create-tub"),
		with.Description("Create a tub"),
		with.ResponseDescription(201, "returns information about the newly created tub"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}",
		web.GetTub,
		with.OperationId("get-tub"),
		with.Description("Get tub information"),
		with.PathParam[string]("tub", "Document tub - the table/bucket fetch"),
		with.ResponseDescription(200, "list information about the tub"),
	)

	strut.Put(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_UPDATE)),
		"/tubs/{tub}",
		web.UpdateTub,
		with.OperationId("update-tub"),
		with.Description("Update embedding model and chunking strategy settings for a tub"),
		with.PathParam[string]("tub", "Document tub - the table/bucket to store document in"),
		with.ResponseDescription(200, "Successfully updated tub settings"),
	)
	strut.Delete(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_DELETE)),
		"/tubs/{tub}",
		web.DeleteTub,
		with.OperationId("delete-tub"),
		with.Description("Delete a tub"),
		with.PathParam[string]("tub", "Document tub - the table/bucket to store document in"),
		with.ResponseDescription(200, "???"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}/documents",
		web.GetDocuments,
		with.OperationId("list-documents"),
		with.Description(`Get documents in a tub with optional filtering.

Filter format supports:
- Equality: {"field": "value"}
- Contains (any): {"field": ["value1", "value2"]}
- Greater than: {"field": {"$gt": "value"}}
- Greater than or equal: {"field": {"$gte": "value"}}
- Less than: {"field": {"$lt": "value"}}
- Less than or equal: {"field": {"$lte": "value"}}
- Explicit equality: {"field": {"$eq": "value"}}

Type hints for numeric comparisons (optional):
- Integer: {"field": {"$gt": "10", "type": "integer"}}
- Numeric/Float: {"field": {"$gte": "3.14", "type": "numeric"}}
- Text (default): {"field": {"$lt": "value"}} or {"field": {"$lt": "value", "type": "text"}}

Without type hints, all comparisons are performed as text/string comparisons.
Use "integer" or "numeric" type hints for proper numeric comparisons.

Example: {"status": "active", "priority": {"$gte": "10", "type": "integer"}}`),
		with.PathParam[string]("tub", "the document tub"),
		with.QueryParam[string]("filter", "Optional filter query in JSON format with support for comparison operators ($eq, $gt, $gte, $lt, $lte) and array contains"),
		with.QueryParam[string]("sort", "Optional sorting of documents"),
		with.QueryParam[int]("limit", "Optional limit query"),
		with.QueryParam[int]("offset", "Optional offset query"),
		with.ResponseDescription(200, "List of found documents matching filter"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}/documents/{document_id}",
		web.GetDocument,
		with.OperationId("get-document"),
		with.Description("Get a specific document in a specific tub"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "The document id"),
		with.ResponseDescription(200, "Document object"),
	)

	strut.RawPost[[]byte, ragnar.Document](
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_CREATE)),
		"/tubs/{tub}/documents",
		web.UpsertDocument,
		with.OperationId("upload-document"),
		with.Description("Upload a document"),
		with.PathParam[string]("tub", "the document tub"),
		with.ResponseDescription(200, "Successfully uploaded document"),
	)

	strut.RawPut[[]byte, ragnar.Document](
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_UPDATE)),
		"/tubs/{tub}/documents/{document_id}",
		web.UpsertDocument,
		with.OperationId("update-document"),
		with.Description("Update a document"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "The document id to be updated"),
		with.ResponseDescription(200, "Successfully updated document"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}/documents/{document_id}/download",
		web.DownloadDocument,
		with.OperationId("download-document"),
		with.Description("Download raw document"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "The document id to be download"),
		with.ResponseDescription(200, "byte steam of document"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}/documents/{document_id}/download/markdown",
		web.DownloadDocumentMarkdown,
		with.OperationId("download-document"),
		with.Description("Download markdown document"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "The document id of which markdown to be download"),
		with.ResponseDescription(200, "byte steam of document"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}/documents/{document_id}/status",
		web.GetDocumentStatus,
		with.OperationId("document-status"),
		with.Description("Get the status of document processing"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "The document id which status will be fetched"),
		with.ResponseDescription(200, "the document status"),
	)

	strut.Delete(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_DELETE)),
		"/tubs/{tub}/documents/{document_id}",
		web.DeleteDocument,
		with.OperationId("delete-document"),
		with.Description("Delete a specific document"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "The document id to be deleted"),
		with.ResponseDescription(200, "confirmation of document deletion"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}/documents/{document_id}/chunks",
		web.GetChunks,
		with.OperationId("get-document-chunks"),
		with.Description("Get document chunks"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "document id"),
		with.QueryParam[int]("limit", "Optional limit query"),
		with.QueryParam[int]("offset", "Optional offset query"),
		with.ResponseDescription(200, "A list of chunks from the requested document"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/tubs/{tub}/document/{document_id}/chunks/{index}",
		web.GetChunk,
		with.OperationId("get-chunk"),
		with.Description("Get chunk at index"),
		with.PathParam[string]("tub", "the document tub"),
		with.PathParam[string]("document_id", "the document id"),
		with.PathParam[int]("index", "the chunk index to retrieve index"),
		with.ResponseDescription(200, "A specific chunk from the requested document"),
	)

	strut.Get(
		s.With(AuthenticateTubAccess(log, db, PathParam("tub"), auth.ALLOW_READ)),
		"/search/xnn/{tub}",
		web.SearchXNN,
		with.OperationId("vector-search"),
		with.Description("Search for chunks matching text prompt, with optional filtering"),
		with.PathParam[string]("tub", "the document tub"),
		with.QueryParam[string]("q", "free text search query"),
		with.QueryParam[int]("limit", "Optional limit query"),
		with.QueryParam[string]("filter", "Optional filter chunk documents query in flat JSON format"),
		with.QueryParam[int]("offset", "Optional offset query"),
		with.ResponseDescription(200, "The chunks best matching the search"),
	)

	//strut.Get(s, "/search/agent", web.SearchAgent,
	//	with.OperationId("agent-vector-search"),
	//	with.Description("AI agent prompting with RAG enhanced results"),
	//	with.QueryParam[[]string]("tub", "a list of tubs that the agent can access to search"),
	//	with.ResponseDescription(200, "The chunks best matching the search"),
	//)

	r.Get("/ping", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("pong"))
	})
	r.Get("/ping/db", func(w http.ResponseWriter, r *http.Request) {
		err := web.db.Ping()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte("pong"))
	})

	// Expose OpenAPI documentation
	r.Get("/.well-known/openapi.yaml", s.SchemaHandlerYAML)
	r.Get("/.well-known/openapi.json", s.SchemaHandlerJSON)

	web.srv = &http.Server{Addr: fmt.Sprintf(":%d", cfg.HttpPort), Handler: r}

	go func() {
		log.Info("Starting HTTP server", "port", cfg.HttpPort)
		if err := web.srv.ListenAndServe(); err != nil {
			if errors.Is(err, http.ErrServerClosed) {
				log.Info("HTTP server closed")
				return
			}
			log.Error("Failed to start HTTP server, killing process", "err", err)
		}
	}()

	return web
}
