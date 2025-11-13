package web

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/modfin/ragnar/internal/util"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/modfin/ragnar"
	"github.com/modfin/strut"
)

const headerPrefix = "x-ragnar-"

var whitelistedHeaders = map[string]bool{
	"content-type":        true,
	"content-length":      true,
	"content-disposition": true,
}

func (web *Web) UpsertDocument(w http.ResponseWriter, r *http.Request) {
	requestId := GetRequestID(r.Context())

	ctx := r.Context()
	tubname := chi.URLParam(r, "tub")
	if tubname == "" {
		web.log.Error("tub name is required", "request_id", requestId)
		http.Error(w, "tub name is required, request_id: "+requestId, http.StatusBadRequest)
		return
	}
	tub, err := web.db.GetTub(ctx, tubname)
	if err != nil {
		web.log.Error("error fetching tub", "err", err, "request_id", requestId)
		http.Error(w, "error fetching tub, request_id: "+requestId, http.StatusBadRequest)
		return
	}
	documentId := chi.URLParam(r, "document_id")

	headers := r.Header
	defer r.Body.Close()

	contentType := headers.Get("Content-Type")
	if contentType == "" {
		web.log.Error("Content-Type header is missing", "request_id", requestId)
		http.Error(w, "Content-Type header is missing", http.StatusBadRequest)
		return
	}

	// Check if this is multipart form data
	mediaType, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		web.log.Error("error parsing media type", "err", err, "request_id", requestId)
		http.Error(w, "error parsing media type, request_id: "+requestId, http.StatusBadRequest)
		return
	}

	requiredDocumentHeaders := tub.GetRequiredDocumentHeaders()
	for _, h := range requiredDocumentHeaders {
		if headers.Get(h) == "" && headers.Get(headerPrefix+h) == "" {
			web.log.Error("missing required document header", "header", h, "request_id", requestId)
			http.Error(w, "missing required document header: "+h+", request_id: "+requestId, http.StatusBadRequest)
			return
		}
	}

	if mediaType == "multipart/form-data" {
		web.handleMultipartUpsert(w, r, ctx, tub, documentId, params, requestId)
	} else {
		web.handleSingleFileUpsert(w, r, ctx, tub, documentId, requestId)
	}
}

func (web *Web) handleSingleFileUpsert(w http.ResponseWriter, r *http.Request, ctx context.Context, tub ragnar.Tub, documentId string, requestId string) {
	var err error
	headers := r.Header

	// Ensure that the request body is not too large
	reader := io.LimitReader(r.Body, web.cfg.HttpUploadLimit)
	if r.Header.Get("Content-Encoding") == "gzip" {
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			web.log.Error("error creating gzip reader", "err", err, "request_id", requestId)
			http.Error(w, "error creating gzip reader", http.StatusBadRequest)
			return
		}
	}

	contentType := headers.Get("Content-Type")
	contentDisposition := headers.Get("Content-Disposition")
	if contentDisposition == "" {
		contentDisposition = fmt.Sprintf("attachment; filename=\"%s\"", "file")
	}

	length := r.ContentLength

	// To deal with s3 crap and be able to stream a file of unknown size...
	if length == -1 {
		tmp, err := os.CreateTemp("", "ragnar-upload-")
		if err != nil {
			web.log.Error("failed to create temporary file", "err", err, "request_id", requestId)
			http.Error(w, "failed to create temporary file, request_id: "+requestId, http.StatusInternalServerError)
			return
		}
		defer os.Remove(tmp.Name())
		defer tmp.Close()
		length, err = io.Copy(tmp, reader)
		if err != nil {
			web.log.Error("failed to copy request body to temporary file", "err", err, "request_id", requestId)
			http.Error(w, "failed to copy request body to temporary file, request_id: "+requestId, http.StatusInternalServerError)
			return
		}

		_, err = tmp.Seek(0, 0)
		if err != nil {
			web.log.Error("failed to seek to beginning of temporary file", "err", err, "request_id", requestId)
			http.Error(w, "failed to seek to beginning of temporary file, request_id: "+requestId, http.StatusInternalServerError)
			return
		}
		reader = tmp
	}

	// Throws error if exactly at limit to avoid truncation
	if length >= web.cfg.HttpUploadLimit {
		web.log.Error("Request body is too large", "limit", web.cfg.HttpUploadLimit-1, "request_id", requestId)
		http.Error(w, fmt.Sprintf("Request body is too large, max %d bytes", web.cfg.HttpUploadLimit-1), http.StatusBadRequest)
		return
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		web.log.Error("failed to read request body", "err", err, "request_id", requestId)
		http.Error(w, "failed to read request body, request_id: "+requestId, http.StatusInternalServerError)
		return
	}
	seekableReader := bytes.NewReader(data)

	isNewDocument := documentId == ""
	doc := ragnar.Document{
		DocumentId: documentId,
		TubId:      tub.TubId,
		TubName:    tub.TubName,
		Headers: pgtype.Hstore{
			"content-type":        &contentType,
			"content-length":      util.Ptr(fmt.Sprintf("%d", length)),
			"content-disposition": &contentDisposition,
		},
	}

	documentHash, err := util.HashReaderSHA256(seekableReader)
	if err != nil {
		web.log.Error("failed to hash document", "err", err, "request_id", requestId)
		http.Error(w, "failed to hash document, request_id: "+requestId, http.StatusInternalServerError)
		return
	}
	// Reset reader after hashing
	_, err = seekableReader.Seek(0, 0)
	if err != nil {
		web.log.Error("failed to seek to beginning of reader after hashing", "err", err, "request_id", requestId)
		http.Error(w, "failed to seek to beginning of reader after hashing, request_id: "+requestId, http.StatusInternalServerError)
		return
	}

	for k, v := range headers {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, headerPrefix) {
			k = strings.TrimPrefix(k, headerPrefix)
			doc.Headers[k] = util.Ptr(v[0])
		}
	}

	doc, err = web.db.UpsertDocument(ctx, doc)
	if err != nil {
		web.log.Error("error creating document", "err", err, "request_id", requestId)
		http.Error(w, "error creating document, request_id: "+requestId, http.StatusInternalServerError)
		return
	}

	documentChanged, err := web.stor.PutDocument(ctx, doc.TubName, doc.DocumentId, seekableReader, length, doc.Headers, documentHash)
	if err != nil {
		web.log.Error("error putting document", "err", err, "request_id", requestId)
		http.Error(w, "error putting document, request_id: "+requestId, http.StatusInternalServerError)
		if isNewDocument {
			web.db.DeleteDocument(ctx, doc.TubName, doc.DocumentId) // try to rollback
		}
		return
	}

	if documentChanged {
		err = web.docket.ScheduleDocumentConversion(doc)
		if err != nil {
			web.log.Error("error scheduling document conversion", "err", err, "request_id", requestId)
			http.Error(w, "error scheduling document conversion, request_id: "+requestId, http.StatusInternalServerError)
			if isNewDocument {
				web.stor.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
				web.db.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
			}
			return
		}
	}

	if !isNewDocument && documentChanged {
		doc, err = web.db.SetDocumentUpdatedAtNow(ctx, doc.TubName, doc.DocumentId)
		if err != nil {
			web.log.Error("error updating document updated_at", "err", err, "request_id", requestId)
			http.Error(w, "error updating document updated_at, request_id: "+requestId, http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(doc)
	if err != nil {
		web.log.Error("error encoding document", "err", err, "request_id", requestId)
		http.Error(w, "error encoding document, request_id: "+requestId, http.StatusInternalServerError)
		return
	}
}

func (web *Web) handleMultipartUpsert(w http.ResponseWriter, r *http.Request, ctx context.Context, tub ragnar.Tub, documentId string, params map[string]string, requestId string) {
	var err error
	boundary, ok := params["boundary"]
	if !ok {
		web.log.Error("multipart boundary not found", "request_id", requestId)
		http.Error(w, "multipart boundary not found, request_id: "+requestId, http.StatusBadRequest)
		return
	}

	reader := r.Body
	if r.Header.Get("Content-Encoding") == "gzip" {
		reader, err = gzip.NewReader(r.Body)
		if err != nil {
			web.log.Error("error creating gzip reader", "err", err, "request_id", requestId)
			http.Error(w, "error creating gzip reader", http.StatusBadRequest)
			return
		}
	}

	multipartReader := multipart.NewReader(reader, boundary)
	defer reader.Close()

	var fileReader io.Reader
	var fileLength int64
	var fileContentType string
	var fileContentDisposition string
	var markdownReader io.Reader
	var markdownLength int64
	var chunks []ragnar.Chunk

	for {
		part, err := multipartReader.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			web.log.Error("error reading multipart", "err", err, "request_id", requestId)
			http.Error(w, "error reading multipart, request_id: "+requestId, http.StatusBadRequest)
			return
		}

		formName := part.FormName()
		switch formName {
		case "file":
			// Handle the main document file
			fileContentType = part.Header.Get("Content-Type")
			if fileContentType == "" {
				web.log.Error("file part must have Content-Type header", "request_id", requestId)
				http.Error(w, "file part must have Content-Type header, request_id: "+requestId, http.StatusBadRequest)
				part.Close()
				return
			}
			fileContentDisposition = part.Header.Get("Content-Disposition")
			if fileContentDisposition == "" {
				fileContentDisposition = fmt.Sprintf("attachment; filename=\"%s\"", part.FileName())
			}

			// Stream to temp file for length calculation
			tmp, err := os.CreateTemp("", "ragnar-upload-file-")
			if err != nil {
				web.log.Error("failed to create temporary file for document", "err", err, "request_id", requestId)
				http.Error(w, "failed to create temporary file, request_id: "+requestId, http.StatusInternalServerError)
				part.Close()
				return
			}
			defer os.Remove(tmp.Name())
			defer tmp.Close()

			fileLength, err = io.Copy(tmp, part)
			if err != nil {
				web.log.Error("failed to copy file part", "err", err, "request_id", requestId)
				http.Error(w, "failed to copy file part, request_id: "+requestId, http.StatusInternalServerError)
				part.Close()
				return
			}

			_, err = tmp.Seek(0, 0)
			if err != nil {
				web.log.Error("failed to seek file part", "err", err, "request_id", requestId)
				http.Error(w, "failed to seek file part, request_id: "+requestId, http.StatusInternalServerError)
				part.Close()
				return
			}

			fileReader = tmp

		case "markdown":
			// Handle the optional markdown content
			tmp, err := os.CreateTemp("", "ragnar-upload-markdown-")
			if err != nil {
				web.log.Error("failed to create temporary file for markdown", "err", err, "request_id", requestId)
				http.Error(w, "failed to create temporary file for markdown, request_id: "+requestId, http.StatusInternalServerError)
				part.Close()
				return
			}
			defer os.Remove(tmp.Name())
			defer tmp.Close()

			markdownLength, err = io.Copy(tmp, part)
			if err != nil {
				web.log.Error("failed to copy markdown part", "err", err, "request_id", requestId)
				http.Error(w, "failed to copy markdown part, request_id: "+requestId, http.StatusInternalServerError)
				part.Close()
				return
			}

			_, err = tmp.Seek(0, 0)
			if err != nil {
				web.log.Error("failed to seek markdown part", "err", err, "request_id", requestId)
				http.Error(w, "failed to seek markdown part, request_id: "+requestId, http.StatusInternalServerError)
				part.Close()
				return
			}

			markdownReader = tmp

		case "chunks":
			// Handle the optional chunks JSON
			var err error
			chunksData, err := io.ReadAll(part)
			if err != nil {
				web.log.Error("failed to read chunks part", "err", err, "request_id", requestId)
				http.Error(w, "failed to read chunks part, request_id: "+requestId, http.StatusInternalServerError)
				part.Close()
				return
			}

			// Validate JSON
			if err := json.Unmarshal(chunksData, &chunks); err != nil {
				web.log.Error("invalid chunks JSON", "err", err, "request_id", requestId)
				http.Error(w, "invalid chunks JSON, request_id: "+requestId, http.StatusBadRequest)
				part.Close()
				return
			}
			// make sure indexes are sorted and set on all chunks
			for i := range chunks {
				if chunks[i].ChunkId != i {
					web.log.Error("chunk ids must be sequential starting from 0", "request_id", requestId)
					http.Error(w, "chunk ids must be sequential starting from 0, request_id: "+requestId, http.StatusBadRequest)
					part.Close()
					return
				}
			}
		}

		part.Close()
	}

	if fileReader == nil {
		web.log.Error("file part is required", "request_id", requestId)
		http.Error(w, "file part is required, request_id: "+requestId, http.StatusBadRequest)
		return
	}

	// Check file size limit
	if fileLength >= web.cfg.HttpUploadLimit {
		web.log.Error("File is too large", "limit", web.cfg.HttpUploadLimit-1, "request_id", requestId)
		http.Error(w, fmt.Sprintf("File is too large, max %d bytes", web.cfg.HttpUploadLimit-1), http.StatusBadRequest)
		return
	}

	if len(chunks) > 0 && markdownReader == nil {
		web.log.Error("chunks provided but markdown part is missing", "request_id", requestId)
		http.Error(w, "chunks provided but markdown part is missing, request_id: "+requestId, http.StatusBadRequest)
		return
	}

	isNewDocument := documentId == ""
	doc := ragnar.Document{
		DocumentId: documentId,
		TubId:      tub.TubId,
		TubName:    tub.TubName,
		Headers: pgtype.Hstore{
			"content-type":        &fileContentType,
			"content-length":      util.Ptr(fmt.Sprintf("%d", fileLength)),
			"content-disposition": &fileContentDisposition,
		},
	}

	// Add custom headers
	headers := r.Header
	for k, v := range headers {
		k = strings.ToLower(k)
		if strings.HasPrefix(k, headerPrefix) {
			k = strings.TrimPrefix(k, headerPrefix)
			doc.Headers[k] = util.Ptr(v[0])
		}
	}

	documentHash, err := util.HashReaderSHA256(fileReader)
	if err != nil {
		web.log.Error("failed to hash document", "err", err, "request_id", requestId)
		http.Error(w, "failed to hash document, request_id: "+requestId, http.StatusInternalServerError)
		return
	}
	// Reset fileReader after hashing
	_, err = fileReader.(io.Seeker).Seek(0, 0)
	if err != nil {
		web.log.Error("failed to seek to beginning of reader after hashing", "err", err, "request_id", requestId)
		http.Error(w, "failed to seek to beginning of reader after hashing, request_id: "+requestId, http.StatusInternalServerError)
		return
	}

	// Store the document
	doc, err = web.db.UpsertDocument(ctx, doc)
	if err != nil {
		web.log.Error("error creating document", "err", err, "request_id", requestId)
		http.Error(w, "error creating document, request_id: "+requestId, http.StatusInternalServerError)
		return
	}

	// Store the file
	documentChanged, err := web.stor.PutDocument(ctx, doc.TubName, doc.DocumentId, fileReader, fileLength, doc.Headers, documentHash)
	if err != nil {
		web.log.Error("error putting document", "err", err, "request_id", requestId)
		http.Error(w, "error putting document, request_id: "+requestId, http.StatusInternalServerError)
		if isNewDocument {
			web.db.DeleteDocument(ctx, doc.TubName, doc.DocumentId) // try to rollback
		}
		return
	}

	// Store markdown if provided
	var markdownChanged bool
	if markdownReader != nil {
		markdownHash, err := util.HashReaderSHA256(markdownReader)
		if err != nil {
			web.log.Error("failed to hash document", "err", err, "request_id", requestId)
			http.Error(w, "failed to hash document, request_id: "+requestId, http.StatusInternalServerError)
			return
		}
		// Reset markdownReader after hashing
		_, err = markdownReader.(io.Seeker).Seek(0, 0)
		if err != nil {
			web.log.Error("failed to seek to beginning of reader after hashing", "err", err, "request_id", requestId)
			http.Error(w, "failed to seek to beginning of reader after hashing, request_id: "+requestId, http.StatusInternalServerError)
			return
		}
		markdownChanged, err = web.stor.PutDocumentMarkdown(ctx, doc.TubName, doc.DocumentId, markdownReader, markdownLength, doc.Headers, markdownHash)
		if err != nil {
			web.log.Error("error putting markdown", "err", err, "request_id", requestId)
			http.Error(w, "error putting markdown, request_id: "+requestId, http.StatusInternalServerError)
			if isNewDocument {
				web.stor.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
				web.db.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
			}
			return
		}
	}

	// Store chunks if provided
	var chunksChanged bool
	if len(chunks) > 0 {
		currentChunks, err := web.db.InternalGetChunks(doc)
		if err != nil {
			web.log.Error("error fetching current chunks", "err", err, "request_id", requestId)
			http.Error(w, "error fetching current chunks, request_id: "+requestId, http.StatusInternalServerError)
			if isNewDocument {
				web.stor.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
				web.db.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
			}
			return
		}
		chunksChanged = !util.ChunkSlicesContentEqual(currentChunks, chunks)
		if !chunksChanged {
			web.log.Info("uploaded chunks identical to existing chunks", "request_id", requestId)
		}
	}

	if len(chunks) > 0 && chunksChanged {
		// Delete old chunks if any
		err = web.db.DeleteChunks(ctx, doc)
		if err != nil {
			web.log.Error("error deleting old chunks", "err", err, "request_id", requestId)
		}
		var ragnarChunks []ragnar.Chunk
		for i, chunk := range chunks {
			ragnarChunks = append(ragnarChunks, ragnar.Chunk{
				ChunkId:    i,
				DocumentId: doc.DocumentId,
				TubId:      doc.TubId,
				TubName:    doc.TubName,
				Content:    chunk.Content,
			})
		}
		err = web.db.InternalInsertChunks(ragnarChunks)
		if err != nil {
			web.log.Error("failed to insert chunks", "error", err)
			http.Error(w, "error inserting chunks, request_id: "+requestId, http.StatusInternalServerError)
			if isNewDocument {
				web.stor.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
				web.db.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
			}
			return
		}
	}

	switch true {
	case len(chunks) > 0:
		if !chunksChanged {
			// No changes to chunks, no need to re-embed
			web.log.Info("chunks unchanged, skipping embedding", "request_id", requestId)
			break
		}
		err = web.docket.ScheduleChunkEmbedding(doc)
	case markdownReader != nil:
		if !markdownChanged {
			// No changes to markdown, no need to re-chunk
			web.log.Info("markdown unchanged, skipping chunking", "request_id", requestId)
			break
		}
		err = web.docket.ScheduleDocumentChunking(doc)
	default:
		if !documentChanged {
			// No changes to document, no need to re-convert
			web.log.Info("document unchanged, skipping conversion", "request_id", requestId)
			break
		}
		err = web.docket.ScheduleDocumentConversion(doc)
	}
	if err != nil {
		web.log.Error("error scheduling document conversion", "err", err, "request_id", requestId)
		http.Error(w, "error scheduling document conversion, request_id: "+requestId, http.StatusInternalServerError)
		if isNewDocument {
			web.stor.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
			web.db.DeleteDocument(ctx, doc.TubName, doc.DocumentId)
		}
		return
	}

	if !isNewDocument && (documentChanged || markdownChanged || chunksChanged) {
		doc, err = web.db.SetDocumentUpdatedAtNow(ctx, doc.TubName, doc.DocumentId)
		if err != nil {
			web.log.Error("error updating document updated_at", "err", err, "request_id", requestId)
			http.Error(w, "error updating document updated_at, request_id: "+requestId, http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusCreated)
	err = json.NewEncoder(w).Encode(doc)
	if err != nil {
		web.log.Error("error encoding document", "err", err, "request_id", requestId)
		http.Error(w, "error encoding document, request_id: "+requestId, http.StatusInternalServerError)
		return
	}
}

func (web *Web) GetDocuments(ctx context.Context) strut.Response[[]ragnar.Document] {
	requestId := GetRequestID(ctx)

	tub := strut.PathParam(ctx, "tub")
	filterstr := strut.QueryParam(ctx, "filter")
	if filterstr == "" {
		filterstr = "{}"
	}
	sortstr := strut.QueryParam(ctx, "sort")
	if sortstr == "" {
		sortstr = "[]"
	}
	limit, err := strconv.Atoi(strut.QueryParam(ctx, "limit"))
	if err != nil {
		limit = 100
	}
	offset, err := strconv.Atoi(strut.QueryParam(ctx, "offset"))
	if err != nil {
		offset = 0
	}

	var filter ragnar.DocumentFilter
	err = json.Unmarshal([]byte(filterstr), &filter)
	if err != nil {
		web.log.Error("Error unmarshalling filter json", "err", err, "request_id", requestId)
		return strut.RespondError[[]ragnar.Document](http.StatusBadRequest,
			fmt.Sprintf("Invalid JSON format in 'filter' query parameter, request_id: %s", requestId))
	}

	var sort ragnar.DocumentSort
	err = json.Unmarshal([]byte(sortstr), &sort)
	if err != nil {
		web.log.Error("Error unmarshalling sort json", "err", err, "request_id", requestId)
		return strut.RespondError[[]ragnar.Document](http.StatusBadRequest,
			fmt.Sprintf("Invalid JSON format in 'sort' query parameter, request_id: %s", requestId))
	}

	docs, err := web.db.ListDocuments(ctx, tub, filter, sort, limit, offset)
	if err != nil {
		web.log.Error("Error listing documents", "err", err, "request_id", requestId)
		return strut.RespondError[[]ragnar.Document](http.StatusInternalServerError,
			fmt.Sprintf("Error listing documents, request_id: %s", requestId))
	}

	return strut.RespondOk(docs)
}

func (web *Web) GetDocument(ctx context.Context) strut.Response[ragnar.Document] {
	requestId := GetRequestID(ctx)

	tub := strut.PathParam(ctx, "tub")
	documentId := strut.PathParam(ctx, "document_id")

	doc, err := web.db.GetDocument(ctx, tub, documentId)
	if err != nil {
		web.log.Error("Error fetching document", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Document](http.StatusBadRequest,
			fmt.Sprintf("Error fetching document, request_id: %s", requestId))
	}

	return strut.RespondOk(doc)
}

func (web *Web) DownloadDocument(ctx context.Context) strut.Response[[]byte] {
	requestId := GetRequestID(ctx)

	tub := strut.PathParam(ctx, "tub")
	documentId := strut.PathParam(ctx, "document_id")

	doc, err := web.db.GetDocument(ctx, tub, documentId)
	if err != nil {
		web.log.Error("Error fetching document", "err", err, "request_id", requestId)
		return strut.RespondError[[]byte](http.StatusBadRequest,
			fmt.Sprintf("Error fetching document, request_id: %s", requestId))
	}

	reader, err := web.stor.GetDocument(ctx, doc.TubName, doc.DocumentId)
	if err != nil {
		web.log.Error("Error getting document", "err", err, "request_id", requestId)
		return strut.RespondError[[]byte](http.StatusBadRequest,
			fmt.Sprintf("Error getting document, request_id: %s", requestId))
	}

	return strut.RespondFunc[[]byte](func(w http.ResponseWriter, r *http.Request) error {

		for k, v := range doc.Headers {
			if v == nil {
				continue
			}
			if !whitelistedHeaders[k] {
				k = "x-ragnar-" + k
			}
			w.Header().Set(k, *v)
		}

		w.WriteHeader(200)
		_, err = io.Copy(w, reader)
		return err
	})
}

func (web *Web) DownloadDocumentMarkdown(ctx context.Context) strut.Response[[]byte] {
	requestId := GetRequestID(ctx)

	tub := strut.PathParam(ctx, "tub")
	documentId := strut.PathParam(ctx, "document_id")

	doc, err := web.db.GetDocument(ctx, tub, documentId)
	if err != nil {
		web.log.Error("Error fetching document", "err", err, "request_id", requestId)
		return strut.RespondError[[]byte](http.StatusBadRequest,
			fmt.Sprintf("Error fetching document, request_id: %s", requestId))
	}

	reader, err := web.stor.GetDocumentMarkdown(ctx, doc.TubName, doc.DocumentId)
	if err != nil {
		web.log.Error("Error getting markdown document", "err", err, "request_id", requestId)
		return strut.RespondError[[]byte](http.StatusBadRequest,
			fmt.Sprintf("Error getting markdown document, request_id: %s", requestId))
	}

	return strut.RespondFunc[[]byte](func(w http.ResponseWriter, r *http.Request) error {

		for k, v := range doc.Headers {
			if v == nil {
				continue
			}
			if !whitelistedHeaders[k] {
				k = "x-ragnar-" + k
			}
			if k == "content-length" {
				continue
			}
			w.Header().Set(k, *v)
		}
		// Override content-type to markdown
		w.Header().Set("Content-Type", "text/markdown")
		//w.Header().Set("Content-Length", fmt.Sprintf("%d", contentLength))

		w.WriteHeader(200)
		_, err = io.Copy(w, reader)
		return err
	})
}

func (web *Web) DeleteDocument(ctx context.Context) strut.Response[ragnar.Document] {
	requestId := GetRequestID(ctx)

	tub := strut.PathParam(ctx, "tub")
	documentId := strut.PathParam(ctx, "document_id")

	doc, err := web.db.GetDocument(ctx, tub, documentId)
	if err != nil {
		web.log.Error("Error fetching document", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Document](http.StatusBadRequest,
			fmt.Sprintf("Error fetching document, request_id: %s", requestId))
	}

	err = web.db.DeleteDocument(ctx, tub, documentId)
	if err != nil {
		web.log.Error("Error deleting document obj", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Document](http.StatusBadRequest,
			fmt.Sprintf("Error deleting document obj, request_id: %s", requestId))
	}

	err = web.stor.DeleteDocument(ctx, tub, documentId)
	if err != nil {
		web.log.Error("Error deleting document from storage", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.Document](http.StatusBadRequest,
			fmt.Sprintf("Error deleting document from storage, request_id: %s", requestId))
	}

	return strut.RespondOk(doc)
}

func (web *Web) GetDocumentStatus(ctx context.Context) strut.Response[ragnar.DocumentStatus] {
	requestId := GetRequestID(ctx)

	tub := strut.PathParam(ctx, "tub")
	documentId := strut.PathParam(ctx, "document_id")

	doc, err := web.db.GetDocument(ctx, tub, documentId)
	if err != nil {
		web.log.Error("Error fetching document", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.DocumentStatus](http.StatusBadRequest,
			fmt.Sprintf("Error fetching document, request_id: %s", requestId))
	}
	status, err := web.docket.DocumentStatus(doc.DocumentId)
	if err != nil {
		web.log.Error("Error fetching document status", "err", err, "request_id", requestId)
		return strut.RespondError[ragnar.DocumentStatus](http.StatusInternalServerError,
			fmt.Sprintf("Error fetching document status, request_id: %s", requestId))
	}
	return strut.RespondOk(status)
}
