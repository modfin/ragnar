package web

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/modfin/ragnar"
	"github.com/modfin/strut"
)

func (web *Web) GetChunks(ctx context.Context) strut.Response[[]ragnar.Document] {
	correlationId := GetRequestID(ctx)
	tub := strut.PathParam(ctx, "tub")
	documentId := strut.PathParam(ctx, "document_id")

	limit, err := strconv.Atoi(strut.QueryParam(ctx, "limit"))
	if err != nil {
		limit = 10_000
	}
	offset, err := strconv.Atoi(strut.QueryParam(ctx, "offset"))
	if err != nil {
		offset = 0
	}

	chunks, err := web.db.GetChunks(ctx, tub, documentId, limit, offset)
	if err != nil {
		web.log.Error("Error listing chunks", "err", err, "correlation_id", correlationId)
		return strut.RespondError[[]ragnar.Document](http.StatusInternalServerError,
			fmt.Sprintf("Error listing chunks, correlation_id: %s", correlationId))
	}

	return strut.RespondOk(chunks)
}

func (web *Web) GetChunk(ctx context.Context) strut.Response[ragnar.Chunk] {

	correlationId := GetRequestID(ctx)

	tub := strut.PathParam(ctx, "tub")
	documentId := strut.PathParam(ctx, "document_id")

	index, err := strconv.Atoi(strut.QueryParam(ctx, "index"))
	if err != nil {
		web.log.Error("Error getting chunk, index was not provided", "err", err, "correlation_id", correlationId)
		return strut.RespondError[ragnar.Chunk](http.StatusBadRequest,
			fmt.Sprintf("Error getting chunk, index was not provided, correlation_id: %s", correlationId))
	}

	chunks, err := web.db.GetChunk(ctx, tub, documentId, index)
	if err != nil {
		web.log.Error("Error listing chunks", "err", err, "correlation_id", correlationId)
		return strut.RespondError[ragnar.Chunk](http.StatusInternalServerError,
			fmt.Sprintf("Error getting chunk, correlation_id: %s", correlationId))
	}

	return strut.RespondOk(chunks)

}
