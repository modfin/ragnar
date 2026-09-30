package web

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/modfin/bellman/models/embed"
	"github.com/modfin/bellman/services/voyageai"
	"math"
	"net/http"
	"strconv"

	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/dao"
	"github.com/modfin/strut"
)

func (web *Web) SearchXNN(ctx context.Context) strut.Response[[]ragnar.Chunk] {
	requestId := GetRequestID(ctx)

	tubName := strut.PathParam(ctx, "tub")
	tub, err := web.db.GetTub(ctx, tubName)
	if err != nil {
		return strut.RespondError[string](http.StatusBadRequest, "Tub not found")
	}

	query := strut.QueryParam(ctx, "q")
	if query == "" {
		return strut.RespondError[string](http.StatusBadRequest, "No query provided")
	}
	filterStr := strut.QueryParam(ctx, "filter")
	if filterStr == "" {
		filterStr = "{}"
	}

	limit, err := strconv.Atoi(strut.QueryParam(ctx, "limit"))
	if err != nil {
		limit = 10
	}
	offset, err := strconv.Atoi(strut.QueryParam(ctx, "offset"))
	if err != nil {
		offset = 0
	}

	var filter ragnar.DocumentFilter
	err = json.Unmarshal([]byte(filterStr), &filter)
	if err != nil {
		web.log.Error("Error unmarshalling filter json", "err", err, "request_id", requestId)
		return strut.RespondError[[]ragnar.Document](http.StatusBadRequest,
			fmt.Sprintf("Invalid JSON format in 'filter' query parameter, request_id: %s", requestId))
	}

	maxPerDocument, err := optionalIntParam(ctx, "max_per_document", 0, math.MaxInt)
	if err != nil {
		return strut.RespondError[string](http.StatusBadRequest, err.Error())
	}
	neighbours, err := optionalIntParam(ctx, "neighbours", 0, ragnar.MaxSearchNeighbours)
	if err != nil {
		return strut.RespondError[string](http.StatusBadRequest, err.Error())
	}

	web.log.Debug("SearchXNN", "tub", tub, "query", query, "limit", limit, "offset", offset)

	embedModel := voyageai.EmbedModel_voyage_context_3 // default model
	modelFQN, ok := tub.Settings["embed_model"]
	if ok && modelFQN != nil {
		embedModel, err = web.ai.EmbedModelOf(*modelFQN)
		if err != nil {
			web.log.Error("failed to get model", "error", err)
			return strut.RespondError[string](http.StatusBadRequest, fmt.Sprintf("Could not find embedding model: %v", *modelFQN))
		}
	}
	queryVector, err := web.ai.EmbedString(embedModel.WithType(embed.TypeQuery), query)
	if err != nil {
		web.log.Error("failed to embed query", "error", err)
		return strut.RespondError[string](http.StatusInternalServerError, fmt.Sprintf("Failed to embed query"))
	}

	chunks, err := web.db.SearchChunks(ctx, tub.TubName, embedModel, dao.SearchParams{
		Vector:         queryVector,
		Filter:         filter,
		Limit:          limit,
		Offset:         offset,
		MaxPerDocument: maxPerDocument,
		Neighbours:     neighbours,
	})
	if err != nil {
		web.log.Error("failed to query chunk embeds", "error", err)
		return strut.RespondError[string](http.StatusInternalServerError, fmt.Sprintf("Failed to query chunk embeds"))
	}

	return strut.RespondOk(chunks)
}

// optionalIntParam reads an integer query parameter that defaults to lo when
// absent, and must lie within [lo, hi] when given.
func optionalIntParam(ctx context.Context, name string, lo, hi int) (int, error) {
	raw := strut.QueryParam(ctx, name)
	if raw == "" {
		return lo, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v < lo || v > hi {
		return 0, fmt.Errorf("invalid %s %q, must be an integer from %d to %d", name, raw, lo, hi)
	}
	return v, nil
}
