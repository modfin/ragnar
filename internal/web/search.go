package web

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/modfin/bellman/models/embed"
	"github.com/modfin/bellman/services/voyageai"
	"net/http"
	"strconv"

	"github.com/modfin/ragnar"
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

	chunks, err := web.db.QueryChunkEmbeds(ctx, tub.TubName, embedModel, filter, queryVector, limit, offset)
	if err != nil {
		web.log.Error("failed to query chunk embeds", "error", err)
		return strut.RespondError[string](http.StatusInternalServerError, fmt.Sprintf("Failed to query chunk embeds"))
	}

	return strut.RespondOk(chunks)
}
