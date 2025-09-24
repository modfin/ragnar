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

func (app *Web) SearchXNN(ctx context.Context) strut.Response[[]ragnar.Chunk] {

	tubName := strut.PathParam(ctx, "tub")
	tub, err := app.db.GetTub(ctx, tubName)
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

	documentFilter := map[string]any{}
	err = json.Unmarshal([]byte(filterStr), &documentFilter)
	if err != nil {
		app.log.Error("Error unmarshalling json string", "err", err, "filter", filterStr)
		return strut.RespondError[[]ragnar.Chunk](http.StatusBadRequest, fmt.Sprintf("Invalid JSON format in 'documentFilter' query parameter"))
	}

	app.log.Debug("SearchXNN", "tub", tub, "query", query, "limit", limit, "offset", offset)

	embedModel := voyageai.EmbedModel_voyage_context_3 // default model
	modelFQN, ok := tub.Settings["embed_model"]
	if ok && modelFQN != nil {
		embedModel, err = app.ai.EmbedModelOf(*modelFQN)
		if err != nil {
			app.log.Error("failed to get model", "error", err)
			return strut.RespondError[string](http.StatusBadRequest, fmt.Sprintf("Could not find embedding model: %v", *modelFQN))
		}
	}
	queryVector, err := app.ai.EmbedString(embedModel.WithType(embed.TypeQuery), query)
	if err != nil {
		app.log.Error("failed to embed query", "error", err)
		return strut.RespondError[string](http.StatusInternalServerError, fmt.Sprintf("Failed to embed query"))
	}

	chunks, err := app.db.QueryChunkEmbeds(ctx, tub.TubName, embedModel, documentFilter, queryVector, limit, offset)
	if err != nil {
		app.log.Error("failed to query chunk embeds", "error", err)
		return strut.RespondError[string](http.StatusInternalServerError, fmt.Sprintf("Failed to query chunk embeds"))
	}

	return strut.RespondOk(chunks)
}
