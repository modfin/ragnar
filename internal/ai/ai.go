package ai

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/modfin/bellman"
	"github.com/modfin/bellman/models/embed"
	"github.com/modfin/bellman/models/gen"
	"github.com/modfin/ragnar"
)

type Config struct {
	BellmanName string `cli:"bellman-name"`
	BellmanKey  string `cli:"bellman-key"`
	BellmanURI  string `cli:"bellman-uri"`

	DefaultEmbedModel string `cli:"default-embed-model"`
	DefaultGenModel   string `cli:"default-gen-model"`
}

func New(log *slog.Logger, config Config) *AI {
	bell := bellman.New(config.BellmanURI, bellman.Key{
		Name:  config.BellmanName,
		Token: config.BellmanKey,
	})

	return &AI{log: log, config: config, bell: bell}
}

type AI struct {
	log    *slog.Logger
	config Config
	bell   *bellman.Bellman
}

func (ai *AI) EmbedModelOf(modelFQN string) (embed.Model, error) {
	model, err := embed.ToModel(modelFQN)
	if err != nil {
		return embed.ToModel(ai.config.DefaultEmbedModel)
	}
	return model, nil
}

func (ai *AI) GenModelOf(modelFQN string) (gen.Model, error) {
	model, err := gen.ToModel(modelFQN)
	if err != nil {
		return gen.ToModel(ai.config.DefaultGenModel)
	}
	return model, nil
}

func (ai *AI) Name() string {
	return "ai"
}

func (ai *AI) Close(ctx context.Context) error {
	return nil
}

func (ai *AI) EmbedChunks(model embed.Model, chunks []ragnar.Chunk) ([][]float32, error) {
	chunkTexts := make([]string, len(chunks))
	for i, chunk := range chunks {
		chunkTexts[i] = chunk.Content
	}
	resp, err := ai.bell.Embed(embed.NewManyRequest(context.Background(), model, chunkTexts))
	if err != nil {
		return nil, err
	}
	return resp.AsFloat32(), nil
}

func (ai *AI) EmbedString(model embed.Model, s string) ([]float32, error) {
	resp, err := ai.bell.EmbedDocument(embed.NewDocumentRequest(context.Background(), model, []string{s}))
	if err != nil {
		return nil, err
	}
	data := resp.AsFloat32()
	if len(data) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}
	return data[0], nil
}

const (
	// add some initial chunks to each batch to provide context
	initialDocumentChunksPerBatch = 5
	defaultMaxModelTokenLength    = 32000
	charactersPerTokenEstimate    = 4
)

func (ai *AI) EmbedDocument(model embed.Model, chunks []ragnar.Chunk) ([][]float32, error) {
	if len(chunks) == 0 {
		return [][]float32{}, nil
	}
	documentId := chunks[0].DocumentId
	l := ai.log.With("document_id", documentId, "total_chunks", len(chunks), "model", model.FQN())

	chars := 0
	chunkTexts := make([]string, len(chunks))
	for i, chunk := range chunks {
		chunkTexts[i] = chunk.Content
		chars += len(chunk.Content)
	}
	modelMaxTokens := model.InputMaxTokens
	if modelMaxTokens == 0 {
		modelMaxTokens = defaultMaxModelTokenLength
	}
	charsPerToken := charactersPerTokenEstimate

	var resp *embed.DocumentResponse
	var err error
	for charsPerToken >= 1 {
		var tokens int
		batches, initialBatchChunks := chunksToBatches(l, modelMaxTokens, chunkTexts, charsPerToken)
		result := make([][]float32, 0, len(chunks))
		for idx, batch := range batches {
			resp, err = ai.bell.EmbedDocument(embed.NewDocumentRequest(context.Background(), model, batch))
			if err != nil && strings.Contains(err.Error(), "400") {
				// likely context length exceeded, try smaller batches
				l.Warn("embedding batch failed, likely context length exceeded, will retry with smaller batches",
					"batch_index", idx,
					"batch_size", len(batch),
					"error", err,
				)
				charsPerToken -= 1
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to embed batch %d/%d: %w", idx+1, len(batches), err)
			}

			batchEmbeddings := resp.AsFloat32()
			if len(batchEmbeddings) != len(batch) {
				return nil, fmt.Errorf(
					"embedding API mismatch in batch %d: sent %d chunks but received %d embeddings",
					idx, len(batch), len(batchEmbeddings),
				)
			}
			tokens += resp.Metadata.TotalTokens

			l.Info("embedded document chunk batch",
				"batch_index", idx,
				"tokens", resp.Metadata.TotalTokens,
				"batch_size", len(batch),
			)
			if idx != 0 && initialBatchChunks > 0 {
				// remove initial chunks from subsequent batches
				batchEmbeddings = batchEmbeddings[initialBatchChunks:]
			}
			result = append(result, batchEmbeddings...)
		}
		if err != nil && strings.Contains(err.Error(), "400") {
			continue
		}
		if err != nil {
			return nil, err
		}
		if len(result) != len(chunks) {
			return nil, fmt.Errorf("final embedding count mismatch: expected %d but got %d", len(chunks), len(result))
		}
		l.Info("successfully embedded document",
			"total_chunks", len(chunks),
			"total_tokens", tokens,
			"total_characters", chars,
			"used_chars_per_token_estimate", charsPerToken,
		)
		return result, nil
	}

	return nil, fmt.Errorf("could not embed document, all chunk batching attempts failed, %w", err)
}

func chunksToBatches(log *slog.Logger, modelMaxTokens int, chunkTexts []string, charsPerToken int) ([][]string, int) {
	var batches [][]string
	var currentBatch []string
	currentBatchTokens := 0

	var initialBatchChunks []string
	var initialBatchTokens int
	for i := 0; i < len(chunkTexts) && i < initialDocumentChunksPerBatch; i++ {
		initialBatchChunks = append(initialBatchChunks, chunkTexts[i])
		initialBatchTokens += len(chunkTexts[i]) / charsPerToken
	}
	if initialBatchTokens > modelMaxTokens/2 {
		log.Warn("initial document chunks exceed half of estimated model token limit",
			"estimated_tokens", initialBatchTokens,
			"model_max_tokens", modelMaxTokens,
		)
		initialBatchChunks = nil
		initialBatchTokens = 0
	}

	for _, chunkText := range chunkTexts {
		chunkEstimatedTokens := len(chunkText) / charsPerToken

		if chunkEstimatedTokens > modelMaxTokens {
			log.Warn("single document chunk exceeds estimated model token limit",
				"estimated_tokens", chunkEstimatedTokens,
				"model_max_tokens", modelMaxTokens,
			)
			if len(currentBatch) > 0 {
				batches = append(batches, currentBatch)
			}
			batches = append(batches, []string{chunkText})
			currentBatch = initialBatchChunks
			currentBatchTokens = initialBatchTokens
			continue
		}

		if len(currentBatch) > 0 && (currentBatchTokens+chunkEstimatedTokens) > modelMaxTokens {
			batches = append(batches, currentBatch)
			currentBatch = append(initialBatchChunks, chunkText)
			currentBatchTokens = initialBatchTokens + chunkEstimatedTokens
		} else {
			currentBatch = append(currentBatch, chunkText)
			currentBatchTokens += chunkEstimatedTokens
		}
	}

	if len(currentBatch) > 0 {
		batches = append(batches, currentBatch)
	}
	return batches, len(initialBatchChunks)
}
