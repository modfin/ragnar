package ai

import (
	"context"
	"fmt"
	"log/slog"

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

func (ai *AI) EmbedDocument(model embed.Model, chunks []ragnar.Chunk) ([][]float32, error) {
	chunkTexts := make([]string, len(chunks))
	for i, chunk := range chunks {
		chunkTexts[i] = chunk.Content
	}
	resp, err := ai.bell.EmbedDocument(embed.NewDocumentRequest(context.Background(), model, chunkTexts))
	if err != nil {
		return nil, err
	}
	return resp.AsFloat32(), nil
}
