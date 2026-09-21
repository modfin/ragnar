package ai

import (
	"strings"
	"testing"

	"github.com/modfin/bellman/services/voyageai"
)

func TestEmbedModelOfResolvesRegisteredModels(t *testing.T) {
	ai := &AI{config: Config{DefaultEmbedModel: voyageai.EmbedModel_voyage_3_large.FQN()}}

	model, err := ai.EmbedModelOf("VoyageAI/voyage-3-large")
	if err != nil {
		t.Fatalf("known model: %v", err)
	}
	if model.OutputDimensions != 1024 || model.InputMaxTokens == 0 {
		t.Fatalf("dimensions and token limit must come from the registry, got %+v", model)
	}

	model, err = ai.EmbedModelOf("  ")
	if err != nil || model.Name != "voyage-3-large" {
		t.Fatalf("empty setting must resolve the default model, got %+v, %v", model, err)
	}

	for name, fqn := range map[string]string{
		"no provider":      "voyage-3-large",
		"unknown model":    "VoyageAI/voyage-context-99",
		"unknown provider": "OpenAI/text-embedding-3-large",
	} {
		if _, err := ai.EmbedModelOf(fqn); err == nil {
			t.Errorf("%s: %q must be rejected", name, fqn)
		}
	}
	_, err = ai.EmbedModelOf("VoyageAI/voyage-context-99")
	if err == nil || !strings.Contains(err.Error(), "voyage-3-large") {
		t.Fatalf("the error must list the known models, got %v", err)
	}
}
