package docket

import (
	"fmt"
	"github.com/modfin/bellman/models/embed"
	"github.com/modfin/bellman/services/voyageai"
	"github.com/modfin/pqdocket"
	"github.com/modfin/ragnar"
	"time"
)

func (d *Docket) ScheduleChunkEmbedding(doc ragnar.Document) error {
	return d.scheduleDocumentTask(doc, taskChunkEmbed)
}

func chunkEmbed(d *Docket) func(pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		start := time.Now()
		l := d.log.With("task", task.TaskId(), "func", task.Func())
		l.Info("starting embedding of document")

		var doc ragnar.Document
		err := task.BindMetadata(&doc)
		if err != nil {
			l.Error("failed to bind metadata", "error", err)
			return fmt.Errorf("in chunkEmbed pqdocket.BindMetadata: %w", err)
		}
		l = l.With("document_id", doc.DocumentId)

		tub, err := d.db.InternalGetTub(doc.TubId)
		if err != nil {
			l.Error("failed to get tub", "error", err)
			return fmt.Errorf("in chunkEmbed pqdocket.InternalGetTub: %w", err)
		}

		chunks, err := d.db.InternalGetChunks(doc)
		if err != nil {
			l.Error("failed to get chunks", "error", err)
			return fmt.Errorf("in chunkEmbed pqdocket.InternalGetChunks: %w", err)
		}
		if len(chunks) == 0 {
			l.Warn("no chunks to embed, skipping")
			return nil
		}

		model := voyageai.EmbedModel_voyage_context_3 // default model
		modelFQN, ok := tub.Settings["embed_model"]
		if ok && modelFQN != nil {
			model, err = d.ai.EmbedModelOf(*modelFQN)
			if err != nil {
				l.Error("failed to get model", "error", err)
				return fmt.Errorf("in chunkEmbed ai.EmbedModelOf: %w", err)
			}
		}

		err = d.db.InternalEnsureTubEmbeddingSchema(doc, model)
		if err != nil {
			l.Error("failed to ensure embedding schema", "error", err)
			return fmt.Errorf("in chunkEmbed ai.InternalEnsureTubEmbeddingSchema: %w", err)
		}

		vectors, err := d.ai.EmbedDocument(model.WithType(embed.TypeDocument), chunks)
		if err != nil {
			l.Error("failed to embed chunks", "error", err)
			return fmt.Errorf("in chunkEmbed ai.EmbedDocument: %w", err)
		}

		err = d.db.InternalSetEmbeds(doc, model, chunks, vectors)
		if err != nil {
			l.Error("failed to set embeds", "error", err)
			return fmt.Errorf("in chunkEmbed ai.InternalSetEmbeds: %w", err)
		}
		l.With("duration_ms", time.Since(start).Milliseconds()).Info("task completed")

		return nil
	}
}
