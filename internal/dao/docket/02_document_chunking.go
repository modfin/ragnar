package docket

import (
	"context"
	"fmt"
	"github.com/modfin/pqdocket"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/chunker"
	"io"
)

func (d *Docket) ScheduleDocumentChunking(doc ragnar.Document) error {
	return d.scheduleDocumentTask(doc, taskChunkDocument)
}

func chunkDocument(d *Docket) func(pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		d.log.Info("starting chunking document", "id", task.TaskId())

		var doc ragnar.Document
		err := task.BindMetadata(&doc)
		if err != nil {
			d.log.Error("failed to bind metadata", "error", err, "task", task.TaskId())
			return fmt.Errorf("chunkDocument, could not bind metadata: %w", err)
		}

		tub, err := d.db.InternalGetTub(doc.TubId)

		// DELETING ALL OLD CHUNKS
		err = d.db.InternalDeleteChunks(doc)
		if err != nil {
			d.log.Error("failed to delete chunks", "error", err, "task", task.TaskId())
			return fmt.Errorf("chunkDocument, could not delete chunks: %w", err)
		}

		// GETTING MARKDOWN OF DOCUMENT
		reader, err := d.stor.GetDocumentMarkdown(context.Background(), doc.TubName, doc.DocumentId)
		if err != nil {
			d.log.Error("failed to get document markdown", "error", err, "task", task.TaskId())
			return fmt.Errorf("chunkDocument, could not get md version of document: %w", err)
		}
		defer reader.Close()

		md, err := io.ReadAll(reader)
		if err != nil {
			d.log.Error("failed to read document markdown", "error", err, "task", task.TaskId())
			return fmt.Errorf("chunkDocument, could not read md version of document: %w", err)
		}

		splitter := chunker.GetTextSplitterFromTubSettings(tub.Settings)

		chunks, err := splitter.SplitText(string(md))
		if err != nil {
			d.log.Error("failed to split document", "error", err, "task", task.TaskId())
			return fmt.Errorf("chunkDocument, could not split document: %w", err)
		}

		// chunk context seems a bit weird to have hardcoded on the document headers
		chunkContext := doc.Headers["chunk_context"]
		if chunkContext == nil {
			chunkContext = new(string)
		}

		for i, chunk := range chunks {
			// TODO map and batch the inserts?
			err = d.db.InternalInsertChunk(ragnar.Chunk{
				ChunkId:    i,
				DocumentId: doc.DocumentId,
				TubId:      doc.TubId,
				TubName:    doc.TubName,
				Context:    *chunkContext,
				Content:    chunk,
			})
			if err != nil {
				d.log.Error("failed to insert chunk", "error", err, "task", task.TaskId())
				return fmt.Errorf("chunkDocument, could not insert chunk: %w", err)
			}
		}

		err = d.ScheduleChunkEmbedding(doc)
		if err != nil {
			d.log.Error("failed to schedule chunk embedding", "error", err, "task", task.TaskId())
			return fmt.Errorf("chunkDocument, could not schedule chunk embedding: %w", err)
		}

		return nil
	}
}
