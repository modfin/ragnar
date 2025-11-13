package docket

import (
	"context"
	"fmt"
	"github.com/modfin/pqdocket"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/chunker"
	"io"
	"time"
)

func (d *Docket) ScheduleDocumentChunking(doc ragnar.Document) error {
	return d.scheduleDocumentTask(doc, taskChunkDocument)
}

func chunkDocument(d *Docket) func(pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		start := time.Now()
		l := d.log.With("task", task.TaskId(), "func", task.Func())
		l.Info("starting chunking document")

		var doc ragnar.Document
		err := task.BindMetadata(&doc)
		if err != nil {
			l.Error("failed to bind metadata", "error", err)
			return fmt.Errorf("chunkDocument, could not bind metadata: %w", err)
		}
		l = l.With("document_id", doc.DocumentId)

		tub, err := d.db.InternalGetTub(doc.TubId)

		// GETTING MARKDOWN OF DOCUMENT
		reader, err := d.stor.GetDocumentMarkdown(context.Background(), doc.TubName, doc.DocumentId)
		if err != nil {
			l.Error("failed to get document markdown", "error", err)
			return fmt.Errorf("chunkDocument, could not get md version of document: %w", err)
		}
		defer reader.Close()

		md, err := io.ReadAll(reader)
		if err != nil {
			l.Error("failed to read document markdown", "error", err)
			return fmt.Errorf("chunkDocument, could not read md version of document: %w", err)
		}

		splitter := chunker.GetTextSplitterFromTubSettings(tub.Settings)

		chunks, err := splitter.SplitText(string(md))
		if err != nil {
			l.Error("failed to split document", "error", err)
			return fmt.Errorf("chunkDocument, could not split document: %w", err)
		}

		currentChunks, err := d.db.InternalGetChunks(doc)
		if err != nil {
			l.Error("failed to get current chunks", "error", err)
			return fmt.Errorf("chunkDocument, could not get current chunks: %w", err)
		}

		if len(currentChunks) == len(chunks) {
			identical := true
			for i, chunk := range chunks {
				if currentChunks[i].Content != chunk {
					identical = false
					break
				}
			}
			if identical {
				l.Info("chunks are identical to existing ones, skipping update")
				return nil
			}
		}

		// DELETING ALL OLD CHUNKS
		err = d.db.InternalDeleteChunks(doc)
		if err != nil {
			l.Error("failed to delete chunks", "error", err)
			return fmt.Errorf("chunkDocument, could not delete chunks: %w", err)
		}

		if len(chunks) == 0 {
			l.Warn("no chunks created from document")
			return nil
		}

		var ragnarChunks []ragnar.Chunk
		for i, chunk := range chunks {
			ragnarChunks = append(ragnarChunks, ragnar.Chunk{
				ChunkId:    i,
				DocumentId: doc.DocumentId,
				TubId:      doc.TubId,
				TubName:    doc.TubName,
				Content:    chunk,
			})
		}
		err = d.db.InternalInsertChunks(ragnarChunks)
		if err != nil {
			l.Error("failed to insert chunks", "error", err)
			return fmt.Errorf("chunkDocument, could not insert chunks: %w", err)
		}

		err = d.ScheduleChunkEmbedding(doc)
		if err != nil {
			l.Error("failed to schedule chunk embedding", "error", err)
			return fmt.Errorf("chunkDocument, could not schedule chunk embedding: %w", err)
		}
		l.With("duration_ms", time.Since(start).Milliseconds()).Info("task completed")

		return nil
	}
}
