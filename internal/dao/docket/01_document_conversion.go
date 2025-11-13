package docket

import (
	"bytes"
	"context"
	"fmt"
	"github.com/modfin/pqdocket"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/document"
	"github.com/modfin/ragnar/internal/util"
	"io"
	"time"
)

func (d *Docket) ScheduleDocumentConversion(doc ragnar.Document) error {
	return d.scheduleDocumentTask(doc, taskDocumentConversion)
}

func documentConversion(d *Docket) func(pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		start := time.Now()
		l := d.log.With("task", task.TaskId(), "func", task.Func())
		l.Info("starting conversion of document")

		var doc ragnar.Document
		err := task.BindMetadata(&doc)
		if err != nil {
			l.Error("failed to bind metadata", "error", err)
			return fmt.Errorf("as documentConversion pqdocket.BindMetadata: %w", err)
		}
		l = l.With("document_id", doc.DocumentId)

		file, err := d.stor.GetDocument(context.Background(), doc.TubName, doc.DocumentId)
		if err != nil {
			l.Error("failed to get document", "error", err)
			return fmt.Errorf("as documentConversion failed to get document: %w", err)
		}
		defer file.Close()

		contentType := doc.Headers["content-type"]
		if contentType == nil {
			l.Error("document missing content-type header")
			return fmt.Errorf("as documentConversion document missing content-type header")
		}
		contentDisposition := doc.Headers["content-disposition"]
		if contentDisposition == nil {
			l.Error("document missing content-disposition header")
			return fmt.Errorf("as documentConversion document missing content-disposition header")
		}

		md, err := document.ConvertToMarkdown(l, file, *contentType, *contentDisposition)
		if err != nil {
			l.Error("failed to convert to markdown", "error", err)
			return fmt.Errorf("as documentConversion failed to convert to markdown: %w", err)
		}

		data, err := io.ReadAll(md)
		if err != nil {
			l.Error("failed to read markdown", "error", err)
			return fmt.Errorf("as documentConversion failed to read markdown: %w", err)
		}
		seekableReader := bytes.NewReader(data)

		markdownHash, err := util.HashReaderSHA256(seekableReader)
		if err != nil {
			l.Error("failed to hash markdown", "error", err)
			return fmt.Errorf("as documentConversion failed to hash markdown: %w", err)
		}
		// Reset reader after hashing
		_, err = seekableReader.Seek(0, 0)
		if err != nil {
			l.Error("failed to seek markdown reader", "error", err)
			return fmt.Errorf("as documentConversion failed to seek markdown reader: %w", err)
		}

		// Fuck, spent all this time on making it a stream...
		_, err = d.stor.PutDocumentMarkdown(context.Background(), doc.TubName, doc.DocumentId, seekableReader, -1, doc.Headers, markdownHash)
		if err != nil {
			l.Error("failed to put document", "error", err)
			return fmt.Errorf("as documentConversion failed to put document: %w", err)
		}

		err = d.ScheduleDocumentChunking(doc)
		if err != nil {
			l.Error("failed to schedule chunking", "error", err)
			return fmt.Errorf("as documentConversion failed to schedule chunking: %w", err)
		}
		l.With("duration_ms", time.Since(start).Milliseconds()).Info("task completed")

		return nil
	}
}
