package docket

import (
	"context"
	"fmt"
	"github.com/modfin/pqdocket"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/document"
)

func (d *Docket) ScheduleDocumentConversion(doc ragnar.Document) error {
	return d.scheduleDocumentTask(doc, taskDocumentConversion)
}

func documentConversion(d *Docket) func(pqdocket.RunningTask) error {
	return func(task pqdocket.RunningTask) error {
		d.log.Info("starting conversion of document", "id", task.TaskId())

		var doc ragnar.Document
		err := task.BindMetadata(&doc)
		if err != nil {
			d.log.Error("failed to bind metadata", "error", err, "task", task.TaskId())
			return fmt.Errorf("as documentConversion pqdocket.BindMetadata: %w", err)
		}

		file, err := d.stor.GetDocument(context.Background(), doc.TubName, doc.DocumentId)
		if err != nil {
			d.log.Error("failed to get document", "error", err, "task", task.TaskId())
			return fmt.Errorf("as documentConversion failed to get document: %w", err)
		}
		defer file.Close()

		contentType := doc.Headers["content-type"]
		if contentType == nil {
			d.log.Error("document missing content-type header", "task", task.TaskId())
			return fmt.Errorf("as documentConversion document missing content-type header")
		}
		contentDisposition := doc.Headers["content-disposition"]
		if contentDisposition == nil {
			d.log.Error("document missing content-disposition header", "task", task.TaskId())
			return fmt.Errorf("as documentConversion document missing content-disposition header")
		}

		md, err := document.ConvertToMarkdown(d.log, file, *contentType, *contentDisposition)
		if err != nil {
			d.log.Error("failed to convert to markdown", "error", err, "task", task.TaskId())
			return fmt.Errorf("as documentConversion failed to convert to markdown: %w", err)
		}

		// Fuck, spent all this time on making it a stream...
		err = d.stor.PutDocumentMarkdown(context.Background(), doc.TubName, doc.DocumentId, md, -1, doc.Headers)
		if err != nil {
			d.log.Error("failed to put document", "error", err, "task", task.TaskId())
			return fmt.Errorf("as documentConversion failed to put document: %w", err)
		}

		err = d.ScheduleDocumentChunking(doc)
		if err != nil {
			d.log.Error("failed to schedule chunking", "error", err, "task", task.TaskId())
			return fmt.Errorf("as documentConversion failed to schedule chunking: %w", err)
		}

		return nil
	}
}
