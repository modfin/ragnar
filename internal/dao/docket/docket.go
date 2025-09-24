package docket

import (
	"context"
	"fmt"
	"github.com/modfin/ragnar"
	"log/slog"
	"strings"
	"time"

	"github.com/modfin/pqdocket"
	"github.com/modfin/ragnar/internal/ai"
	"github.com/modfin/ragnar/internal/dao"
	"github.com/modfin/ragnar/internal/storage"
)

type Config struct {
	URI string `cli:"db-uri"`
}
type Docket struct {
	docket pqdocket.Docket
	log    *slog.Logger
	config Config
	db     *dao.DAO
	stor   *storage.Storage
	ai     *ai.AI
}

const taskDocumentConversion = "document-conversion"
const taskChunkDocument = "chunk-document"
const taskChunkEmbed = "chunks-embed"

func New(log *slog.Logger, db *dao.DAO, stor *storage.Storage, ai *ai.AI, config Config) (*Docket, error) {

	log.Info("Initializing pqdocket", "funcs", []string{taskDocumentConversion, taskChunkDocument, taskChunkEmbed})

	pq, err := pqdocket.Init(config.URI,
		pqdocket.WithLogger(log.With("who", "pqdocket")),
		pqdocket.Namespace("embedding_queue"),
		pqdocket.MaxClaimCount(2),
		pqdocket.DefaultClaimTime(5*60),
	)
	if err != nil {
		return nil, fmt.Errorf("pqdocket.Init: %w", err)
	}

	docket := &Docket{
		config: config,
		docket: pq,
		db:     db,
		stor:   stor,
		log:    log,
		ai:     ai,
	}

	pq.RegisterFunctionWithFuncName(taskDocumentConversion, documentConversion(docket))
	pq.RegisterFunctionWithFuncName(taskChunkDocument, chunkDocument(docket))
	pq.RegisterFunctionWithFuncName(taskChunkEmbed, chunkEmbed(docket))

	return docket, nil
}

func (d *Docket) DocumentStatus(documentId string) (ragnar.DocumentStatus, error) {
	tasks, err := d.docket.FindTasks(nil, pqdocket.WithRefId(strings.TrimPrefix(documentId, "doc_")))
	if err != nil {
		return ragnar.DocumentStatus{}, fmt.Errorf("at DocumentStatus pqdocket.FindTasks: %w", err)
	}
	for _, task := range tasks {
		if task.Func() == taskChunkEmbed && task.CompletedAt().Valid && task.CompletedAt().Time.Before(time.Now()) {
			return ragnar.DocumentStatus{Status: "completed"}, nil
		}
	}
	if len(tasks) > 0 {
		return ragnar.DocumentStatus{Status: "processing"}, nil
	}
	return ragnar.DocumentStatus{Status: "pending"}, nil
}

func (d *Docket) Name() string {
	return "docket"
}

func (d *Docket) deleteDocumentTaskIfExists(refId, funcName string) error {
	tasks, err := d.docket.FindTasks(nil, pqdocket.WithRefId(refId))
	if err != nil {
		return fmt.Errorf("%s pqdocket.FindTasks: %w", funcName, err)
	}
	for _, t := range tasks {
		if t.Func() == funcName {
			err = t.Delete(nil)
			if err != nil {
				return fmt.Errorf("%s pqdocket.DeleteTask: %w", funcName, err)
			}
			d.log.Info(fmt.Sprintf("Removed old pending document %s task", funcName), "doc", refId, "task", t.TaskId())
			return nil
		}
	}
	return nil
}

func (d *Docket) scheduleDocumentTask(doc ragnar.Document, funcName string) error {
	md, err := pqdocket.CreateMetadata(doc)
	if err != nil {
		return fmt.Errorf("at %s pqdocket.CreateMetadata: %w", funcName, err)
	}
	refId := strings.TrimPrefix(doc.DocumentId, "doc_")
	creator := d.docket.
		CreateTaskWithFuncName(funcName).
		WithRefId(refId).
		WithMetadata(md).
		ScheduleAt(time.Now())

	task, err := d.docket.InsertTask(nil, creator)
	if err != nil && strings.Contains(err.Error(), "pq: duplicate key value violates unique constraint") {
		// there is already a pending task for this document, remove it and try again
		err = d.deleteDocumentTaskIfExists(refId, funcName)
		if err != nil {
			return fmt.Errorf("at %s deleteDocumentTaskIfExists: %w", funcName, err)
		}
		task, err = d.docket.InsertTask(nil, creator)
	}
	if err != nil {
		d.log.Error(fmt.Sprintf("failed to insert task %s", funcName), "error", err)
		return fmt.Errorf("at %s pqdocket.InsertTask: %w", funcName, err)
	}
	d.log.Debug(fmt.Sprintf("Scheduled %s for document", funcName), "id", doc.DocumentId, "task", task.TaskId())
	return nil
}

func (d *Docket) Close(ctx context.Context) error {
	var closed = make(chan struct{})
	go func() {
		d.docket.Close()
		close(closed)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-closed:
		return nil
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timed out waiting for database connection to close")
	}
}
