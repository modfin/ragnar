package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/auth"
)

func (d *DAO) GetChunks(ctx context.Context, tubname string, documentId string, limit int, offset int) ([]ragnar.Chunk, error) {
	var chunks []ragnar.Chunk

	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return nil, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	err := d.txx(ctx, func(tx *sqlx.Tx) error {
		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_READ)
		if err != nil {
			return fmt.Errorf("error checking permission to read tub: %w", err)
		}

		q := `SELECT tub_id, tub_name, document_id, chunk_id, content, created_at, updated_at
			  FROM "%s".chunk
			  WHERE tub_name = $1
			    AND document_id = $2
			  ORDER BY chunk_id
			    LIMIT $3
			    OFFSET $4
		`
		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q = fmt.Sprintf(q, schema)
		err = d.db.SelectContext(ctx, &chunks, q, tubname, documentId, limit, offset)
		if err != nil {
			return fmt.Errorf("error getting chunks: %w", err)
		}

		return nil

	})
	if err != nil {
		return nil, err
	}

	return chunks, nil

}

func (d *DAO) GetChunk(ctx context.Context, tubname string, documentId string, index int) (ragnar.Chunk, error) {
	var chunk ragnar.Chunk

	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return chunk, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	err := d.txx(ctx, func(tx *sqlx.Tx) error {

		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_READ)
		if err != nil {
			return fmt.Errorf("error checking permission to read tub: %w", err)
		}

		q := `SELECT tub_id, tub_name, document_id, chunk_id, content, created_at, updated_at
			  FROM "%s".chunk
			  WHERE tub_name = $1
			    AND document_id = $2
			    AND chunk_id = $3
		`
		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q = fmt.Sprintf(q, schema)
		err = d.db.GetContext(ctx, &chunk, q, tubname, documentId, index)
		if err != nil {
			return fmt.Errorf("error getting chunk: %w", err)
		}

		return nil

	})
	if err != nil {
		return chunk, err
	}
	return chunk, nil

}

func (d *DAO) DeleteChunks(ctx context.Context, doc ragnar.Document) error {
	tubname := doc.TubName
	if !bucketNameRegExp.MatchString(tubname) {
		return errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	return d.txx(ctx, func(tx *sqlx.Tx) error {
		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_DELETE)
		if err != nil {
			return fmt.Errorf("error checking permission to read tub: %w", err)
		}

		q := `DELETE FROM "%s".chunk WHERE document_id = $1 AND tub_id = $2`
		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q = fmt.Sprintf(q, schema)
		_, err = d.db.ExecContext(ctx, q, doc.DocumentId, doc.TubId)
		if err != nil {
			return fmt.Errorf("error getting chunk: %w", err)
		}

		return nil
	})
}
