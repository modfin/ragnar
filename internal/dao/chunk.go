package dao

import (
	"context"
	"errors"
	"fmt"
	"github.com/modfin/bellman/models/embed"
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

		q := `SELECT tub_id, tub_name, document_id, chunk_id, context, content, created_at, updated_at
			  FROM "%s".chunk
			  WHERE tub_name = $1
			    AND document_id = $2
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

		q := `SELECT tub_id, tub_name, document_id, chunk_id, context, content, created_at, updated_at
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

func (d *DAO) QueryChunkEmbeds(ctx context.Context, tubname string, model embed.Model, documentFilter map[string]any, vector []float32, limit, offset int) ([]ragnar.Chunk, error) {
	var chunks []ragnar.Chunk

	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return chunks, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	err := d.txx(ctx, func(tx *sqlx.Tx) error {
		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_READ)
		if err != nil {
			return fmt.Errorf("error checking permission to read tub: %w", err)
		}
		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		colName, err := embedModelToColName(model)
		if err != nil {
			return fmt.Errorf("error getting column name from model, %s: %w", model.FQN(), err)
		}
		q := `
SELECT chunk.tub_id, chunk.tub_name, chunk.document_id, chunk.chunk_id, chunk.context, chunk.content, chunk.created_at, chunk.updated_at 
FROM "%s".chunk
INNER JOIN "%s".document USING (tub_id, document_id)
WHERE chunk.tub_name = $1 
  AND chunk."%s" IS NOT NULL
`
		q = fmt.Sprintf(q, schema, schema, colName)
		args := []any{tubname, vectorToSQLArray(vector)}

		i := len(args) + 1
		for k, v := range documentFilter {
			// check if v is slice or array
			if _, ok := v.([]any); ok {
				q += fmt.Sprintf(" AND document.headers -> $%d = ANY($%d) \n", i, i+1)
			} else {
				q += fmt.Sprintf(" AND document.headers -> $%d = $%d \n", i, i+1)
			}
			args = append(args, strings.ToLower(k), v)
			i += 2
		}

		q += fmt.Sprintf("\nORDER BY chunk.\"%s\" <-> CAST($2 AS VECTOR(%d))", colName, model.OutputDimensions)
		q += fmt.Sprintf("\nLIMIT $%d\nOFFSET $%d", i, i+1)

		args = append(args, limit, offset)
		i += 2

		err = d.db.SelectContext(ctx, &chunks, q, args...)
		if err != nil {
			return fmt.Errorf("error getting chunk: %w", err)
		}

		return nil

	})
	if err != nil {
		return chunks, err
	}
	return chunks, nil
}
