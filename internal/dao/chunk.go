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

func (d *DAO) QueryChunkEmbeds(ctx context.Context, tubname string, model embed.Model, documentFilter ragnar.DocumentFilter, vector []float32, limit, offset int) ([]ragnar.Chunk, error) {
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
SELECT chunk.tub_id, chunk.tub_name, chunk.document_id, chunk.chunk_id, chunk.content, chunk.created_at, chunk.updated_at 
FROM "%s".chunk
INNER JOIN "%s".document USING (tub_id, document_id)
WHERE chunk."%s" IS NOT NULL
`
		q = fmt.Sprintf(q, schema, schema, colName)
		args := []any{vectorToSQLArray(vector)}

		i := len(args) + 1
		for fieldName, filterValues := range documentFilter {
			fieldName = strings.ToLower(fieldName)

			// Process each filter value for this field (multiple conditions are AND-ed together)
			for _, filterValue := range filterValues {
				if filterValue.Simple != nil {
					// Simple equality check
					q += fmt.Sprintf(" AND document.headers -> $%d = $%d \n", i, i+1)
					args = append(args, fieldName, *filterValue.Simple)
					i += 2
				} else if filterValue.Array != nil {
					// Array contains check (ANY operator)
					q += fmt.Sprintf(" AND document.headers -> $%d = ANY($%d) \n", i, i+1)
					args = append(args, fieldName, filterValue.Array)
					i += 2
				} else if filterValue.Condition != nil {
					// Operator-based condition
					// Determine the cast expression based on value type
					leftSide := fmt.Sprintf("document.headers -> $%d", i)
					rightSide := fmt.Sprintf("$%d", i+1)

					// Apply type casting for numeric comparisons
					switch filterValue.Condition.ValueType {
					case ragnar.ValueTypeInteger:
						leftSide = fmt.Sprintf("CAST(document.headers -> $%d AS INTEGER)", i)
						rightSide = fmt.Sprintf("CAST($%d AS INTEGER)", i+1)
					case ragnar.ValueTypeNumeric:
						leftSide = fmt.Sprintf("CAST(document.headers -> $%d AS NUMERIC)", i)
						rightSide = fmt.Sprintf("CAST($%d AS NUMERIC)", i+1)
						// ValueTypeText or default - no casting needed
					}

					switch filterValue.Condition.Operator {
					case ragnar.OpEqual:
						q += fmt.Sprintf(" AND %s = %s \n", leftSide, rightSide)
					case ragnar.OpGreaterThan:
						q += fmt.Sprintf(" AND %s > %s \n", leftSide, rightSide)
					case ragnar.OpGreaterThanOrEqual:
						q += fmt.Sprintf(" AND %s >= %s \n", leftSide, rightSide)
					case ragnar.OpLessThan:
						q += fmt.Sprintf(" AND %s < %s \n", leftSide, rightSide)
					case ragnar.OpLessThanOrEqual:
						q += fmt.Sprintf(" AND %s <= %s \n", leftSide, rightSide)
					case ragnar.OpIn:
						// For $in operator with a single value in condition
						q += fmt.Sprintf(" AND %s = %s \n", leftSide, rightSide)
					default:
						return fmt.Errorf("unsupported operator: %s", filterValue.Condition.Operator)
					}
					args = append(args, fieldName, filterValue.Condition.Value)
					i += 2
				}
			}
		}

		q += fmt.Sprintf("\nORDER BY chunk.\"%s\" <=> CAST($1 AS VECTOR(%d))", colName, model.OutputDimensions)
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
