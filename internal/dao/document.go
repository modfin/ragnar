package dao

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/auth"
)

func (d *DAO) UpsertDocument(ctx context.Context, doc ragnar.Document) (ragnar.Document, error) {
	var retdoc ragnar.Document

	tubname := strings.ToLower(doc.TubName)
	if !bucketNameRegExp.MatchString(tubname) {
		return ragnar.Document{}, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	tub, err := d.GetTub(ctx, tubname)
	if err != nil {
		return ragnar.Document{}, fmt.Errorf("error getting new document's tub info: %w", err)
	}
	requiredDocumentHeaders := tub.GetRequiredDocumentHeaders()
	for _, h := range requiredDocumentHeaders {
		if _, ok := doc.Headers[h]; !ok {
			return ragnar.Document{}, fmt.Errorf("missing required document header: %s", h)
		}
	}

	err = d.txx(ctx, func(tx *sqlx.Tx) error {
		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_CREATE, auth.ALLOW_UPDATE)
		if err != nil {
			return fmt.Errorf("error checking permission to update / create document: %w", err)
		}

		q := `INSERT INTO "%s"."document" (tub_id, tub_name, headers) 
			 VALUES ($1, $2, $3) 
			 RETURNING *`
		args := []interface{}{doc.TubId, tubname, doc.Headers}
		if len(doc.DocumentId) == 36+4 {
			_, err = uuid.Parse(doc.DocumentId[4:])
			if err != nil {
				return fmt.Errorf("error parsing document id: %w", err)
			}
			q = `UPDATE "%s"."document" 
				 SET headers = $3, 
				     updated_at = now() 
				WHERE tub_id = $1
				  AND tub_name = $2
				  AND document_id = $4 
				RETURNING *`
			args = append(args, doc.DocumentId)
		}

		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}

		q = fmt.Sprintf(q, schema)

		err = tx.GetContext(ctx, &retdoc, q, args...)
		if err != nil {
			return fmt.Errorf("error inserting / updating document: %w", err)
		}

		return nil
	})

	if err != nil {
		return ragnar.Document{}, err
	}

	return retdoc, nil
}

func (d *DAO) ListDocuments(ctx context.Context, tubname string, filter ragnar.DocumentFilter, limit int, offset int) ([]ragnar.Document, error) {

	if limit == 0 {
		limit = 100
	}

	var docs []ragnar.Document

	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return nil, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	err := d.txx(ctx, func(tx *sqlx.Tx) error {

		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_READ)
		if err != nil {
			return fmt.Errorf("error checking permission to read tub: %w", err)
		}

		q := `SELECT * FROM "%s"."document"
              WHERE tub_name = $1
		  `

		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q = fmt.Sprintf(q, schema)

		var args []interface{}
		args = append(args, tubname)

		i := 2
		for fieldName, filterValues := range filter {
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

		q += fmt.Sprintf(" LIMIT $%d OFFSET $%d \n", i, i+1)
		args = append(args, limit, offset)
		i += 2

		return tx.SelectContext(ctx, &docs, q, args...)
	})

	if err != nil {
		return nil, err
	}

	return docs, nil
}

func (d *DAO) AllDocumentsHasHeaders(ctx context.Context, tubname string, headers []string) (bool, error) {
	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return false, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	var badDocsExists bool
	err := d.txx(ctx, func(tx *sqlx.Tx) error {

		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_READ)
		if err != nil {
			return fmt.Errorf("error checking permission to read tub: %w", err)
		}

		q := `
SELECT COUNT(1) > 0 
FROM "%s"."document"
WHERE tub_name = $1 
  AND NOT headers ?& $2`

		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q = fmt.Sprintf(q, schema)

		args := []any{tubname, headers}

		return tx.GetContext(ctx, &badDocsExists, q, args...)
	})

	if err != nil {
		return false, err
	}
	return !badDocsExists, nil
}

func (d *DAO) GetDocument(ctx context.Context, tubname string, documentId string) (ragnar.Document, error) {

	var doc ragnar.Document

	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return ragnar.Document{}, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	err := d.txx(ctx, func(tx *sqlx.Tx) error {

		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_READ)
		if err != nil {
			return fmt.Errorf("error checking permission to read tub: %w", err)
		}

		q := `SELECT * FROM "%s"."document"
              WHERE tub_name = $1
 				AND document_id = $2
		  `
		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q = fmt.Sprintf(q, schema)

		return tx.GetContext(ctx, &doc, q, tubname, documentId)
	})

	if err != nil {
		return ragnar.Document{}, err
	}

	return doc, err
}

// DeleteDocument deletes a document, will cascade to all referenced tables
func (d *DAO) DeleteDocument(ctx context.Context, tubname string, documentId string) error {
	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}

	return d.txx(ctx, func(tx *sqlx.Tx) error {

		err := allowedTubOperation(tx, ctx, tubname, auth.ALLOW_DELETE)
		if err != nil {
			return fmt.Errorf("error checking permission to delete document: %w", err)
		}

		schema, err := tubToSchema(tubname)
		if err != nil {
			return fmt.Errorf("error getting schema: %w", err)
		}
		q := `DELETE FROM "%s"."chunk"
              WHERE tub_name = $1
 				AND document_id = $2
		  `
		q = fmt.Sprintf(q, schema)
		_, err = tx.Exec(q, tubname, documentId)
		if err != nil {
			return fmt.Errorf("error deleting chunks: %w", err)
		}

		q = `DELETE FROM "%s"."document"
              WHERE tub_name = $1
 				AND document_id = $2
		  `
		q = fmt.Sprintf(q, schema)

		r, err := tx.Exec(q, tubname, documentId)
		if err != nil {
			return fmt.Errorf("error deleting document: %w", err)
		}
		n, err := r.RowsAffected()
		if err != nil {
			return fmt.Errorf("error getting rows affected: %w", err)
		}
		if n != 1 {
			return errors.New("document not found, n " + strconv.FormatInt(n, 10))
		}

		return nil
	})
}
