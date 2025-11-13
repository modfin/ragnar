package dao

import (
	"errors"
	"fmt"
	"github.com/modfin/bellman/models/embed"
	"strings"

	"github.com/modfin/ragnar"
)

func embedModelToColName(model embed.Model) (string, error) {
	name := strings.ToLower(model.Name)
	// replace all non-alphanumeric characters with underscores
	name = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}
		return '_'
	}, name)
	// trim leading and trailing underscores
	name = strings.Trim(name, "_")
	// if the name starts with a number, prefix it with an underscore
	if len(name) > 0 && name[0] >= '0' && name[0] <= '9' {
		name = "_" + name
	}
	// if the name is empty, return an error
	if name == "" {
		return "", errors.New("invalid model name, could not convert to column name")
	}
	return fmt.Sprintf("embedding_%s", name), nil
}

func (d *DAO) InternalDeleteChunks(doc ragnar.Document) error {

	tubname := doc.TubName
	schema, err := tubToSchema(tubname)
	if err != nil {
		return fmt.Errorf("error getting schema from tubname, %s: %w", tubname, err)
	}

	q := `DELETE FROM "%s".chunk WHERE document_id = $1 AND tub_id = $2`
	q = fmt.Sprintf(q, schema)

	_, err = d.db.Exec(q, doc.DocumentId, doc.TubId)
	if err != nil {
		return fmt.Errorf("error deleting chunks: %w", err)
	}

	return nil
}

func (d *DAO) InternalGetTub(tubId string) (ragnar.Tub, error) {

	q := `SELECT * FROM "public"."tub" WHERE tub_id = $1`

	var tub ragnar.Tub
	err := d.db.Get(&tub, q, tubId)
	if err != nil {
		return ragnar.Tub{}, fmt.Errorf("error getting tub: %w", err)
	}
	return tub, nil
}

func (d *DAO) InternalInsertChunk(chunk ragnar.Chunk) error {

	schema, err := tubToSchema(chunk.TubName)
	if err != nil {
		return fmt.Errorf("at InternalInsertChunk, error getting schema from tubname, %s: %w", chunk.TubName, err)
	}

	q := `INSERT INTO "%s"."chunk" (chunk_id, document_id, tub_id, tub_name, content) VALUES ($1, $2, $3, $4, $5)`

	q = fmt.Sprintf(q, schema)

	_, err = d.db.Exec(q, chunk.ChunkId, chunk.DocumentId, chunk.TubId, chunk.TubName, chunk.Content)
	if err != nil {
		return fmt.Errorf("at InternalInsertChunk, error inserting chunk: %w", err)
	}

	return nil
}

func (d *DAO) InternalInsertChunks(chunks []ragnar.Chunk) error {
	if len(chunks) == 0 {
		return nil
	}
	chunk := chunks[0]
	schema, err := tubToSchema(chunk.TubName)
	if err != nil {
		return fmt.Errorf("at InternalInsertChunks, error getting schema from tubname, %s: %w", chunk.TubName, err)
	}
	for _, c := range chunks {
		if c.TubName != chunk.TubName {
			return fmt.Errorf("mismatch in chunk tub name")
		}
	}

	q := `
INSERT INTO "%s"."chunk" (chunk_id, document_id, tub_id, tub_name, content) 
VALUES 
%s`

	values := make([]string, len(chunks))
	args := make([]interface{}, 0, len(chunks)*5)
	for i, chunk := range chunks {
		values[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d)", len(args)+1, len(args)+2, len(args)+3, len(args)+4, len(args)+5)
		args = append(args, chunk.ChunkId, chunk.DocumentId, chunk.TubId, chunk.TubName, chunk.Content)
	}
	q = fmt.Sprintf(q, schema, strings.Join(values, ",\n"))
	_, err = d.db.Exec(q, args...)
	if err != nil {
		return fmt.Errorf("at InternalInsertChunk, error inserting chunk: %w", err)
	}

	return nil
}

func (d *DAO) InternalGetChunks(doc ragnar.Document) ([]ragnar.Chunk, error) {
	tubname := doc.TubName
	schema, err := tubToSchema(tubname)
	if err != nil {
		return nil, fmt.Errorf("error getting schema from tubname, %s: %w", tubname, err)
	}

	q := `
SELECT tub_id, tub_name, document_id, chunk_id, content, created_at, updated_at 
FROM "%s".chunk 
WHERE document_id = $1 
  AND tub_id = $2 
ORDER BY chunk_id`
	q = fmt.Sprintf(q, schema)

	var chunks []ragnar.Chunk
	err = d.db.Select(&chunks, q, doc.DocumentId, doc.TubId)
	if err != nil {
		return nil, fmt.Errorf("error getting chunks: %w", err)
	}

	return chunks, nil
}

func (d *DAO) InternalEnsureTubEmbeddingSchema(doc ragnar.Document, model embed.Model) error {
	tubname := doc.TubName
	schema, err := tubToSchema(tubname)
	if err != nil {
		return fmt.Errorf("error getting schema from tubname, %s: %w", tubname, err)
	}
	colName, err := embedModelToColName(model)
	if err != nil {
		return fmt.Errorf("error getting column name from model, %s: %w", model.FQN(), err)
	}
	if model.OutputDimensions <= 0 {
		return fmt.Errorf("model %s has invalid output dimensions: %d", model.FQN(), model.OutputDimensions)
	}
	q := `SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='%s' AND table_name='chunk' AND column_name='%s');`
	q = fmt.Sprintf(q, schema, colName)
	// Check if the column already exists
	var exists bool
	err = d.db.Get(&exists, q)
	if err != nil {
		return fmt.Errorf("error checking if column exists: %w", err)
	}
	if exists {
		// Column already exists, nothing to do
		return nil
	}
	// Column does not exist, add it
	q = `ALTER TABLE "%s".chunk ADD COLUMN "%s" VECTOR(%d) DEFAULT NULL;`
	q = fmt.Sprintf(q, schema, colName, model.OutputDimensions)
	_, err = d.db.Exec(q)
	if err != nil {
		return fmt.Errorf("error adding column: %w", err)
	}
	// setup hnsw index on the column too
	q = `CREATE INDEX ON "%s".chunk USING hnsw (%s vector_cosine_ops);`
	q = fmt.Sprintf(q, schema, colName)
	_, err = d.db.Exec(q)
	if err != nil {
		return fmt.Errorf("error creating index: %w", err)
	}
	return nil
}

func (d *DAO) InternalSetEmbeds(doc ragnar.Document, model embed.Model, chunks []ragnar.Chunk, vectors [][]float32) error {
	if len(chunks) != len(vectors) {
		return fmt.Errorf("number of chunks (%d) does not match number of vectors (%d)", len(chunks), len(vectors))
	}
	if len(chunks) == 0 {
		return nil
	}
	tubname := doc.TubName
	schema, err := tubToSchema(tubname)
	if err != nil {
		return fmt.Errorf("error getting schema from tubname, %s: %w", tubname, err)
	}
	colName, err := embedModelToColName(model)
	if err != nil {
		return fmt.Errorf("error getting column name from model, %s: %w", model.FQN(), err)
	}
	q := `
UPDATE "%s".chunk AS o 
SET "%s" = n.embedding
FROM (VALUES 
%s
) AS n(document_id, tub_id, chunk_id, embedding)
WHERE o.document_id = n.document_id AND o.tub_id = n.tub_id AND o.chunk_id = n.chunk_id`
	values := make([]string, len(chunks))
	args := make([]any, 0, len(chunks)*4)
	for i, chunk := range chunks {
		values[i] = fmt.Sprintf("(CAST($%d AS TEXT), CAST($%d AS TEXT), CAST($%d AS INTEGER), CAST($%d AS VECTOR(%d)))", len(args)+1, len(args)+2, len(args)+3, len(args)+4, model.OutputDimensions)
		args = append(args, chunk.DocumentId, chunk.TubId, chunk.ChunkId, vectorToSQLArray(vectors[i]))
	}

	q = fmt.Sprintf(q, schema, colName, strings.Join(values, ",\n"))
	_, err = d.db.Exec(q, args...)
	if err != nil {
		return fmt.Errorf("error setting embeds: %w", err)
	}
	return nil
}

func vectorToSQLArray(vec []float32) string {
	strs := make([]string, len(vec))
	for i, v := range vec {
		strs[i] = fmt.Sprintf("%f", v)
	}
	return fmt.Sprintf("[%s]", strings.Join(strs, ","))
}
