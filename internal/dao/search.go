package dao

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/modfin/bellman/models/embed"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/auth"
)

type SearchParams struct {
	// Vector is the query embedded with the tub's model.
	Vector         []float32
	Filter         ragnar.DocumentFilter
	Limit          int
	Offset         int
	MaxPerDocument int
	Neighbours     int
}

const (
	// Candidates fetched when hits are capped per document, so there are
	// enough left to fill a page. pgvector's HNSW ef_search tops out at 1000.
	minCandidates = 50
	maxCandidates = 1000
	// hnswDefaultEfSearch is pgvector's default hnsw.ef_search.
	hnswDefaultEfSearch = 40
	// filteredMaxScanTuples bounds an iterative index scan under a filter.
	// A filter so selective that this many chunks yield no full page matches
	// few enough chunks to rank exactly instead.
	filteredMaxScanTuples = 20000
)

// SearchChunks returns the chunks most similar to the query vector, best
// first. Only chunks embedded with the tub's model are searched, so documents
// still processing do not show up.
func (d *DAO) SearchChunks(ctx context.Context, tubname string, model embed.Model, p SearchParams) ([]ragnar.Chunk, error) {
	var chunks []ragnar.Chunk

	tubname = strings.ToLower(tubname)
	if !bucketNameRegExp.MatchString(tubname) {
		return chunks, errors.New("tub name must only contain a-z0-9_-, and be at least 3 character long")
	}
	if p.Limit < 0 || p.Offset < 0 || p.MaxPerDocument < 0 {
		return chunks, errors.New("limit, offset and max per document must not be negative")
	}
	if p.Neighbours < 0 || p.Neighbours > ragnar.MaxSearchNeighbours {
		return chunks, fmt.Errorf("neighbours must be between 0 and %d", ragnar.MaxSearchNeighbours)
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

		// Without a cap the vector ranking is the result, so fetch exactly one
		// page's worth, as search always has.
		candidates := p.Limit + p.Offset
		if p.MaxPerDocument > 0 {
			candidates = min(max(candidates*3, minCandidates), maxCandidates)
			candidates = max(candidates, p.Limit+p.Offset)
		}
		// HNSW returns at most ef_search rows, so a larger page would come back short.
		if efSearch := min(candidates, maxCandidates); efSearch > hnswDefaultEfSearch {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL hnsw.ef_search = %d", efSearch)); err != nil {
				return fmt.Errorf("error setting hnsw.ef_search: %w", err)
			}
		}

		filterSQL, filterArgs, err := documentFilterSQL(p.Filter, 2)
		if err != nil {
			return err
		}
		filtered := filterSQL != ""
		// Without iterative scans HNSW returns ef_search chunks in total and
		// the filter is applied to those, so a filter on a small part of the
		// tub found nothing.
		if filtered {
			for _, set := range []string{"SET LOCAL hnsw.iterative_scan = strict_order", fmt.Sprintf("SET LOCAL hnsw.max_scan_tuples = %d", filteredMaxScanTuples)} {
				if _, err := tx.ExecContext(ctx, set); err != nil {
					return fmt.Errorf("error enabling iterative index scans: %w", err)
				}
			}
		}

		query := func(exact bool) error {
			chunks = nil
			args := []any{vectorToSQLArray(p.Vector)}
			args = append(args, filterArgs...)
			arg := func(v any) string {
				args = append(args, v)
				return fmt.Sprintf("$%d", len(args))
			}
			distance := fmt.Sprintf(`chunk."%s" <=> CAST($1 AS VECTOR(%d))`, colName, model.OutputDimensions)
			// The index only serves an ORDER BY on the bare distance operator.
			order := distance
			if exact {
				order = "(" + distance + ") + 0"
			}
			maxPerDocument := arg(p.MaxPerDocument)
			q := fmt.Sprintf(searchSQL, distance, schema, schema, colName, filterSQL, order, arg(candidates),
				schema, schema, maxPerDocument, maxPerDocument, arg(p.Limit), arg(p.Offset))
			return tx.SelectContext(ctx, &chunks, q, args...)
		}
		if err := query(false); err != nil {
			return fmt.Errorf("error searching chunks: %w", err)
		}
		// A short page under a filter is either the end of the matches or an
		// index scan that gave up; ranking the matching chunks exactly tells.
		if filtered && len(chunks) < p.Limit {
			if err := query(true); err != nil {
				return fmt.Errorf("error searching chunks exactly: %w", err)
			}
		}
		if p.Neighbours > 0 && len(chunks) > 0 {
			return addNeighbours(ctx, tx, schema, chunks, p.Neighbours)
		}
		return nil
	})
	if err != nil {
		return chunks, err
	}
	return chunks, nil
}

const searchSQL = `
WITH candidates AS (
	SELECT chunk.document_id, chunk.chunk_id, CAST(1 - (%s) AS FLOAT8) AS score
	FROM "%s".chunk
	INNER JOIN "%s".document USING (tub_id, document_id)
	WHERE chunk."%s" IS NOT NULL
	%s
	ORDER BY %s
	LIMIT %s
),
ranked AS (
	SELECT document_id, chunk_id, score,
	       row_number() OVER (PARTITION BY document_id ORDER BY score DESC, chunk_id) AS document_rank
	FROM candidates
)
SELECT chunk.tub_id, chunk.tub_name, chunk.document_id, chunk.chunk_id, chunk.content,
       chunk.created_at, chunk.updated_at, document.headers, ranked.score
FROM ranked
INNER JOIN "%s".chunk USING (document_id, chunk_id)
INNER JOIN "%s".document ON document.document_id = chunk.document_id
WHERE CAST(%s AS INT) = 0 OR ranked.document_rank <= %s
ORDER BY ranked.score DESC, chunk.document_id, chunk.chunk_id
LIMIT %s
OFFSET %s`

// addNeighbours fills Before and After on each hit with the chunks up to n
// positions away in the same document.
func addNeighbours(ctx context.Context, tx *sqlx.Tx, schema string, hits []ragnar.Chunk, n int) error {
	documentIds := make([]string, len(hits))
	chunkIds := make([]int, len(hits))
	for i, hit := range hits {
		documentIds[i], chunkIds[i] = hit.DocumentId, hit.ChunkId
	}
	var rows []struct {
		DocumentId string `db:"document_id"`
		ragnar.NeighbourChunk
	}
	q := fmt.Sprintf(`SELECT DISTINCT chunk.document_id, chunk.chunk_id, chunk.content
		FROM "%s".chunk
		INNER JOIN unnest(CAST($1 AS TEXT[]), CAST($2 AS INT[])) AS hit(document_id, chunk_id)
		        ON chunk.document_id = hit.document_id
		       AND chunk.chunk_id BETWEEN hit.chunk_id - $3 AND hit.chunk_id + $3
		       AND chunk.chunk_id <> hit.chunk_id
		ORDER BY chunk.document_id, chunk.chunk_id`, schema)
	if err := tx.SelectContext(ctx, &rows, q, documentIds, chunkIds, n); err != nil {
		return fmt.Errorf("error getting neighbouring chunks: %w", err)
	}
	for i := range hits {
		hit := &hits[i]
		for _, row := range rows {
			if row.DocumentId != hit.DocumentId {
				continue
			}
			switch {
			case row.ChunkId >= hit.ChunkId-n && row.ChunkId < hit.ChunkId:
				hit.Before = append(hit.Before, row.NeighbourChunk)
			case row.ChunkId > hit.ChunkId && row.ChunkId <= hit.ChunkId+n:
				hit.After = append(hit.After, row.NeighbourChunk)
			}
		}
	}
	return nil
}
