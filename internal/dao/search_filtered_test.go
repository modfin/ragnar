package dao

import (
	"fmt"
	"math"
	"testing"

	"github.com/modfin/ragnar"
)

// A filter matching chunks far from the query must still return them. HNSW
// alone yields only about ef_search nearest chunks across the whole tub, so a
// filter applied to those finds nothing once the tub is large.
func TestSearchChunksFilterFindsDistantMatches(t *testing.T) {
	d, ctx := testDAO(t)
	tub, ids := seedTub(t, d, ctx,
		[]map[string]string{{"trade-company": "1", "publish-date": "2026-01-01T00:00:00Z"}},
		[][]testChunk{{
			{"Target far", []float32{0, 0, 1}},
			{"Target farther", []float32{0, 0.1, 1}},
			{"Target near", []float32{0.3, 0, 1}},
		}})
	schema, _ := tubToSchema(tub)
	col, _ := embedModelToColName(testModel)
	var tubID string
	if err := d.db.Get(&tubID, `SELECT tub_id FROM public.tub WHERE tub_name = $1`, tub); err != nil {
		t.Fatal(err)
	}
	// Bulk noise: documents of other companies, all closer to the query.
	_, err := d.db.Exec(fmt.Sprintf(`
		INSERT INTO "%[1]s".document (document_id, tub_id, tub_name, headers)
		SELECT 'noise_' || i, $1, $2, hstore(ARRAY['trade-company', (2 + i %% 50)::text, 'publish-date', '2025-06-01T00:00:00Z'])
		FROM generate_series(1, 2000) i`, schema), tubID, tub)
	if err != nil {
		t.Fatal(err)
	}
	_, err = d.db.Exec(fmt.Sprintf(`
		INSERT INTO "%[1]s".chunk (chunk_id, document_id, tub_id, tub_name, content, "%[2]s")
		SELECT c, 'noise_' || i, $1, $2, 'noise',
		       CAST('[1,' || (0.001 * ((i * 7 + c) %% 997))::text || ',' || (0.0001 * c)::text || ']' AS VECTOR(3))
		FROM generate_series(1, 2000) i, generate_series(0, 2) c`, schema, col), tubID, tub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.db.Exec(fmt.Sprintf(`ANALYZE "%[1]s".chunk; ANALYZE "%[1]s".document`, schema)); err != nil {
		t.Fatal(err)
	}

	search := func(p SearchParams) []ragnar.Chunk {
		t.Helper()
		p.Vector = []float32{1, 0, 0}
		hits, err := d.SearchChunks(ctx, tub, testModel, p)
		if err != nil {
			t.Fatal(err)
		}
		return hits
	}
	company := ragnar.NewDocumentFilter().WithEqual("trade-company", "1")
	for _, limit := range []int{1, 2, 10, 100} {
		hits := search(SearchParams{Limit: limit, Filter: company})
		if len(hits) != min(limit, 3) {
			t.Fatalf("limit %d: %d hits, want %d", limit, len(hits), min(limit, 3))
		}
		for _, h := range hits {
			if h.DocumentId != ids[0] {
				t.Fatalf("limit %d: hit from %s outside the filter", limit, h.DocumentId)
			}
		}
		if hits[0].Content != "Target near" {
			t.Fatalf("limit %d: best hit %q, want the nearest matching chunk", limit, hits[0].Content)
		}
	}
	page2 := search(SearchParams{Limit: 1, Offset: 1, Filter: company})
	if len(page2) != 1 || page2[0].Content != "Target far" {
		t.Fatalf("second page %+v", page2)
	}

	// A broad filter matches most of the tub: the index is still used and
	// returns a full page in order.
	broad := ragnar.NewDocumentFilter().WithCondition("publish-date", ragnar.OpLessThan, "2025-12-31T00:00:00Z", ragnar.ValueTypeText)
	hits := search(SearchParams{Limit: 50, Filter: broad})
	if len(hits) != 50 {
		t.Fatalf("broad filter: %d hits", len(hits))
	}
	for i := 1; i < len(hits); i++ {
		if *hits[i].Score > *hits[i-1].Score+1e-9 || math.IsNaN(*hits[i].Score) {
			t.Fatalf("broad filter hits out of order at %d", i)
		}
	}
}
