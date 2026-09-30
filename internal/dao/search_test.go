package dao

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/modfin/bellman/models/embed"
	"github.com/modfin/ragnar"
	"github.com/modfin/ragnar/internal/auth"
)

// Needs a Postgres with pgvector, hstore and pgcrypto, e.g.
//
//	docker run -d -p 6790:5432 -e POSTGRES_PASSWORD=qwerty pgvector/pgvector:pg18
//	RAGNAR_TEST_DB_URI="postgres://postgres:qwerty@localhost:6790/postgres?sslmode=disable" go test ./internal/dao/
func testDAO(t *testing.T) (*DAO, context.Context) {
	t.Helper()
	uri := os.Getenv("RAGNAR_TEST_DB_URI")
	if uri == "" {
		t.Skip("RAGNAR_TEST_DB_URI not set")
	}
	d, err := New(slog.New(slog.NewTextHandler(io.Discard, nil)), Config{URI: uri}, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close(context.Background()) })
	var key string
	err = d.db.Get(&key, `INSERT INTO public.access_token (token_name, allow_create_tubs, allow_read_tubs)
		VALUES ('search-test', true, true) RETURNING access_key`)
	if err != nil {
		t.Fatal(err)
	}
	return d, context.WithValue(context.Background(), auth.ACCESS_KEY, key)
}

var testModel = embed.Model{Provider: "test", Name: "search-test-3d", OutputDimensions: 3}

type testChunk struct {
	content string
	vector  []float32
}

// seedTub creates a tub with one document per entry in docs, each with the
// given headers and chunks embedded with testModel.
func seedTub(t *testing.T, d *DAO, ctx context.Context, docs []map[string]string, chunks [][]testChunk) (string, []string) {
	t.Helper()
	tubName := fmt.Sprintf("search-test-%d", time.Now().UnixNano())
	tub, err := d.CreateTub(ctx, ragnar.Tub{TubName: tubName})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for i, headers := range docs {
		h := pgtype.Hstore{}
		for k, v := range headers {
			h[k] = &v
		}
		doc, err := d.UpsertDocument(ctx, ragnar.Document{TubId: tub.TubId, TubName: tubName, Headers: h})
		if err != nil {
			t.Fatal(err)
		}
		if err := d.InternalEnsureTubEmbeddingSchema(doc, testModel); err != nil {
			t.Fatal(err)
		}
		var rows []ragnar.Chunk
		var vectors [][]float32
		for j, c := range chunks[i] {
			chunk := ragnar.Chunk{TubId: tub.TubId, TubName: tubName, DocumentId: doc.DocumentId, ChunkId: j, Content: c.content}
			if err := d.InternalInsertChunk(chunk); err != nil {
				t.Fatal(err)
			}
			rows = append(rows, chunk)
			vectors = append(vectors, c.vector)
		}
		if err := d.InternalSetEmbeds(doc, testModel, rows, vectors); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, doc.DocumentId)
	}
	return tubName, ids
}

func TestSearchChunks(t *testing.T) {
	d, ctx := testDAO(t)
	tub, ids := seedTub(t, d, ctx,
		[]map[string]string{{"kind": "report", "title": "Volvo Q3", "year": "2025"}, {"kind": "filing", "title": "Atlas Copco", "year": "900"}},
		[][]testChunk{
			{
				{"Volvo Cars reported EBIT of 5.2 billion in the third quarter", []float32{1, 0, 0}},
				{"Margins improved on lower costs", []float32{0.9, 0.1, 0}},
				{"The outlook is unchanged", []float32{0.8, 0.2, 0}},
				{"Next report is due in February", []float32{0.7, 0.3, 0}},
			},
			{
				{"Atlas Copco share ISIN SE0017486889 listed on Nasdaq Stockholm", []float32{0, 0, 1}},
				{"Revenue grew in all regions", []float32{0.95, 0.05, 0}},
			},
		})
	query := []float32{1, 0, 0}
	search := func(p SearchParams) []ragnar.Chunk {
		t.Helper()
		p.Vector = query
		if p.Limit == 0 {
			p.Limit = 10
		}
		hits, err := d.SearchChunks(ctx, tub, testModel, p)
		if err != nil {
			t.Fatal(err)
		}
		return hits
	}
	type hit struct {
		doc   int
		chunk int
	}
	keys := func(hits []ragnar.Chunk) []hit {
		var out []hit
		for _, h := range hits {
			doc := 0
			if h.DocumentId == ids[1] {
				doc = 1
			}
			out = append(out, hit{doc, h.ChunkId})
		}
		return out
	}

	t.Run("vector ranks by similarity with score and headers", func(t *testing.T) {
		hits := search(SearchParams{Limit: 3})
		if got, want := fmt.Sprint(keys(hits)), fmt.Sprint([]hit{{0, 0}, {1, 1}, {0, 1}}); got != want {
			t.Fatalf("hits = %s, want %s", got, want)
		}
		if hits[0].Score == nil || *hits[0].Score < 0.999 || *hits[1].Score >= *hits[0].Score {
			t.Fatalf("scores not cosine similarity, best first: %v %v", hits[0].Score, hits[1].Score)
		}
		if *hits[0].Headers["title"] != "Volvo Q3" || *hits[1].Headers["title"] != "Atlas Copco" {
			t.Fatalf("headers = %v, %v", hits[0].Headers, hits[1].Headers)
		}
		if hits[0].Before != nil || hits[0].After != nil {
			t.Fatal("neighbours returned without asking")
		}
	})

	t.Run("offset pages through the same ranking", func(t *testing.T) {
		all := keys(search(SearchParams{Limit: 6}))
		page := keys(search(SearchParams{Limit: 2, Offset: 2}))
		if fmt.Sprint(page) != fmt.Sprint(all[2:4]) {
			t.Fatalf("page = %v, want %v", page, all[2:4])
		}
	})

	t.Run("max per document", func(t *testing.T) {
		hits := keys(search(SearchParams{MaxPerDocument: 1}))
		if fmt.Sprint(hits) != fmt.Sprint([]hit{{0, 0}, {1, 1}}) {
			t.Fatalf("hits = %v", hits)
		}
		hits = keys(search(SearchParams{MaxPerDocument: 2, Limit: 3}))
		if fmt.Sprint(hits) != fmt.Sprint([]hit{{0, 0}, {1, 1}, {0, 1}}) {
			t.Fatalf("hits = %v", hits)
		}
	})

	t.Run("neighbours", func(t *testing.T) {
		hits := search(SearchParams{Limit: 1, Filter: ragnar.NewDocumentFilter().WithEqual("document_id", ids[0]), Neighbours: 2})
		if len(hits) != 1 || hits[0].ChunkId != 0 || len(hits[0].Before) != 0 || len(hits[0].After) != 2 ||
			hits[0].After[0].ChunkId != 1 || hits[0].After[1].Content != "The outlook is unchanged" {
			t.Fatalf("hit = %+v", hits)
		}
		hits = search(SearchParams{Limit: 10, Filter: ragnar.NewDocumentFilter().WithEqual("document_id", ids[0]), Neighbours: 1})
		for _, h := range hits {
			if h.ChunkId == 2 && (len(h.Before) != 1 || h.Before[0].ChunkId != 1 || len(h.After) != 1 || h.After[0].ChunkId != 3) {
				t.Fatalf("middle hit = %+v", h)
			}
		}
	})

	t.Run("filters", func(t *testing.T) {
		docsOf := func(f ragnar.DocumentFilter) string {
			seen := map[int]bool{}
			for _, h := range keys(search(SearchParams{Filter: f})) {
				seen[h.doc] = true
			}
			return fmt.Sprint(seen)
		}
		for name, c := range map[string]struct {
			filter ragnar.DocumentFilter
			want   string
		}{
			"equal":             {ragnar.NewDocumentFilter().WithEqual("kind", "filing"), "map[1:true]"},
			"key case":          {ragnar.NewDocumentFilter().WithEqual("KIND", "filing"), "map[1:true]"},
			"in":                {ragnar.NewDocumentFilter().WithIn("kind", []string{"filing", "report"}), "map[0:true 1:true]"},
			"none match":        {ragnar.NewDocumentFilter().WithIn("kind", []string{"nope"}), "map[]"},
			"integer compare":   {ragnar.NewDocumentFilter().WithCondition("year", ragnar.OpGreaterThan, "1000", ragnar.ValueTypeInteger), "map[0:true]"},
			"text compare":      {ragnar.NewDocumentFilter().WithCondition("year", ragnar.OpGreaterThan, "1000", ragnar.ValueTypeText), "map[0:true 1:true]"},
			"and on one field":  {ragnar.NewDocumentFilter().WithCondition("year", ragnar.OpGreaterThanOrEqual, "900", ragnar.ValueTypeInteger).WithCondition("year", ragnar.OpLessThan, "2025", ragnar.ValueTypeInteger), "map[1:true]"},
			"document id in":    {ragnar.NewDocumentFilter().WithIn("document_id", []string{ids[1]}), "map[1:true]"},
			"document id equal": {ragnar.NewDocumentFilter().WithCondition("document_id", ragnar.OpEqual, ids[0], ragnar.ValueTypeText), "map[0:true]"},
		} {
			if got := docsOf(c.filter); got != c.want {
				t.Errorf("%s: documents = %s, want %s", name, got, c.want)
			}
		}
	})

	t.Run("invalid params", func(t *testing.T) {
		for _, p := range []SearchParams{{Neighbours: 6}, {Neighbours: -1}, {MaxPerDocument: -1}} {
			p.Vector = query
			if _, err := d.SearchChunks(ctx, tub, testModel, p); err == nil {
				t.Fatalf("no error for %+v", p)
			}
		}
	})
}
