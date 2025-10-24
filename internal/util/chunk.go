package util

import "github.com/modfin/ragnar"

func ChunkSlicesContentEqual(a, b []ragnar.Chunk) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ChunkId != b[i].ChunkId {
			return false
		}
		if a[i].Content != b[i].Content {
			return false
		}
	}
	return true
}
