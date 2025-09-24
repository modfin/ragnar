package chunker

import (
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/modfin/ragnar/internal/util"
	"github.com/tmc/langchaingo/textsplitter"
	"strconv"
	"strings"
)

func GetTextSplitterFromTubSettings(settings pgtype.Hstore) textsplitter.TextSplitter {
	var ops []textsplitter.Option

	splitter, ok := settings["chunk_splitter"]
	if !ok || splitter == nil {
		splitter = util.Ptr("markdown")
	}

	chunkSize := 512
	chunkSizeStr, ok := settings["chunk_size"]
	if ok && chunkSizeStr != nil {
		_chunkSize, err := strconv.Atoi(*chunkSizeStr)
		if err == nil {
			chunkSize = _chunkSize
		}
	}
	ops = append(ops, textsplitter.WithChunkSize(chunkSize))

	chunkOverlap := 0 // Default no overlap, we want to use context aware chunking instead
	chunkOverlapStr, ok := settings["chunk_overlap"]
	if ok && chunkOverlapStr != nil {
		_chunkOverlap, err := strconv.Atoi(*chunkOverlapStr)
		if err == nil {
			chunkOverlap = _chunkOverlap
		}
	}
	ops = append(ops, textsplitter.WithChunkOverlap(chunkOverlap)) // Default chunk overlap is 0 so

	chunkSeparators := []string{"\n\n"}                    // Default to only chunk in paragraphs in markdown
	chunkSeparatorsStr, ok := settings["chunk_separators"] // comma separated string
	if ok && chunkSeparatorsStr != nil {
		chunkSeparators = []string{}
		for _, sep := range strings.Split(*chunkSeparatorsStr, ",") {
			chunkSeparators = append(chunkSeparators, sep)
		}
	}
	ops = append(ops, textsplitter.WithSeparators(chunkSeparators))

	headingHierarchy := true // Default to only chunk in paragraphs in markdown
	headingHierarchyStr, ok := settings["chunk_heading_hierarchy"]
	if ok && headingHierarchyStr != nil {
		headingHierarchy = *headingHierarchyStr == "true"
	}
	ops = append(ops, textsplitter.WithHeadingHierarchy(headingHierarchy))

	switch *splitter {
	case "token":
		return textsplitter.NewTokenSplitter(ops...)
	case "recursive":
		return textsplitter.NewRecursiveCharacter(ops...)
	case "markdown":
		fallthrough
	default:
		return textsplitter.NewMarkdownTextSplitter(ops...)

	}
}
