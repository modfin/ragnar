package ragnar

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"io"
	"strings"
	"testing"
	"time"
)

var ragnarClient = NewClient(ClientConfig{
	BaseURL:   "http://localhost:7100",
	AccessKey: "rag_cdd77e0b-9931-45c5-914d-ebe1c15c1913", // todo figure out how to manage test access keys
})

const tubTestName = "mfn-test"

// Integration tests - require running server
func TestCreateTub(t *testing.T) {
	tub, err := ragnarClient.CreateTub(
		context.Background(),
		Tub{TubName: tubTestName},
	)
	if err != nil {
		t.Fatal(err)
	}
	if tub.TubName != tubTestName {
		t.Fatalf("expected tub name %s, got %s", tubTestName, tub.TubName)
	}
}

func TestGetTubs(t *testing.T) {
	tubs, err := ragnarClient.GetTubs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(tubs) == 0 {
		t.Fatal("expected to find at least one tub")
	}
	fmt.Println(">>>tubs", tubs)
}

func TestGetTub(t *testing.T) {
	tub, err := ragnarClient.GetTub(context.Background(), tubTestName)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>tub", tub)

	if tub.TubName != tubTestName {
		t.Fatalf("expected tub name %s, got %s", tubTestName, tub.TubName)
	}
}

func TestUpdateTub(t *testing.T) {
	description := fmt.Sprintf("Test tub description %d", time.Now().Unix())
	updatedTub := Tub{
		TubName:  tubTestName,
		Settings: pgtype.Hstore{"description": &description},
	}.WithRequiredDocumentHeaders("mfn-news-id")

	result, err := ragnarClient.UpdateTub(context.Background(), updatedTub)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>updated tub", result)

	if result.TubName != tubTestName {
		t.Fatalf("expected tub name %s, got %s", tubTestName, result.TubName)
	}
	if desc, ok := result.Settings["description"]; !ok || *desc != description {
		t.Fatalf("expected description to be 'Updated test tub description', got '%v'", result.Settings["description"])
	}
}

func TestGetTubDocuments(t *testing.T) {
	docs, err := ragnarClient.GetTubDocuments(context.Background(), tubTestName, nil, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>documents", docs)
}

func TestInvalidTubDocument(t *testing.T) {
	content := strings.NewReader("This is bad test document content")
	headers := map[string]string{
		"Content-Type":      "text/plain",
		"x-ragnar-filename": "test.txt",
	}
	_, err := ragnarClient.CreateTubDocument(context.Background(), tubTestName, content, headers)
	if err == nil {
		t.Fatal("expected error creating document without required header")
	}
	if err.Error() != "HTTP 400: 400 Bad Request" {
		t.Fatal("expected 400 Bad Request error creating document without required header, got", err)
	}
}

func TestTubDocument(t *testing.T) {
	content := strings.NewReader("This is test document content")
	mfnId := "test-id-12345"
	headers := map[string]string{
		"Content-Type":         "text/plain",
		"x-ragnar-filename":    "test.txt",
		"x-ragnar-mfn-news-id": mfnId,
	}

	doc, err := ragnarClient.CreateTubDocument(context.Background(), tubTestName, content, headers)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>created document", doc)
	if doc.DocumentId == "" {
		t.Fatal("expected document ID to be set")
	}
	if doc.TubName != tubTestName {
		t.Fatalf("expected tub name %s, got %s", tubTestName, doc.TubName)
	}
	if *doc.Headers["content-type"] != "text/plain" {
		t.Fatalf("expected content-type header to be 'text/plain', got '%v'", doc.Headers["content-type"])
	}
	if *doc.Headers["filename"] != "test.txt" {
		t.Fatalf("expected filename header to be 'test.txt', got '%v'", doc.Headers["filename"])
	}
	fetchedDoc, err := ragnarClient.GetTubDocument(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>fetched document", fetchedDoc)
	if fetchedDoc.DocumentId != doc.DocumentId {
		t.Fatal("expected document ID to be set", fetchedDoc.DocumentId, doc.DocumentId)
	}
	fetchedDocs, err := ragnarClient.GetTubDocuments(context.Background(), tubTestName, map[string]any{"mfn-news-id": []string{mfnId}}, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>fetched documents", fetchedDocs)
	if len(fetchedDocs) == 0 {
		t.Fatal("expected to find at least one document")
	}
	if fetchedDocs[0].DocumentId != doc.DocumentId {
		t.Fatal("expected document ID to be set", fetchedDocs[0].DocumentId, doc.DocumentId)
	}
	noMatchDocs, err := ragnarClient.GetTubDocuments(context.Background(), tubTestName, map[string]any{"mfn-news-id": "test-id-4321"}, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>fetched (no match) documents", noMatchDocs)
	if len(noMatchDocs) != 0 {
		t.Fatal("expected to find no documents")
	}
	reader1, err := ragnarClient.DownloadTubDocument(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	defer reader1.Close()
	downloadedContent, err := io.ReadAll(reader1)
	if err != nil {
		t.Fatal(err)
	}
	if string(downloadedContent) != "This is test document content" {
		t.Fatalf("expected downloaded content to be 'This is updated document content', got '%s'", string(downloadedContent))
	}

	content = strings.NewReader("This is updated document content")
	headers = map[string]string{
		"Content-Type":      "text/plain",
		"x-ragnar-filename": "test.txt",
		"x-ragnar-test":     "test-header",
	}
	err = waitUntilStatusCompletedOrTimeout(tubTestName, doc.DocumentId, time.Minute)
	if err != nil {
		t.Fatal("document is not completed")
	}
	updatedDoc, err := ragnarClient.UpdateTubDocument(context.Background(), tubTestName, doc.DocumentId, content, headers)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>updated document", updatedDoc)
	if updatedDoc.DocumentId != doc.DocumentId {
		t.Fatal(fmt.Sprintf("expected document ID to be set. expected: %s, got: %s", doc.DocumentId, updatedDoc.DocumentId))
	}
	if *updatedDoc.Headers["content-type"] != "text/plain" {
		t.Fatalf("expected content-type header to be 'text/plain', got '%v'", doc.Headers["content-type"])
	}
	if *updatedDoc.Headers["filename"] != "test.txt" {
		t.Fatalf("expected filename header to be 'test.txt', got '%v'", doc.Headers["filename"])
	}
	if *updatedDoc.Headers["test"] != "test-header" {
		t.Fatalf("expected test header to be 'test-header', got '%v'", updatedDoc.Headers["test"])
	}
	reader2, err := ragnarClient.DownloadTubDocument(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	defer reader2.Close()
	downloadedContent, err = io.ReadAll(reader2)
	if err != nil {
		t.Fatal(err)
	}
	if string(downloadedContent) != "This is updated document content" {
		t.Fatalf("expected downloaded content to be 'This is updated document content', got '%s'", string(downloadedContent))
	}
	fmt.Println(">>>downloaded content", string(downloadedContent))

	err = ragnarClient.DeleteTubDocument(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>document deleted successfully")

	_, err = ragnarClient.GetTubDocument(context.Background(), tubTestName, doc.DocumentId)
	if err == nil {
		t.Fatal("expected error fetching deleted document")
	}
	fmt.Println(">>>error fetching deleted document (expected)", err)
}

func TestGetTubDocumentChunks(t *testing.T) {
	mfnPressReleaseContent := strings.NewReader("<div class=\"title\">\n    <a href=\"/cis/a/spotlight-group/spotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932\">Spotlight Group: Invoicery Group godkänt för listning på Spotlight Value</a>\n</div>\n<div class=\"publish-date\">\n      2025-09-18 14:00:00\n\n</div>\n\n<div class=\"content s-cis\">\n\n\n<div class=\"mfn-preamble\"><strong><p><span><span><span><span><span><span><span>Spotlight Group meddelar att dotterbolaget Spotlight Stock Market har godkänt Invoicery Group för listning på Spotlight Value. Bolaget, som i Sverige är mer känt under ett av sina svenska varumärken Frilans Finans, blir därmed det sjunde bolaget på listan och det första nya sedan Spotlight Value lanserades den 17 juni 2025. Första dag för handel i Invoicery Group på Spotlight Value planeras till onsdagen den 24 september 2025.</span></span></span></span></span></span></span></p></strong></div><p><span><span><span><span><span><span><span><span>\"Det är alltid roligt att andra observerar och uppskattar det arbete som vi gör i vår verksamhet, särskilt i en tid då uppdragsbaserat arbete får allt större uppmärksamhet på arbetsmarknaden\", säger Invoicerys VD Stephen Schad.</span></span></span></span></span></span></span></span></p><p><span><span><span><span><span><span><span><span>Kraven för att listas på Spotlight Value är att bolaget ska ha visat vinst på sista raden de tre senaste åren, ha haft positiv tillväxt under minst två av de tre senaste åren och ha gett utdelning till sina aktieägaren minst två av de tre senaste åren. Invoicery lever därmed upp till alla dessa krav.</span></span></span></span></span></span></span></span></p><p><span><span><span><span><span><span><span><span>\"Spotlight Value har tagits emot med stort intresse från såväl investerare som bolag och vi är väldigt glada över att kunna välkomna Invoicery som nytt Spotlight Value-bolag. Med dem blir listan ännu starkare och ännu mer attraktiv för investerarna. Vi hoppas att de får sällskap snart av fler stabila bolag som i dag har andra listningar eller kanske inte är noterade alls\", säger Spotlight Stock Markets VD Peter Gönczi.</span></span></span></span></span></span></span></span></p><p><span><span><span><span><span><span><span><span>För att kvalificera sig för Spotlight Value måste ett bolag ha haft vinst på sista raden de tre senaste åren, visat tillväxt minst två av de tre senaste tre åren och dessutom ha gett aktieutdelning minst två av de tre senaste åren. Med avdrag för engångskostnader relaterade till noteringen lever Invoicery upp till alla dessa krav. Sedan tidigare finns sex bolag på Spotlight Value: Homemaid, Veteranpoolen, Gosol Energy Group, Transferator, Aquaticus Real Estate och Logistri Fastighets AB.</span></span></span></span></span></span></span></span></p><div></div><div class=\"mfn-footer\"><p><span><span><span><span><span><span><span><span><span><strong><span><span><span>För ytterligare information om Spotlight Group, vänligen kontakta:</span></span></span></strong></span></span></span></span></span></span></span></span></span><br><span><span><span><span><span><span><span><span><span><span><span><span>Peter Gönczi, VD</span></span></span></span></span></span></span></span></span></span></span></span><br><span><span><span><span><span><span><span><span><span><span><span><span>E-post: ir@spotlightgroup.se</span></span></span></span></span></span></span></span></span></span></span></span><br><span><span><span><span><span><span><span><span><span><span><span><span>Hemsida: www.spotlightgroup.se </span></span></span></span></span></span></span></span></span></span></span></span></p></div>\n\n</div>\n\n\n\n\n<div class=\"footer\">\n     \n     <div class=\"source\">\n        Källa <strong>Cision</strong>\n     </div>\n     \n     \n\n<div class=\"social-tray\">\n    <a class=\"social-ico social-mail\" title=\"Share mail\" rel=\"noopener\" href=\"mailto:?subject=Hej%2C%20jag%20vill%20dela%20denna%20nyhet%20fr%C3%A5n%20mfn.se%20med%20dig%2C%20Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&amp;body=https://mfn.se/cis/a/spotlight-group/spotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932\"></a>\n    <a class=\"social-ico social-twitter\" title=\"Share twitter\" target=\"_blank\" rel=\"noopener\" href=\"https://twitter.com/intent/tweet?url=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932&amp;text=Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&amp;via=MFN_IRnews\"></a>\n    <a class=\"social-ico social-linked-in\" title=\"Share LinkedIn\" target=\"_blank\" rel=\"noopener\" href=\"http://www.linkedin.com/shareArticle?mini=true&amp;url=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932&amp;title=Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&amp;summary=&amp;source=mfn.se\"></a>\n    <a class=\"social-ico social-facebook\" title=\"Share Facebook\" target=\"_blank\" rel=\"noopener\" href=\"http://www.facebook.com/sharer/sharer.php?u=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932\"></a>\n</div>\n</div>\n\n")
	headers := map[string]string{
		"Content-Type":         "text/html",
		"x-ragnar-filename":    "test.txt",
		"x-ragnar-mfn-news-id": "eb8bb932-58b0-5aaa-9850-13029c3830d0",
	}
	doc, err := ragnarClient.CreateTubDocument(context.Background(), tubTestName, mfnPressReleaseContent, headers)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Headers["mfn-news-id"] == nil || *doc.Headers["mfn-news-id"] != "eb8bb932-58b0-5aaa-9850-13029c3830d0" {
		t.Fatal("expected mfn-news-id header to be set")
	}
	docStatus, err := ragnarClient.GetTubDocumentStatus(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	if docStatus.Status != "processing" {
		t.Fatal("expected document status to be 'processing', got", docStatus.Status)
	}
	fmt.Println(">>>created document", doc)
	statusErr := waitUntilStatusCompletedOrTimeout(tubTestName, doc.DocumentId, time.Minute*10)
	if statusErr != nil {
		t.Fatal("document is not completed")
	}

	markdown, err := ragnarClient.DownloadTubDocumentMarkdown(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	defer markdown.Close()
	mdContent, err := io.ReadAll(markdown)
	if err != nil {
		t.Fatal(err)
	}
	if len(mdContent) == 0 {
		t.Fatal("expected markdown content to be created")
	}
	//fmt.Printf(">>>document markdown content\n'%s'\n", string(mdContent))

	chunks, err := ragnarClient.GetTubDocumentChunks(context.Background(), tubTestName, doc.DocumentId, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 {
		t.Fatal("expected chunks to be created")
	}
	fmt.Println(">>>chunks", len(chunks))
	for i, chunk := range chunks {
		fmt.Printf(">>>chunk %d: \n%+v\n\n", i, chunk.Content)
	}
}

func TestGetTubDocumentWithOptionalMarkdown(t *testing.T) {
	mfnPressReleaseContent := strings.NewReader("<div class=\"title\">\n    <a href=\"/a/k33/k33-completes-strategic-purchase-of-15-bitcoin\">K33 Completes Strategic Purchase of 15 Bitcoin</a>\n</div>\n<div class=\"publish-date\">\n      2025-09-22 14:40:00\n\n</div>\n\n<div class=\"content s-mfn\">\n\n\n<div class=\"mfn-preamble\"><p><strong>K33 AB (publ) (\"K33\"), a leading digital asset brokerage and research firm, announces the acquisition of 15 Bitcoin (BTC) for a total consideration of approximately SEK 16.0 million.</strong></p></div>\n<div class=\"mfn-body\"><p>Following today’s transaction, K33 holds a total of 141 BTC on its balance sheet, with an average acquisition cost of SEK 1,114,859 per BTC.</p><p>K33’s Bitcoin Treasury strategy reflects both the company’s conviction in Bitcoin’s long-term value proposition and its intention to establish a strong position in the asset to unlock operational alpha in its broker business.</p></div>\n<div class=\"mfn-footer mfn-contacts mfn-88304a0cc28f\"><p><strong class=\"mfn-heading-1\">For further information, please contact:</strong><br>Torbjørn Bull Jenssen, CEO, K33 AB (publ)<br>E-mail: ir@k33.com<br>Web: k33.com/ir</p></div>\n<div class=\"mfn-footer mfn-about mfn-3dfe054bd57f\"><p><strong class=\"mfn-heading-1\">About K33</strong><br>K33 AB (publ), listed on Nasdaq First North Growth Market, is the new gold standard for investments in digital assets. <a href=\"http://k33.com\" rel=\"noopener\" target=\"_blank\">K33</a> offers market-leading execution, actionable insights, and superior support to private and institutional partners across EMEA. Mangold Fondkommission serves as the Certified Adviser for K33 AB (publ).</p></div>\n<div class=\"mfn-footer mfn-attachment mfn-attachment-general\"><p><strong class=\"mfn-heading-1\">Attachments</strong><br><a class=\"mfn-generated mfn-primary\" href=\"https://storage.mfn.se/80ec718e-8584-4d4c-90fe-a02b5e0540c4/k33-completes-strategic-purchase-of-15-bitcoin.pdf\" rel=\"noopener\" target=\"_blank\">K33 Completes Strategic Purchase of 15 Bitcoin</a></p></div>\n\n</div>\n\n\n\n\n<div class=\"footer\">\n     \n     <div class=\"source\">\n        Källa <strong>MFN</strong>\n     </div>\n     \n     \n\n<div class=\"social-tray\">\n    <a class=\"social-ico social-mail\" title=\"Share mail\" rel=\"noopener\" href=\"mailto:?subject=Hej%2C%20jag%20vill%20dela%20denna%20nyhet%20fr%C3%A5n%20mfn.se%20med%20dig%2C%20K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;body=https://mfn.se/a/k33/k33-completes-strategic-purchase-of-15-bitcoin\"></a>\n    <a class=\"social-ico social-twitter\" title=\"Share twitter\" target=\"_blank\" rel=\"noopener\" href=\"https://twitter.com/intent/tweet?url=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin&amp;text=K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;via=MFN_IRnews\"></a>\n    <a class=\"social-ico social-linked-in\" title=\"Share LinkedIn\" target=\"_blank\" rel=\"noopener\" href=\"http://www.linkedin.com/shareArticle?mini=true&amp;url=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin&amp;title=K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;summary=&amp;source=mfn.se\"></a>\n    <a class=\"social-ico social-facebook\" title=\"Share Facebook\" target=\"_blank\" rel=\"noopener\" href=\"http://www.facebook.com/sharer/sharer.php?u=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin\"></a>\n</div>\n</div>\n\n")
	headers := map[string]string{
		"Content-Type":         "text/html",
		"x-ragnar-filename":    "test.txt",
		"x-ragnar-mfn-news-id": "eb8bb932-58b0-5aaa-9850-13029c3830d0",
	}
	markdownContent := "# K33 Completes Strategic Purchase of 15 Bitcoin\n\nK33 AB (publ) (\"K33\"), a leading digital asset brokerage and research firm,\nannounces the acquisition of 15 Bitcoin (BTC) for a total consideration of\napproximately SEK 16.0 million.\n\nFollowing today’s transaction, K33 holds a total of 141 BTC on its balance\nsheet, with an average acquisition cost of SEK 1,114,859 per BTC.\n\nK33’s Bitcoin Treasury strategy reflects both the company’s conviction in\nBitcoin’s long-term value proposition and its intention to establish a strong\nposition in the asset to unlock operational alpha in its broker business."
	markdownContentReader := strings.NewReader(markdownContent)
	doc, err := ragnarClient.CreateTubDocumentWithOptionals(context.Background(), tubTestName, mfnPressReleaseContent, markdownContentReader, nil, headers)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Headers["mfn-news-id"] == nil || *doc.Headers["mfn-news-id"] != "eb8bb932-58b0-5aaa-9850-13029c3830d0" {
		t.Fatal("expected mfn-news-id header to be set")
	}
	fmt.Println(">>>created document", doc)
	statusErr := waitUntilStatusCompletedOrTimeout(tubTestName, doc.DocumentId, time.Minute)
	if statusErr != nil {
		t.Fatal("document is not completed")
	}

	markdown, err := ragnarClient.DownloadTubDocumentMarkdown(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	defer markdown.Close()
	mdContent, err := io.ReadAll(markdown)
	if err != nil {
		t.Fatal(err)
	}
	if string(mdContent) != markdownContent {
		t.Fatalf("expected markdown content to be '%s', got '%s'", markdownContent, string(mdContent))
	}

	chunks, err := ragnarClient.GetTubDocumentChunks(context.Background(), tubTestName, doc.DocumentId, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) == 0 {
		t.Fatal("expected chunks to be created")
	}
	fmt.Println(">>>chunks", len(chunks))
	for i, chunk := range chunks {
		fmt.Printf(">>>chunk %d: \n%+v\n\n", i, chunk.Content)
	}
}

func TestGetTubDocumentWithOptionalMarkdownAndChunks(t *testing.T) {
	mfnPressReleaseContent := strings.NewReader("<div class=\"title\">\n    <a href=\"/a/k33/k33-completes-strategic-purchase-of-15-bitcoin\">K33 Completes Strategic Purchase of 15 Bitcoin</a>\n</div>\n<div class=\"publish-date\">\n      2025-09-22 14:40:00\n\n</div>\n\n<div class=\"content s-mfn\">\n\n\n<div class=\"mfn-preamble\"><p><strong>K33 AB (publ) (\"K33\"), a leading digital asset brokerage and research firm, announces the acquisition of 15 Bitcoin (BTC) for a total consideration of approximately SEK 16.0 million.</strong></p></div>\n<div class=\"mfn-body\"><p>Following today’s transaction, K33 holds a total of 141 BTC on its balance sheet, with an average acquisition cost of SEK 1,114,859 per BTC.</p><p>K33’s Bitcoin Treasury strategy reflects both the company’s conviction in Bitcoin’s long-term value proposition and its intention to establish a strong position in the asset to unlock operational alpha in its broker business.</p></div>\n<div class=\"mfn-footer mfn-contacts mfn-88304a0cc28f\"><p><strong class=\"mfn-heading-1\">For further information, please contact:</strong><br>Torbjørn Bull Jenssen, CEO, K33 AB (publ)<br>E-mail: ir@k33.com<br>Web: k33.com/ir</p></div>\n<div class=\"mfn-footer mfn-about mfn-3dfe054bd57f\"><p><strong class=\"mfn-heading-1\">About K33</strong><br>K33 AB (publ), listed on Nasdaq First North Growth Market, is the new gold standard for investments in digital assets. <a href=\"http://k33.com\" rel=\"noopener\" target=\"_blank\">K33</a> offers market-leading execution, actionable insights, and superior support to private and institutional partners across EMEA. Mangold Fondkommission serves as the Certified Adviser for K33 AB (publ).</p></div>\n<div class=\"mfn-footer mfn-attachment mfn-attachment-general\"><p><strong class=\"mfn-heading-1\">Attachments</strong><br><a class=\"mfn-generated mfn-primary\" href=\"https://storage.mfn.se/80ec718e-8584-4d4c-90fe-a02b5e0540c4/k33-completes-strategic-purchase-of-15-bitcoin.pdf\" rel=\"noopener\" target=\"_blank\">K33 Completes Strategic Purchase of 15 Bitcoin</a></p></div>\n\n</div>\n\n\n\n\n<div class=\"footer\">\n     \n     <div class=\"source\">\n        Källa <strong>MFN</strong>\n     </div>\n     \n     \n\n<div class=\"social-tray\">\n    <a class=\"social-ico social-mail\" title=\"Share mail\" rel=\"noopener\" href=\"mailto:?subject=Hej%2C%20jag%20vill%20dela%20denna%20nyhet%20fr%C3%A5n%20mfn.se%20med%20dig%2C%20K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;body=https://mfn.se/a/k33/k33-completes-strategic-purchase-of-15-bitcoin\"></a>\n    <a class=\"social-ico social-twitter\" title=\"Share twitter\" target=\"_blank\" rel=\"noopener\" href=\"https://twitter.com/intent/tweet?url=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin&amp;text=K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;via=MFN_IRnews\"></a>\n    <a class=\"social-ico social-linked-in\" title=\"Share LinkedIn\" target=\"_blank\" rel=\"noopener\" href=\"http://www.linkedin.com/shareArticle?mini=true&amp;url=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin&amp;title=K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;summary=&amp;source=mfn.se\"></a>\n    <a class=\"social-ico social-facebook\" title=\"Share Facebook\" target=\"_blank\" rel=\"noopener\" href=\"http://www.facebook.com/sharer/sharer.php?u=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin\"></a>\n</div>\n</div>\n\n")
	headers := map[string]string{
		"Content-Type":         "text/html",
		"x-ragnar-filename":    "test.txt",
		"x-ragnar-mfn-news-id": "eb8bb932-58b0-5aaa-9850-13029c3830d0",
	}

	markdownContent := "# K33 Completes Strategic Purchase of 15 Bitcoin\n\nK33 AB (publ) (\"K33\"), a leading digital asset brokerage and research firm,\nannounces the acquisition of 15 Bitcoin (BTC) for a total consideration of\napproximately SEK 16.0 million.\n\nFollowing today’s transaction, K33 holds a total of 141 BTC on its balance\nsheet, with an average acquisition cost of SEK 1,114,859 per BTC.\n\nK33’s Bitcoin Treasury strategy reflects both the company’s conviction in\nBitcoin’s long-term value proposition and its intention to establish a strong\nposition in the asset to unlock operational alpha in its broker business."
	markdownContentReader := strings.NewReader(markdownContent)

	chunks := []Chunk{
		{
			ChunkId: 0,
			Content: "K33 AB (publ) (\"K33\"), a leading digital asset brokerage and research firm,\nannounces the acquisition of 15 Bitcoin (BTC) for a total consideration of\napproximately SEK 16.0 million.\nFollowing today’s transaction, K33 holds a total of 141 BTC on its balance\nsheet, with an average acquisition cost of SEK 1,114,859 per BTC.",
		},
		{
			ChunkId: 1,
			Content: "K33’s Bitcoin Treasury strategy reflects both the company’s conviction in\nBitcoin’s long-term value proposition and its intention to establish a strong\nposition in the asset to unlock operational alpha in its broker business.",
		},
	}
	doc, err := ragnarClient.CreateTubDocumentWithOptionals(context.Background(), tubTestName, mfnPressReleaseContent, markdownContentReader, chunks, headers)
	if err != nil {
		t.Fatal(err)
	}
	if doc.Headers["mfn-news-id"] == nil || *doc.Headers["mfn-news-id"] != "eb8bb932-58b0-5aaa-9850-13029c3830d0" {
		t.Fatal("expected mfn-news-id header to be set")
	}
	fmt.Println(">>>created document", doc)
	statusErr := waitUntilStatusCompletedOrTimeout(tubTestName, doc.DocumentId, time.Minute)
	if statusErr != nil {
		t.Fatal("document is not completed")
	}

	markdown, err := ragnarClient.DownloadTubDocumentMarkdown(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	defer markdown.Close()
	mdContent, err := io.ReadAll(markdown)
	if err != nil {
		t.Fatal(err)
	}
	if string(mdContent) != markdownContent {
		t.Fatalf("expected markdown content to be '%s', got '%s'", markdownContent, string(mdContent))
	}
	createdChunks, err := ragnarClient.GetTubDocumentChunks(context.Background(), tubTestName, doc.DocumentId, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(createdChunks) == 0 {
		t.Fatal("expected chunks to be created")
	}
	for i, chunk := range chunks {
		if createdChunks[i].Content != chunk.Content {
			t.Fatalf("expected chunk content to be '%s', got '%s'", chunk.Content, createdChunks[i].Content)
		}
		if createdChunks[i].ChunkId != chunk.ChunkId {
			t.Fatalf("expected chunk ID to be '%d', got '%d'", chunk.ChunkId, createdChunks[i].ChunkId)
		}
	}

	// now update the document with new markdown & chunks
	markdownContentReader = strings.NewReader(markdownContent + "\n\nThis is additional markdown content added in update.")
	updatedChunks := []Chunk{
		{
			ChunkId: 0,
			Content: "Updated chunk 0 content.",
		},
		{
			ChunkId: 1,
			Content: "Updated chunk 1 content.",
		},
		{
			ChunkId: 2,
			Content: "New chunk 2 content.",
		},
	}
	updatedDoc, err := ragnarClient.UpdateTubDocumentWithOptionals(context.Background(), tubTestName, doc.DocumentId, mfnPressReleaseContent, markdownContentReader, updatedChunks, headers)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>updated document", updatedDoc)
	if updatedDoc.DocumentId != doc.DocumentId {
		t.Fatal(fmt.Sprintf("expected document ID to be set. expected: %s, got: %s", doc.DocumentId, updatedDoc.DocumentId))
	}
	statusErr = waitUntilStatusCompletedOrTimeout(tubTestName, doc.DocumentId, time.Minute)
	if statusErr != nil {
		t.Fatal("document is not completed after update")
	}

	markdown, err = ragnarClient.DownloadTubDocumentMarkdown(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	defer markdown.Close()
	mdContent, err = io.ReadAll(markdown)
	if err != nil {
		t.Fatal(err)
	}
	if string(mdContent) != markdownContent+"\n\nThis is additional markdown content added in update." {
		t.Fatalf("expected updated markdown content to be '%s', got '%s'", markdownContent+"\n\nThis is additional markdown content added in update.", string(mdContent))
	}
	createdChunks, err = ragnarClient.GetTubDocumentChunks(context.Background(), tubTestName, doc.DocumentId, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(createdChunks) == 0 {
		t.Fatal("expected chunks to be created")
	}
	for i, chunk := range updatedChunks {
		if createdChunks[i].Content != chunk.Content {
			t.Fatalf("expected updated chunk content to be '%s', got '%s'", chunk.Content, createdChunks[i].Content)
		}
		if createdChunks[i].ChunkId != chunk.ChunkId {
			t.Fatalf("expected updated chunk ID to be '%d', got '%d'", chunk.ChunkId, createdChunks[i].ChunkId)
		}
	}

	// now try and delete a document with markdown file and chunks
	err = ragnarClient.DeleteTubDocument(context.Background(), tubTestName, doc.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>document deleted successfully")

	_, err = ragnarClient.GetTubDocument(context.Background(), tubTestName, doc.DocumentId)
	if err == nil {
		t.Fatal("expected error fetching deleted document")
	}

}

func TestGetTubDocumentWithBadOptionals(t *testing.T) {
	// chunks without markdown should fail
	mfnPressReleaseContent := strings.NewReader("<div class=\"title\">\n    <a href=\"/a/k33/k33-completes-strategic-purchase-of-15-bitcoin\">K33 Completes Strategic Purchase of 15 Bitcoin</a>\n</div>\n<div class=\"publish-date\">\n      2025-09-22 14:40:00\n\n</div>\n\n<div class=\"content s-mfn\">\n\n\n<div class=\"mfn-preamble\"><p><strong>K33 AB (publ) (\"K33\"), a leading digital asset brokerage and research firm, announces the acquisition of 15 Bitcoin (BTC) for a total consideration of approximately SEK 16.0 million.</strong></p></div>\n<div class=\"mfn-body\"><p>Following today’s transaction, K33 holds a total of 141 BTC on its balance sheet, with an average acquisition cost of SEK 1,114,859 per BTC.</p><p>K33’s Bitcoin Treasury strategy reflects both the company’s conviction in Bitcoin’s long-term value proposition and its intention to establish a strong position in the asset to unlock operational alpha in its broker business.</p></div>\n<div class=\"mfn-footer mfn-contacts mfn-88304a0cc28f\"><p><strong class=\"mfn-heading-1\">For further information, please contact:</strong><br>Torbjørn Bull Jenssen, CEO, K33 AB (publ)<br>E-mail: ir@k33.com<br>Web: k33.com/ir</p></div>\n<div class=\"mfn-footer mfn-about mfn-3dfe054bd57f\"><p><strong class=\"mfn-heading-1\">About K33</strong><br>K33 AB (publ), listed on Nasdaq First North Growth Market, is the new gold standard for investments in digital assets. <a href=\"http://k33.com\" rel=\"noopener\" target=\"_blank\">K33</a> offers market-leading execution, actionable insights, and superior support to private and institutional partners across EMEA. Mangold Fondkommission serves as the Certified Adviser for K33 AB (publ).</p></div>\n<div class=\"mfn-footer mfn-attachment mfn-attachment-general\"><p><strong class=\"mfn-heading-1\">Attachments</strong><br><a class=\"mfn-generated mfn-primary\" href=\"https://storage.mfn.se/80ec718e-8584-4d4c-90fe-a02b5e0540c4/k33-completes-strategic-purchase-of-15-bitcoin.pdf\" rel=\"noopener\" target=\"_blank\">K33 Completes Strategic Purchase of 15 Bitcoin</a></p></div>\n\n</div>\n\n\n\n\n<div class=\"footer\">\n     \n     <div class=\"source\">\n        Källa <strong>MFN</strong>\n     </div>\n     \n     \n\n<div class=\"social-tray\">\n    <a class=\"social-ico social-mail\" title=\"Share mail\" rel=\"noopener\" href=\"mailto:?subject=Hej%2C%20jag%20vill%20dela%20denna%20nyhet%20fr%C3%A5n%20mfn.se%20med%20dig%2C%20K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;body=https://mfn.se/a/k33/k33-completes-strategic-purchase-of-15-bitcoin\"></a>\n    <a class=\"social-ico social-twitter\" title=\"Share twitter\" target=\"_blank\" rel=\"noopener\" href=\"https://twitter.com/intent/tweet?url=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin&amp;text=K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;via=MFN_IRnews\"></a>\n    <a class=\"social-ico social-linked-in\" title=\"Share LinkedIn\" target=\"_blank\" rel=\"noopener\" href=\"http://www.linkedin.com/shareArticle?mini=true&amp;url=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin&amp;title=K33%20Completes%20Strategic%20Purchase%20of%2015%20Bitcoin&amp;summary=&amp;source=mfn.se\"></a>\n    <a class=\"social-ico social-facebook\" title=\"Share Facebook\" target=\"_blank\" rel=\"noopener\" href=\"http://www.facebook.com/sharer/sharer.php?u=https:%2F%2Fmfn.se%2Fa%2Fk33%2Fk33-completes-strategic-purchase-of-15-bitcoin\"></a>\n</div>\n</div>\n\n")
	headers := map[string]string{
		"Content-Type":         "text/html",
		"x-ragnar-filename":    "test.txt",
		"x-ragnar-mfn-news-id": "eb8bb932-58b0-5aaa-9850-13029c3830d0",
	}
	_, err := ragnarClient.CreateTubDocumentWithOptionals(context.Background(), tubTestName, mfnPressReleaseContent, nil, []Chunk{{
		ChunkId: 0,
		Context: "",
		Content: "123321",
	}}, headers)
	if err == nil {
		t.Fatal(err)
	}
	if !strings.Contains(err.Error(), "HTTP 400: chunks provided but markdown part is missing") {
		t.Fatal("expected error about missing markdown", err)
	}
}

func TestSearchTubDocumentChunks(t *testing.T) {
	chunks, err := ragnarClient.SearchTubDocumentChunks(context.Background(), tubTestName, "planeras till onsdagen den 24 september 2025", nil, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatal("expected chunks to be found")
	}
	// with doc filter
	chunks, err = ragnarClient.SearchTubDocumentChunks(context.Background(), tubTestName, "planeras till onsdagen den 24 september 2025", map[string]any{"mfn-news-id": "eb8bb932-58b0-5aaa-9850-13029c3830d0"}, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatal("expected chunks to be found")
	}
	// with doc slice filter
	chunks, err = ragnarClient.SearchTubDocumentChunks(context.Background(), tubTestName, "planeras till onsdagen den 24 september 2025", map[string]any{"mfn-news-id": []string{"eb8bb932-58b0-5aaa-9850-13029c3830d0"}}, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 3 {
		t.Fatal("expected chunks to be found")
	}
	for i, chunk := range chunks {
		fmt.Printf(">>>chunk %d: \n%+v\n\n", i, chunk.Content)
	}
	// with "empty" slice filter
	chunks, err = ragnarClient.SearchTubDocumentChunks(context.Background(), tubTestName, "planeras till onsdagen den 24 september 2025", map[string]any{"mfn-news-id": []string{"does-not-exist"}}, 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunks) != 0 {
		t.Fatal("expected no chunks to be found")
	}
}

func TestDownloadMarkdownDocument(t *testing.T) {
	docs, err := ragnarClient.GetTubDocuments(context.Background(), tubTestName, nil, 10, 0)
	if err != nil {
		t.Fatal("error fetching documents", err)
	}
	markdown, err := ragnarClient.DownloadTubDocumentMarkdown(context.Background(), tubTestName, docs[0].DocumentId)
	if err != nil {
		t.Fatal("error downloading markdown", err)
	}
	defer markdown.Close()
	content, err := io.ReadAll(markdown)
	if err != nil {
		t.Fatal("could not read markdown body", err)
	}
	fmt.Printf(">>>document content\n'%s'", string(content))
}

func TestDeleteTub(t *testing.T) {
	result, err := ragnarClient.DeleteTub(context.Background(), tubTestName)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(">>>deleted tub", result)
}

func TestCreateTubAndUpdateWithInvalidRequiredHeaders(t *testing.T) {
	ctx := context.Background()
	requiredHeader := "test-required-header"
	tubName := "tub-with-no-required-headers-but-will-be-updated"
	_, _ = ragnarClient.DeleteTub(ctx, tubName)
	tub, err := ragnarClient.CreateTub(ctx, Tub{TubName: tubName})
	if err != nil {
		t.Fatal(err)
	}
	content := strings.NewReader("This is test document without the header")
	headers := map[string]string{
		"Content-Type":      "text/plain",
		"x-ragnar-filename": "test.txt",
	}
	markdownContent := "# Test header\n\nThis is test document without the header"
	markdownContentReader := strings.NewReader(markdownContent)
	chunks := []Chunk{{ChunkId: 0, Content: "Test test"}}
	docToUpdateLater, err := ragnarClient.CreateTubDocumentWithOptionals(ctx, tubName, content, markdownContentReader, chunks, headers)
	if err != nil {
		t.Fatal(err)
	}
	err = waitUntilStatusCompletedOrTimeout(tubName, docToUpdateLater.DocumentId, time.Minute)
	if err != nil {
		t.Fatal("document is not completed")
	}
	updatedTub := tub.WithRequiredDocumentHeaders(requiredHeader)
	_, err = ragnarClient.UpdateTub(ctx, updatedTub)
	if err == nil {
		t.Fatal("expected error updating tub to require header that existing documents do not have")
	}
	markdownContent2 := "# Test header\n\nThis is test document without the header but with required header"
	markdownContentReader2 := strings.NewReader(markdownContent2)
	chunks2 := []Chunk{{ChunkId: 0, Content: "Test test"}, {ChunkId: 1, Content: "Test test 2"}}
	docToUpdateLater, err = ragnarClient.UpdateTubDocumentWithOptionals(ctx, tubName, docToUpdateLater.DocumentId, content, markdownContentReader2, chunks2, map[string]string{
		"Content-Type":      "text/plain",
		"x-ragnar-filename": "test.txt",
		fmt.Sprintf("x-ragnar-%s", requiredHeader): "test-value",
	})
	if err != nil {
		t.Fatal(err)
	}
	if docToUpdateLater.Headers[requiredHeader] == nil || *docToUpdateLater.Headers[requiredHeader] != "test-value" {
		t.Fatal("expected required header to be set on updated document")
	}
	updatedMarkdown, err := ragnarClient.DownloadTubDocumentMarkdown(ctx, tubName, docToUpdateLater.DocumentId)
	if err != nil {
		t.Fatal(err)
	}
	updatedMarkdownContent, err := io.ReadAll(updatedMarkdown)
	if err != nil {
		t.Fatal(err)
	}
	if string(updatedMarkdownContent) != markdownContent2 {
		t.Fatalf("expected updated markdown content to be '%s', got '%s'", markdownContent2, string(updatedMarkdownContent))
	}
	updatedChunks, err := ragnarClient.GetTubDocumentChunks(ctx, tubName, docToUpdateLater.DocumentId, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(updatedChunks) != 2 {
		t.Fatalf("expected 2 updated chunks, got %d", len(updatedChunks))
	}
	if updatedChunks[0].Content != chunks2[0].Content {
		t.Fatalf("expected updated chunk 0 content to be '%s', got '%s'", chunks2[0].Content, updatedChunks[0].Content)
	}
	if updatedChunks[1].Content != chunks2[1].Content {
		t.Fatalf("expected updated chunk 1 content to be '%s', got '%s'", chunks2[1].Content, updatedChunks[1].Content)
	}
	// added header -> should be able to update tub now
	_, err = ragnarClient.UpdateTub(ctx, updatedTub)
	if err != nil {
		t.Fatal(err)
	}
	// try creating document without required header -> should fail
	_, err = ragnarClient.CreateTubDocument(context.Background(), tubName, content, headers)
	if err == nil {
		t.Fatal("expected error creating document without required header")
	}
	if err.Error() != "HTTP 400: 400 Bad Request" {
		t.Fatal("expected 400 Bad Request error creating document without required header, got", err)
	}
	_, err = ragnarClient.DeleteTub(ctx, tubName) // cleanup
	if err != nil {
		t.Fatal(err)
	}
}

var ragnarUnauthorizedClient = NewClient(ClientConfig{
	BaseURL:   "http://localhost:7100",
	AccessKey: "rag_cdd77e0b-9931-45c5-914d-ebe1c15c1914",
})

const unauthorizedError = "HTTP 401: access key is not allowed"
const rawUnauthorizedError = "HTTP 401: 401 Unauthorized"

func TestUnauthorizedAccess(t *testing.T) {
	var err error
	_, err = ragnarUnauthorizedClient.GetTub(context.Background(), tubTestName)
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 fetching tub", err)
	}
	_, err = ragnarUnauthorizedClient.GetTubs(context.Background())
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 fetching tubs", err)
	}
	_, err = ragnarUnauthorizedClient.CreateTub(context.Background(), Tub{TubName: tubTestName})
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 creating tub", err)
	}
	_, err = ragnarUnauthorizedClient.UpdateTub(context.Background(), Tub{TubName: tubTestName})
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 updating tub", err)
	}
	_, err = ragnarUnauthorizedClient.DeleteTub(context.Background(), tubTestName)
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 deleting tub", err)
	}
	_, err = ragnarUnauthorizedClient.GetTubDocuments(context.Background(), tubTestName, nil, 10, 0)
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 fetching tub documents", err)
	}
	_, err = ragnarUnauthorizedClient.GetTubDocument(context.Background(), tubTestName, "doc1")
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 fetching tub document", err)
	}
	_, err = ragnarUnauthorizedClient.CreateTubDocument(context.Background(), tubTestName, strings.NewReader("test"), map[string]string{})
	if err == nil || !strings.Contains(err.Error(), rawUnauthorizedError) {
		t.Fatal("expected 401 creating tub document", err)
	}
	_, err = ragnarUnauthorizedClient.UpdateTubDocument(context.Background(), tubTestName, "doc1", strings.NewReader("test"), map[string]string{})
	if err == nil || !strings.Contains(err.Error(), rawUnauthorizedError) {
		t.Fatal("expected 401 updating tub document", err)
	}
	_, err = ragnarUnauthorizedClient.DownloadTubDocument(context.Background(), tubTestName, "doc1")
	if err == nil || !strings.Contains(err.Error(), rawUnauthorizedError) {
		t.Fatal("expected 401 downloading tub document", err)
	}
	err = ragnarUnauthorizedClient.DeleteTubDocument(context.Background(), tubTestName, "doc1")
	if err == nil || !strings.Contains(err.Error(), rawUnauthorizedError) {
		t.Fatal("expected 401 deleting tub document", err)
	}
	_, err = ragnarUnauthorizedClient.GetTubDocumentChunks(context.Background(), tubTestName, "doc1", 10, 0)
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 fetching tub document chunks", err)
	}
	_, err = ragnarUnauthorizedClient.GetTubDocumentChunk(context.Background(), tubTestName, "doc1", 0)
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 fetching tub document chunk", err)
	}
	_, err = ragnarUnauthorizedClient.SearchTubDocumentChunks(context.Background(), tubTestName, "test", nil, 10, 0)
	if err == nil || !strings.Contains(err.Error(), unauthorizedError) {
		t.Fatal("expected 401 searching tub document chunks", err)
	}
}

func waitUntilStatusCompletedOrTimeout(tubName, documentId string, timeout time.Duration) error {
	start := time.Now()
	for {
		doc, err := ragnarClient.GetTubDocumentStatus(context.Background(), tubName, documentId)
		if err != nil {
			return fmt.Errorf("error getting document status: %w", err)
		}
		if doc.Status == "completed" {
			fmt.Println(">>>document completed waited:", time.Since(start).Milliseconds(), "ms")
			return nil
		}
		fmt.Println(">>>document status:", doc.Status, " waited:", time.Since(start).Milliseconds(), "ms")
		if time.Since(start) > timeout {
			return fmt.Errorf("timeout waiting for document to be completed")
		}
		time.Sleep(5 * time.Second)
	}
}
