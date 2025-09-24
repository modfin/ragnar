package chunker

import (
	"fmt"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/modfin/ragnar/internal/util"
	"reflect"
	"testing"
)

func TestSplitMarkdownText(t *testing.T) {
	type args struct {
		text string
		ops  pgtype.Hstore
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "simple markdown",
			args: args{
				text: "# Annos me aufert postquam\n\n## Incipiat tempore oblita violentia\n\nLorem markdownum rami hic haeserat te ensem! Non ars, acerbo unda mater fidumque\ncuspide altera sunt coniunx. Ubi herbis Talibus et flemus `wrapEup` quas\niuvenum.\n\n- Verticis scelerata vulnere tempus petendum potest vestes\n- Stridore Cerealia socias\n- Et fuerat virtute sanguine propiore\n- Me tu spes quare pectus\n- Vacuus omnia volventia silet et redeunt campos\n- Acies oravere\n\n## Placidi florente obsita qui\n\nFortibus illa novas; peregrina veretur `internic` pectus hactenus ferat brevem\net prima: commentaque vanos vident dictaque si sonus? Quisque Triptolemo\n**ambobus subtraheret** fidem agresti! Cum vix socialia, lapsis dumque manu\ncausa **sacrasque** deorum dextra [fuerant](#incipiat-tempore-oblita-violentia),\nnon et natura. Dubitat moto tenet, secum o animus et cum in Italicis fefellerat\npenetrabile castos Peleus et tamen; peccavimus sub.\n\n- Freta inpulsu\n- Est valle notam deponere\n- Iove cacumine vocibus\n- Adsum nec armenta sibi regna et manes\n- Stringit ortu\n- Fides animo rapuere haut dum\n\n## Sub amatas sine trux\n\nRecens variarum in atque culpavit Echecli terrae! Ora ille reccidimus Agenorides\nnitebant dixit pro theatris `bookmarkPpl` iubent, est nubes illa lenire? Quo si\nvidit.\n\n> Pigneror `drop_trim_registry` conclamat matutinis victoria **volucresque**\n> pudor; nostrum eruerit teneras simul se est, ille altera ausus orbem. Usus\n> dedit si chlamydem precando ponere sui verba exsereret navigat. Tellure dares,\n> hic et remisit mihi totidem pallada canibus ubi morer hospes, dei mirata\n> [pellis](#incipiat-tempore-oblita-violentia). Hydros *ebrius*.\n\n## Genitor petunt inperfossus prodit\n\nO *summoque accipe*, cui [non](#placidi-florente-obsita-qui) egerere ictu holus\nnutu concita iugulaberis. Urbem inrita umbras iam malus iaculum sua undique\nCorythi. Suarum miluus, age stant, quas **cognoscendo inania Acheloiadumque**\npostquam blandita.\n\nLiquit se et ut [huic](#incipiat-tempore-oblita-violentia) nec sanior nubila\n*seductaque et*. Recisum velamina rursus. Esse seu valentior, hospitis adhuc\nperlucida: et vultu Helopsque caeloque.\n",
				ops:  nil,
			},
			want: []string{
				"# Annos me aufert postquam",
				"# Annos me aufert postquam\n## Incipiat tempore oblita violentia\nLorem markdownum rami hic haeserat te ensem! Non ars, acerbo unda mater fidumque\ncuspide altera sunt coniunx. Ubi herbis Talibus et flemus `wrapEup` quas\niuvenum.\n- Verticis scelerata vulnere tempus petendum potest vestes\n- Stridore Cerealia socias\n- Et fuerat virtute sanguine propiore\n- Me tu spes quare pectus\n- Vacuus omnia volventia silet et redeunt campos\n- Acies oravere",
				"# Annos me aufert postquam\n## Placidi florente obsita qui\nFortibus illa novas; peregrina veretur `internic` pectus hactenus ferat brevem\net prima: commentaque vanos vident dictaque si sonus? Quisque Triptolemo\n**ambobus subtraheret** fidem agresti! Cum vix socialia, lapsis dumque manu\ncausa **sacrasque** deorum dextra [fuerant](#incipiat-tempore-oblita-violentia),\nnon et natura. Dubitat moto tenet, secum o animus et cum in Italicis fefellerat\npenetrabile castos Peleus et tamen; peccavimus sub.\n- Freta inpulsu\n- Est valle notam deponere\n- Iove cacumine vocibus",
				"# Annos me aufert postquam\n## Placidi florente obsita qui\n- Adsum nec armenta sibi regna et manes\n- Stringit ortu\n- Fides animo rapuere haut dum",
				"# Annos me aufert postquam\n## Sub amatas sine trux\nRecens variarum in atque culpavit Echecli terrae! Ora ille reccidimus Agenorides\nnitebant dixit pro theatris `bookmarkPpl` iubent, est nubes illa lenire? Quo si\nvidit.",
				"# Annos me aufert postquam\n## Sub amatas sine trux\n> Pigneror `drop_trim_registry` conclamat matutinis victoria **volucresque**\n> pudor; nostrum eruerit teneras simul se est, ille altera ausus orbem. Usus\n> dedit si chlamydem precando ponere sui verba exsereret navigat. Tellure dares,\n> hic et remisit mihi totidem pallada canibus ubi morer hospes, dei mirata\n> [pellis](#incipiat-tempore-oblita-violentia). Hydros *ebrius*.",
				"# Annos me aufert postquam\n## Genitor petunt inperfossus prodit\nO *summoque accipe*, cui [non](#placidi-florente-obsita-qui) egerere ictu holus\nnutu concita iugulaberis. Urbem inrita umbras iam malus iaculum sua undique\nCorythi. Suarum miluus, age stant, quas **cognoscendo inania Acheloiadumque**\npostquam blandita.\nLiquit se et ut [huic](#incipiat-tempore-oblita-violentia) nec sanior nubila\n*seductaque et*. Recisum velamina rursus. Esse seu valentior, hospitis adhuc\nperlucida: et vultu Helopsque caeloque.",
			},
		},
		{
			name: "mfn example markdown",
			args: args{
				text: "::: title\n[Spotlight Group: Invoicery Group godkänt för listning på Spotlight\nValue](/cis/a/spotlight-group/spotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932)\n:::\n\n::: publish-date\n2025-09-18 14:00:00\n:::\n\n:::::: {.content .s-cis}\n::: mfn-preamble\n\nSpotlight Group meddelar att dotterbolaget Spotlight Stock Market har\ngodkänt Invoicery Group för listning på Spotlight Value. Bolaget, som i\nSverige är mer känt under ett av sina svenska varumärken Frilans Finans,\nblir därmed det sjunde bolaget på listan och det första nya sedan\nSpotlight Value lanserades den 17 juni 2025. Första dag för handel i\nInvoicery Group på Spotlight Value planeras till onsdagen den 24\nseptember 2025.\n:::\n\n\\\"Det är alltid roligt att andra observerar och uppskattar det arbete\nsom vi gör i vår verksamhet, särskilt i en tid då uppdragsbaserat arbete\nfår allt större uppmärksamhet på arbetsmarknaden\\\", säger Invoicerys VD\nStephen Schad.\n\nKraven för att listas på Spotlight Value är att bolaget ska ha visat\nvinst på sista raden de tre senaste åren, ha haft positiv tillväxt under\nminst två av de tre senaste åren och ha gett utdelning till sina\naktieägaren minst två av de tre senaste åren. Invoicery lever därmed upp\ntill alla dessa krav.\n\n\\\"Spotlight Value har tagits emot med stort intresse från såväl\ninvesterare som bolag och vi är väldigt glada över att kunna välkomna\nInvoicery som nytt Spotlight Value-bolag. Med dem blir listan ännu\nstarkare och ännu mer attraktiv för investerarna. Vi hoppas att de får\nsällskap snart av fler stabila bolag som i dag har andra listningar\neller kanske inte är noterade alls\\\", säger Spotlight Stock Markets VD\nPeter Gönczi.\n\nFör att kvalificera sig för Spotlight Value måste ett bolag ha haft\nvinst på sista raden de tre senaste åren, visat tillväxt minst två av de\ntre senaste tre åren och dessutom ha gett aktieutdelning minst två av de\ntre senaste åren. Med avdrag för engångskostnader relaterade till\nnoteringen lever Invoicery upp till alla dessa krav. Sedan tidigare\nfinns sex bolag på Spotlight Value: Homemaid, Veteranpoolen, Gosol\nEnergy Group, Transferator, Aquaticus Real Estate och Logistri\nFastighets AB.\n\n<div>\n\n</div>\n\n::: mfn-footer\n**För ytterligare information om Spotlight Group, vänligen kontakta:**\\\nPeter Gönczi, VD\\\nE-post: ir@spotlightgroup.se\\\nHemsida: www.spotlightgroup.se\n:::\n::::::\n\n::::: footer\n::: source\nKälla **Cision**\n:::\n\n::: social-tray\n[](mailto:?subject=Hej%2C%20jag%20vill%20dela%20denna%20nyhet%20fr%C3%A5n%20mfn.se%20med%20dig%2C%20Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&body=https://mfn.se/cis/a/spotlight-group/spotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932 \"Share mail\"){.social-ico\n.social-mail rel=\"noopener\"}\n[](https://twitter.com/intent/tweet?url=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932&text=Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&via=MFN_IRnews \"Share twitter\"){.social-ico\n.social-twitter target=\"_blank\" rel=\"noopener\"}\n[](http://www.linkedin.com/shareArticle?mini=true&url=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932&title=Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&summary=&source=mfn.se \"Share LinkedIn\"){.social-ico\n.social-linked-in target=\"_blank\" rel=\"noopener\"}\n[](http://www.facebook.com/sharer/sharer.php?u=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932 \"Share Facebook\"){.social-ico\n.social-facebook target=\"_blank\" rel=\"noopener\"}\n:::\n:::::",
				ops: pgtype.Hstore{
					"chunk_splitter": util.Ptr("markdown"),
					"chunk_size":     util.Ptr("1024"),
				},
			},
			want: []string{
				"::: title\n[Spotlight Group: Invoicery Group godkänt för listning på Spotlight\nValue](/cis/a/spotlight-group/spotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932)\n:::\n::: publish-date\n2025-09-18 14:00:00\n:::\n:::::: {.content .s-cis}\n::: mfn-preamble\nSpotlight Group meddelar att dotterbolaget Spotlight Stock Market har\ngodkänt Invoicery Group för listning på Spotlight Value. Bolaget, som i\nSverige är mer känt under ett av sina svenska varumärken Frilans Finans,\nblir därmed det sjunde bolaget på listan och det första nya sedan\nSpotlight Value lanserades den 17 juni 2025. Första dag för handel i\nInvoicery Group på Spotlight Value planeras till onsdagen den 24\nseptember 2025.\n:::\n\\\"Det är alltid roligt att andra observerar och uppskattar det arbete\nsom vi gör i vår verksamhet, särskilt i en tid då uppdragsbaserat arbete\nfår allt större uppmärksamhet på arbetsmarknaden\\\", säger Invoicerys VD\nStephen Schad.",
				"Kraven för att listas på Spotlight Value är att bolaget ska ha visat\nvinst på sista raden de tre senaste åren, ha haft positiv tillväxt under\nminst två av de tre senaste åren och ha gett utdelning till sina\naktieägaren minst två av de tre senaste åren. Invoicery lever därmed upp\ntill alla dessa krav.\n\\\"Spotlight Value har tagits emot med stort intresse från såväl\ninvesterare som bolag och vi är väldigt glada över att kunna välkomna\nInvoicery som nytt Spotlight Value-bolag. Med dem blir listan ännu\nstarkare och ännu mer attraktiv för investerarna. Vi hoppas att de får\nsällskap snart av fler stabila bolag som i dag har andra listningar\neller kanske inte är noterade alls\\\", säger Spotlight Stock Markets VD\nPeter Gönczi.",
				"För att kvalificera sig för Spotlight Value måste ett bolag ha haft\nvinst på sista raden de tre senaste åren, visat tillväxt minst två av de\ntre senaste tre åren och dessutom ha gett aktieutdelning minst två av de\ntre senaste åren. Med avdrag för engångskostnader relaterade till\nnoteringen lever Invoicery upp till alla dessa krav. Sedan tidigare\nfinns sex bolag på Spotlight Value: Homemaid, Veteranpoolen, Gosol\nEnergy Group, Transferator, Aquaticus Real Estate och Logistri\nFastighets AB.\n<div>\n</div>\n::: mfn-footer\n**För ytterligare information om Spotlight Group, vänligen kontakta:**\\\nPeter Gönczi, VD\\\nE-post: ir@spotlightgroup.se\\\nHemsida: www.spotlightgroup.se\n:::\n::::::\n::::: footer\n::: source\nKälla **Cision**\n:::",
				"::: social-tray\n[](mailto:?subject=Hej%2C%20jag%20vill%20dela%20denna%20nyhet%20fr%C3%A5n%20mfn.se%20med%20dig%2C%20Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&body=https://mfn.se/cis/a/spotlight-group/spotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932 \"Share mail\"){.social-ico\n.social-mail rel=\"noopener\"}\n[](https://twitter.com/intent/tweet?url=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932&text=Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&via=MFN_IRnews \"Share twitter\"){.social-ico\n.social-twitter target=\"_blank\" rel=\"noopener\"}",
				"[](http://www.linkedin.com/shareArticle?mini=true&url=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932&title=Spotlight%20Group:%20Invoicery%20Group%20godk%C3%A4nt%20f%C3%B6r%20listning%20p%C3%A5%20Spotlight%20Value&summary=&source=mfn.se \"Share LinkedIn\"){.social-ico\n.social-linked-in target=\"_blank\" rel=\"noopener\"}\n[](http://www.facebook.com/sharer/sharer.php?u=https:%2F%2Fmfn.se%2Fcis%2Fa%2Fspotlight-group%2Fspotlight-group-invoicery-group-godkant-for-listning-pa-spotlight-value-eb8bb932 \"Share Facebook\"){.social-ico\n.social-facebook target=\"_blank\" rel=\"noopener\"}\n:::\n:::::",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//got, err := SplitMarkdownText(tt.args.text, tt.args.ops...)
			got, err := GetTextSplitterFromTubSettings(tt.args.ops).SplitText(tt.args.text)
			if (err != nil) != tt.wantErr {
				t.Errorf("SplitMarkdownText() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				fmt.Printf("number of chunks: got %d, want %d\n", len(got), len(tt.want))
				for i, g := range got {
					fmt.Printf("Chunk %d: \n\"%s\"\n", i, g)
				}
				t.Errorf("SplitMarkdownText() got = %v, want %v", got, tt.want)
			}
		})
	}
}
