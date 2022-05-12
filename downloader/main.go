package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/antchfx/xmlquery"
	"github.com/cenkalti/backoff/v4"
	"github.com/davecgh/go-spew/spew"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/samber/lo"
	"go.uber.org/ratelimit"
)

var (
	secRL = ratelimit.New(9)

	indexURL = "https://www.sec.gov/Archives/edgar/daily-index/%d/QTR%d/"
)

type DailyFilingsRow struct {
	CIK             string
	CompanyName     string
	FormType        string
	DateFiled       string
	FileName        string
	AccessionNumber string
}

func main() {
	// Get the master files
	filings, err := GetFilingsForYearQuarter(2022, 2)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Fetched %d filings", len(filings))
	spew.Dump("Filtering down filings")

	filings = lo.Filter(filings, func(v *DailyFilingsRow, i int) bool {
		return v.FormType == "4" || v.FormType == "4/A"
	})
	log.Printf("Filtered down to %d 4 and 4/A filings", len(filings))

	csvData := [][]string{
		{"ISSUER_CIK", "REPORTER_CIK", "ACCESSION_NUMBER", "NAME_OF_REPORTING_PERSON", "A_OR_D", "AMOUNT", "PRICE", "TRANSACTION_DATE", "TITLE_OF_SECURITY", "ISSUER_NAME", "ISSUER_TICKER", "IS_DIRECTOR", "IS_OFFICER", "IS_TEN_PERCENT_OWNER", "IS_OTHER_RELATIONSHIP", "NEW_AMOUNT_OWNED", "DIRECT_OR_INDIRECT_OWNERSHIP"},
	}
	csvData = csvData

	for i, filing := range filings {
		// Check if exists
		log.Printf("Downlaoding %+v", filing)
		var content []byte
		var err error
		filePath := "form4_xml/" + fmt.Sprintf("%s_%s.xml", filing.CIK, filing.AccessionNumber)
		if _, err = os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
			// path/to/whatever does not exist
			content, err = DownloadSECFile("https://www.sec.gov/Archives/" + filing.FileName)
			if err != nil {
				log.Printf("Error downloading file %s", filePath)
				log.Fatal(err)
			}

			// Write file to disk
			err = ioutil.WriteFile(filePath, content, 0777)
			if err != nil {
				log.Println("Failed to write file to disk", filePath)
				log.Fatal(err)
			}
		} else {
			// Read from disk
			content, err = ioutil.ReadFile(filePath)
			if err != nil {
				log.Println("Error reading file on disk", filePath)
				log.Fatal(err)
			}
		}

		// Extract the XML portion, some files use form4.xml while others use primarydocument.xml
		// https://www.sec.gov/Archives/edgar/data/0001184237/000156218022003904/xslF345X03/primarydocument.xml
		// https://www.sec.gov/Archives/edgar/data/1000623/000106299322009210/xslF345X03/form4.xml
		parts := strings.Split(string(content), "<XML>")
		if len(parts) != 2 {
			log.Printf("Skipping %s, invalid parts 1", filePath)
			continue
		}
		parts = strings.Split(parts[1], "</XML>")
		if len(parts) != 2 {
			log.Printf("Skipping %s, invalid parts 2", filePath)
			continue
		}

		content = []byte(parts[0])

		doc, err := xmlquery.Parse(bytes.NewReader(content))
		if err != nil {
			log.Println("Failed to parse file", filePath)
			log.Fatal(err)
		}

		issuerCIK, err := xmlquery.Query(doc, "//ownershipDocument/issuer/issuerCik")
		if err != nil {
			log.Println("Error getting issuer cik")
			log.Println(err)
			continue
		} else if issuerCIK == nil {
			log.Println("Issuer CIK was nil for", filePath)
			continue
		}

		reportingPersonCIK, err := xmlquery.Query(doc, "//ownershipDocument/reportingOwner/rptOwnerCik")
		if err != nil {
			log.Println("Error getting reporting cik")
			log.Println(err)
			continue
		} else if reportingPersonCIK == nil {
			log.Println("Reporting CIK was nil for", filePath)
			continue
		}

		reportingPersonName, err := xmlquery.Query(doc, "//ownershipDocument/reportingOwner/rptOwnerName")
		if err != nil {
			log.Println("Error getting reportingPerson name")
			log.Println(err)
			continue
		} else if reportingPersonName == nil {
			log.Println("reportingPerson Name was nil for", filePath)
			continue
		}

		aOrD, err := xmlquery.Query(doc, "//ownershipDocument/derivativeTable/derivativeTransaction/transactionAmounts/transactionAcquiredDisposedCode/value")
		if err != nil {
			log.Println("Error getting a or d")
			log.Println(err)
			continue
		} else if aOrD == nil {
			log.Println("a or d was nil for", filePath)
			continue
		}

		amount, err := xmlquery.Query(doc, "//ownershipDocument/derivativeTable/derivativeTransaction/transactionAmounts/transactionShares/value")
		if err != nil {
			log.Println("Error getting amount")
			log.Println(err)
			continue
		} else if amount == nil {
			log.Println("amount was nil for", filePath)
			continue
		}

		price, err := xmlquery.Query(doc, "//ownershipDocument/derivativeTable/derivativeTransaction/transactionAmounts/transactionPricePerShare/value")
		if err != nil {
			log.Println("Error getting price")
			log.Println(err)
			continue
		} else if price == nil {
			log.Println("price was nil for", filePath)
			continue
		}

		transactionDate, err := xmlquery.Query(doc, "//ownershipDocument/derivativeTable/derivativeTransaction/transactionDate/value")
		if err != nil {
			log.Println("Error getting transaction date")
			log.Println(err)
			continue
		} else if transactionDate == nil {
			log.Println("transaction date was nil for", filePath)
			continue
		}

		titleOfSecurity, err := xmlquery.Query(doc, "//ownershipDocument/derivativeTable/derivativeTransaction/securityTitle/value")
		if err != nil {
			log.Println("Error getting security title")
			log.Println(err)
			continue
		} else if titleOfSecurity == nil {
			log.Println("security title was nil for", filePath)
			continue
		}

		issuerName, err := xmlquery.Query(doc, "//ownershipDocument/issuer/issuerName")
		if err != nil {
			log.Println("Error getting issuer name")
			log.Println(err)
			continue
		} else if issuerName == nil {
			log.Println("Issuer Name was nil for", filePath)
			continue
		}

		issuerTicker, err := xmlquery.Query(doc, "//ownershipDocument/issuer/issuerTradingSymbol")
		if err != nil {
			log.Println("Error getting issuer ticker")
			log.Println(err)
			continue
		} else if issuerTicker == nil {
			log.Println("issuer ticker was nil for", filePath)
			continue
		}

		isDirector, err := xmlquery.Query(doc, "//ownershipDocument/reportingOwner/reportingOwnerRelationship/isDirector")
		if err != nil {
			log.Println("Error getting isDirector")
			log.Println(err)
			continue
		} else if isDirector == nil {
			log.Println("isDirector was nil for", filePath)
			continue
		}

		isOfficer, err := xmlquery.Query(doc, "//ownershipDocument/reportingOwner/reportingOwnerRelationship/isOfficer")
		if err != nil {
			log.Println("Error getting isOfficer")
			log.Println(err)
			continue
		} else if isOfficer == nil {
			log.Println("isOfficer was nil for", filePath)
			continue
		}

		isTenPercentOwner, err := xmlquery.Query(doc, "//ownershipDocument/reportingOwner/reportingOwnerRelationship/isTenPercentOwner")
		if err != nil {
			log.Println("Error getting isTenPercentOwner")
			log.Println(err)
			continue
		} else if isTenPercentOwner == nil {
			log.Println("isTenPercentOwner was nil for", filePath)
			continue
		}

		isOther, err := xmlquery.Query(doc, "//ownershipDocument/reportingOwner/reportingOwnerRelationship/isOther")
		if err != nil {
			log.Println("Error getting isOther")
			log.Println(err)
			continue
		} else if isOther == nil {
			log.Println("isOther was nil for", filePath)
			continue
		}

		newAmountOwned, err := xmlquery.Query(doc, "//ownershipDocument/derivativeTable/derivativeTransaction/postTransactionAmounts/sharesOwnedFollowingTransaction/value")
		if err != nil {
			log.Println("Error getting price")
			log.Println(err)
			continue
		} else if newAmountOwned == nil {
			log.Println("price was nil for", filePath)
			continue
		}

		directOrIndirectOwnership, err := xmlquery.Query(doc, "//ownershipDocument/derivativeTable/derivativeTransaction/ownershipNature/directOrIndirectOwnership/value")
		if err != nil {
			log.Println("Error getting directOrIndirectOwnership")
			log.Println(err)
			continue
		} else if directOrIndirectOwnership == nil {
			log.Println("ownershipForm was nil for", filePath)
			continue
		}

		csvData = append(csvData, []string{issuerCIK.InnerText(), reportingPersonCIK.InnerText(), filing.AccessionNumber, reportingPersonName.InnerText(), aOrD.InnerText(), amount.InnerText(), price.InnerText(), transactionDate.InnerText(), titleOfSecurity.InnerText(), issuerName.InnerText(), issuerTicker.InnerText(), isDirector.InnerText(), isOfficer.InnerText(), isTenPercentOwner.InnerText(), isOther.InnerText(), newAmountOwned.InnerText(), directOrIndirectOwnership.InnerText()})

		// log.Println("Got issuer", issuerName.InnerText())
		log.Printf("Parsed %d/%d", i, len(filings))
	}

	log.Println("Done")
}

func DownloadSECFile(url string) ([]byte, error) {

	s := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("Error creating request")
		return nil, err
	}

	req.Header.Add("accept-language", "en-US,en;q=0.9")
	req.Header.Add("accept-encoding", "gzip,deflate")
	req.Header.Add("User-Agent", fmt.Sprintf("Sample Company Name %s@sampledomain.com", gonanoid.Must()))

	var resp *http.Response
	err = backoff.RetryNotify(func() error {
		secRL.Take()
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			log.Printf("Error making request")
			return err
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Millisecond*100), 5), func(err error, d time.Duration) {
		log.Printf("Failed to make request after %s: %s", d, err.Error())
	})

	if resp.StatusCode == 404 {
		log.Printf("File not found %s", url)
		return nil, fmt.Errorf("ErrNotFound")
	} else if resp.StatusCode == 429 {
		log.Printf("Getting rate limited at url %s", url)
		return nil, fmt.Errorf("ErrRateLimited")
	} else if resp.StatusCode == 403 {
		// Does not exist
		return nil, fmt.Errorf("ErrDoesNotExist")
	} else if resp.StatusCode > 299 {
		log.Printf("Got status code %d for url %s", resp.StatusCode, url)
		return nil, fmt.Errorf("ErrHighStatusCode")
	}

	if err != nil {
		log.Printf("Error getting %s", url)
		return nil, err
	}

	gReader, err := gzip.NewReader(resp.Body)
	if err != nil {
		log.Printf("error creating new reader")
		return nil, err
	}

	content, err := ioutil.ReadAll(gReader)
	if err != nil {
		log.Printf("Error reading file content")
		return nil, err
	}

	log.Printf("Downloaded SEC file %s in %s", url, time.Since(s))
	return content, nil
}

func parseDailyMasterFile(fileContent []byte) []*DailyFilingsRow {
	s := string(fileContent)
	rows := strings.Split(s, "\n")
	// Get rid of first 7 lines
	rows = rows[7:]

	resp := []*DailyFilingsRow{}

	for _, row := range rows {
		if row == "" {
			continue
		}
		parts := strings.Split(row, "|")
		if len(parts) != 5 {
			log.Printf("Row did not have correct amount of parts: %+v", row)
			continue
		}

		accessionNumber := strings.Split(parts[4], ".txt")[0]
		split := strings.Split(accessionNumber, "/")
		accessionNumber = split[len(split)-1]
		accessionNumber = strings.ReplaceAll(accessionNumber, "-", "")

		d := &DailyFilingsRow{
			CIK:             parts[0],
			CompanyName:     parts[1],
			FormType:        parts[2],
			DateFiled:       parts[3],
			FileName:        parts[4],
			AccessionNumber: accessionNumber,
		}
		resp = append(resp, d)
	}

	return resp
}

func GetFilingsForYearQuarter(year, quarter int) ([]*DailyFilingsRow, error) {
	qtr, err := DownloadSECFile(fmt.Sprintf(indexURL, year, quarter))
	if err != nil {
		log.Println("failed to get master file")
		return nil, err
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(qtr))
	if err != nil {
		log.Println("Error reading the master link HTML")
		return nil, err
	}

	masterFiles := []string{}
	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		if href, ok := s.Attr("href"); ok && strings.HasPrefix(strings.TrimSpace(s.Text()), "master.") {
			masterFiles = append(masterFiles, fmt.Sprintf(indexURL, year, quarter)+href)
		}
	})

	log.Printf("Got %d master files", len(masterFiles))

	filings := []*DailyFilingsRow{}

	for _, masterFile := range masterFiles {
		// Check if the file already exists on disk
		var mf []byte
		var err error
		filePath := "masterfiles/" + strings.Split(masterFile, fmt.Sprintf("QTR%d/", quarter))[1]
		if _, err = os.Stat(filePath); errors.Is(err, os.ErrNotExist) {
			// path/to/whatever does not exist
			mf, err = DownloadSECFile(masterFile)
			if err != nil {
				log.Printf("Error downloading master file %s", masterFile)
				return nil, err
			}

			// Write file to disk
			err = ioutil.WriteFile(filePath, mf, 0777)
			if err != nil {
				log.Println("Failed to write file to disk", filePath)
				return nil, err
			}
		} else {
			// Read from disk
			mf, err = ioutil.ReadFile(filePath)
			if err != nil {
				log.Println("Error reading file on disk", filePath)
				return nil, err
			}
		}
		dfs := parseDailyMasterFile(mf)
		filings = append(filings, dfs...)
	}

	return filings, nil
}
