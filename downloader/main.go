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
	"github.com/cenkalti/backoff/v4"
	"github.com/davecgh/go-spew/spew"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/samber/lo"
	"go.uber.org/ratelimit"
)

var (
	secRL = ratelimit.New(5)

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

func DownloadDailyFilingsToDisk(filings []*DailyFilingsRow) error {

	return nil
}
