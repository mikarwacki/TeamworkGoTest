// Package customerimporter reads from a CSV file and returns a sorted (data
// structure of your choice) of email domains along with the number of customers
// with e-mail addresses for each domain. This should be able to be ran from the
// CLI and output the sorted domains to the terminal or to a file. Any errors
// should be logged (or handled). Performance matters (this is only ~3k lines,
// but could be 1m lines or run on a small machine).
package customerimporter

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
)

const EMAIL_IDX = 2
const OUTPUT_LINE_FORMAT = "Domain: %s, Customers: %d\n"

type DomainStat struct {
	Name  string
	Count int
}

type DomainsCount struct {
	DomainStats []DomainStat
	TotalCount  int
}

func WriteOutput(domainsCount DomainsCount, filePath *string) error {
	if filePath != nil && *filePath != "" {
		return writeFile(domainsCount, filePath)
	} else {
		return writeStdOut(domainsCount)
	}
}

func writeFile(domainsCount DomainsCount, filePath *string) error {
	file, err := os.OpenFile(*filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("Error opening file: %v", err)
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	header := fmt.Sprintf("Total number of customers: %d\n", domainsCount.TotalCount)
	_, err = writer.WriteString(header)
	if err != nil {
		log.Printf("Error writing to file: %v\n", err)
		return fmt.Errorf("error writing to file: %s, %v", *filePath, err)
	}
	for _, domainStat := range domainsCount.DomainStats {
		line := fmt.Sprintf(OUTPUT_LINE_FORMAT, domainStat.Name, domainStat.Count)

		_, err := writer.WriteString(line)
		if err != nil {
			log.Printf("Error writing to file: %v\n", err)
			return fmt.Errorf("error writing to file: %s, %v", *filePath, err)
		}
	}

	err = writer.Flush()
	if err != nil {
		log.Printf("Error flushing the buffer: %v", err)
		return err
	}

	return nil
}

func writeStdOut(domainsCount DomainsCount) error {
	fmt.Printf("Total number of customers: %d\n", domainsCount.TotalCount)
	for _, domainStat := range domainsCount.DomainStats {
		fmt.Printf(OUTPUT_LINE_FORMAT, domainStat.Name, domainStat.Count)
	}
	return nil
}

func ProcessFile(filePath string) (*DomainsCount, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return &DomainsCount{}, err
	}
	defer file.Close()

	numWorkers := runtime.NumCPU()

	domainMap, totalcustomers, err := processCsv(file, numWorkers)
	if err != nil {
		return &DomainsCount{}, err
	}

	domainStats := createStats(domainMap)

	return &DomainsCount{
		DomainStats: domainStats,
		TotalCount:  totalcustomers,
	}, nil
}

func createStats(domainMap map[string]int) []DomainStat {
	domainStats := make([]DomainStat, 0, len(domainMap))
	for domain, customers := range domainMap {
		domainStats = append(domainStats, DomainStat{
			Name:  domain,
			Count: customers,
		})
	}

	sort.Slice(domainStats, func(i, j int) bool {
		return domainStats[i].Name < domainStats[j].Name
	})

	return domainStats
}

func processCsv(reader io.Reader, numWorkers int) (map[string]int, int, error) {
	csvreader := csv.NewReader(reader)

	_, err := csvreader.Read()
	if err != nil {
		return nil, 0, fmt.Errorf("error reading the header of csv: %v", err)
	}

	emailChan := make(chan string, numWorkers)
	domains := make(chan string, numWorkers)
	var wg sync.WaitGroup

	wg.Add(1)
	go csvReader(csvreader, emailChan, &wg)

	for range numWorkers {
		wg.Add(1)
		go extractDomains(domains, emailChan, &wg)
	}

	domainMap := make(map[string]int)
	totalCustomers := 0
	doneAggregating := make(chan struct{})

	go aggregateDomains(domains, domainMap, &totalCustomers, doneAggregating)

	wg.Wait()
	close(domains)

	<-doneAggregating

	return domainMap, totalCustomers, nil
}

func csvReader(csvreader *csv.Reader, emailChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(emailChan)
	lineNum := 0

	for {
		records, err := csvreader.Read()
		lineNum++
		if err == io.EOF {
			log.Println("End of file reached")
			break
		}
		if err != nil {
			log.Printf("Error reading csv line %d: %v\n", lineNum+1, err)
			continue
		}

		if len(records) <= EMAIL_IDX {
			log.Printf("Line %d email column index out of range\n", lineNum)
			continue
		}

		emailChan <- records[EMAIL_IDX]
	}
}

func extractDomains(domains chan string, emailChan chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for email := range emailChan {
		domain := extractDomain(strings.TrimSpace(email))
		if domain == "" {
			log.Println("Invalid email address, doesn't contain domain name")
		} else {
			domains <- domain
		}
	}
}

func extractDomain(email string) string {
	emailSplit := strings.SplitN(email, "@", 2)
	if len(emailSplit) != 2 || strings.Contains(emailSplit[1], "@") {
		return ""
	}

	return strings.ToLower(emailSplit[1])
}

func aggregateDomains(domains chan string, domainMap map[string]int, totalCustomers *int, doneAggregating chan struct{}) {
	defer close(doneAggregating)

	for domain := range domains {
		domainMap[domain]++
		*totalCustomers++
	}
}
