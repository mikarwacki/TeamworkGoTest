package customerimporter

import (
	"io"
	"os"
	"strings"
	"testing"
)

func TestExtractDomain(t *testing.T) {
	testCases := []struct {
		name           string
		inputEmail     string
		expectedDomain string
	}{
		{
			name:           "Simple valid email",
			inputEmail:     "test@example.com",
			expectedDomain: "example.com",
		},
		{
			name:           "Email with mixed case",
			inputEmail:     "User@Example.COM",
			expectedDomain: "example.com",
		},
		{
			name:           "Invalid email - no @ symbol",
			inputEmail:     "testexample.com",
			expectedDomain: "",
		},
		{
			name:           "Invalid email - multiple @ symbols",
			inputEmail:     "test@@example.com",
			expectedDomain: "",
		},
		{
			name:           "Invalid email - @ at the end",
			inputEmail:     "test@",
			expectedDomain: "",
		},
		{
			name:           "Empty string input",
			inputEmail:     "",
			expectedDomain: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualDomain := extractDomain(tc.inputEmail)

			if actualDomain != tc.expectedDomain {
				t.Errorf(
					"extractDomain(%q) = %q; want %q",
					tc.inputEmail,
					actualDomain,
					tc.expectedDomain,
				)
			}
		})
	}
}

func TestProcessCsv_ValidCsv(t *testing.T) {
	tc := struct {
		csvInputString       string
		expectedDomainsCount DomainsCount
	}{
		csvInputString: `first_name,last_name,email,gender,ip_address
Mildred,Hernandez,mhernandez0@github.io,Female,38.194.51.128
Bonnie,Ortiz,bortiz1@github.io,Female,197.54.209.129
Dennis,Henry,dhenry2@github.io,Male,155.75.186.217
Gary,Henderson,ghenderson6@acquirethisname.com,Male,30.97.220.14
Norma,Allen,nallen8@cnet.com,Female,168.67.162.1`,
		expectedDomainsCount: DomainsCount{DomainStats: []DomainStat{
			{
				Name:  "acquirethisname.com",
				Count: 1,
			},
			{
				Name:  "cnet.com",
				Count: 1,
			},
			{
				Name:  "github.io",
				Count: 3,
			},
		},
			TotalCount: 5,
		},
	}

	t.Run("valid csv", func(t *testing.T) {
		file, err := os.CreateTemp("", "csvTestFile_*.csv")
		if err != nil {
			t.Errorf("Error creating temp file: %v", err)
		}
		defer file.Close()
		defer os.Remove(file.Name())

		_, err = file.WriteString(tc.csvInputString)
		if err != nil {
			t.Errorf("error writing to file: %v", err)
		}

		domainsCount, err := ProcessFile(file.Name())

		if err != nil {
			t.Errorf("test failed")
		}

		if domainsCount.TotalCount != tc.expectedDomainsCount.TotalCount {
			t.Errorf("Total count: %d, expected: %d", domainsCount.TotalCount, tc.expectedDomainsCount.TotalCount)
		}

		for i, domain := range domainsCount.DomainStats {
			if domain.Name != tc.expectedDomainsCount.DomainStats[i].Name {
				t.Errorf("Domain name: %s, expected: %s", domain.Name, tc.expectedDomainsCount.DomainStats[i].Name)
			}
		}
	})
}

func TestProcessFile_EmptyCsv(t *testing.T) {
	tc := struct {
		csvInputString     string
		errorMessagePrefix string
	}{
		csvInputString:     "",
		errorMessagePrefix: "error reading the header of csv:",
	}

	t.Run("invalid_csv", func(t *testing.T) {
		file, err := os.CreateTemp("", "csvTestFile_*.csv")
		if err != nil {
			t.Errorf("Error creating temp file: %v", err)
		}
		defer file.Close()
		defer os.Remove(file.Name())

		_, err = file.WriteString(tc.csvInputString)
		if err != nil {
			t.Errorf("error writing to file: %v", err)
		}

		domainsCount, err := ProcessFile(file.Name())
		if err == nil {
			t.Error("error expected, got nil")
		}

		if err == io.EOF {
			t.Error("Expected wrapper of io.EOF error, got io.EOF")
		}

		if !strings.HasPrefix(err.Error(), tc.errorMessagePrefix) {
			t.Errorf("expected error message to start with: %s, got: %s", tc.errorMessagePrefix, err.Error())
		}

		if len(domainsCount.DomainStats) != 0 || domainsCount.TotalCount != 0 {
			t.Error("expected domainsCount to be empty struct")
		}
	})
}

func TestWriteFile(t *testing.T) {
	testCases := []struct {
		name                string
		domainsCount        DomainsCount
		expectedFileContent string
	}{
		{
			name: "valid_domains_count",
			domainsCount: DomainsCount{DomainStats: []DomainStat{
				{
					Name:  "acquirethisname.com",
					Count: 1,
				},
				{
					Name:  "cnet.com",
					Count: 1,
				},
				{
					Name:  "github.io",
					Count: 3,
				},
			},
				TotalCount: 5,
			},
			expectedFileContent: `Total number of customers: 5
Domain: acquirethisname.com, Customers: 1
Domain: cnet.com, Customers: 1
Domain: github.io, Customers: 3` + "\n",
		},
		{
			name:                "empty_domains_count",
			domainsCount:        DomainsCount{},
			expectedFileContent: "Total number of customers: 0\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			file, err := os.CreateTemp("", "csvTestFile_*.csv")
			if err != nil {
				t.Errorf("Error creating temp file: %v", err)
			}
			defer file.Close()
			defer os.Remove(file.Name())

			filePath := file.Name()
			err = writeFile(tc.domainsCount, &filePath)
			if err != nil {
				t.Fatalf("unexpected error occured: %v", err)
			}

			fileContents, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("error reading the contents of output file: %v", err)
			}

			if string(fileContents) != tc.expectedFileContent {
				t.Errorf("file contents %s, expected: %s", string(fileContents), tc.expectedFileContent)
			}
		})
	}
}
