package parser

import (
	"fmt"
	"os"
	"github.com/gocarina/gocsv"
)

func PrintToCsv(filename string, output interface{}) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		return fmt.Errorf("can't open %q to write: %s", filename, err)
	}
	defer f.Close()

	err = gocsv.MarshalFile(output, f) // Use this to save the CSV back to the file
	if err != nil {
		return fmt.Errorf("can't write to %q csv: %s", filename, err)
	}
	return nil
}


