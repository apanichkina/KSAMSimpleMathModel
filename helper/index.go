package helper

import (
	"fmt"
	"log"
	"github.com/apanichkina/KSAMSimpleMathModel/parser"
)

func CheckError(message string, err error) {
	if err != nil {
		var fullError = parser.Errors{{Message: fmt.Sprint(message, err)}}
		var err1 = parser.PrintToCsv("data/result.csv", fullError)
		if err1 != nil {
			log.Fatal(message, err1)
		}
		log.Fatal(message, err)
	}
}

func MbitToByte(num float64) (float64) {
	return num * 1024 * 1024 / 8
}

func HourToSecond(time float64) (float64) {
	return time / 3600
}