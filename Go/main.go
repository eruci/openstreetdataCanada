package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/olivere/elastic/v7"
	"io"
	"log"
	"os"
	"strconv"
)


func stringToUint32(s string) (uint32, error) {
	i64, err := strconv.ParseInt(s, 10, 32)

	if err != nil {
		return 0, err
	}

	i := uint32(i64)
	return i, nil
}

func ConvertRecordToStruct(csv []string) (Record, error) {

	pntID, 		err := stringToUint32(csv[1])
	segID, 		err := stringToUint32(csv[2])
	civicNum, 	err := stringToUint32(csv[3])
	commID, 	err := stringToUint32(csv[4])

	lat, 		err := strconv.ParseFloat(csv[15], 32)
	long, 		err := strconv.ParseFloat(csv[16], 32)

	if err != nil {
		return Record{}, err
	}

	r := Record{
		PntID:     pntID,
		SegID:     segID,
		CivicNum:  civicNum,
		CommID:    commID,

		Civsuffix: csv[5],
		UnitNum:   csv[6],
		Add_loc:   csv[7],
		Strprefix: csv[8],
		Strname:   csv[9],
		Strsuffix: csv[10],
		Strdir:    csv[11],
		Comm:      csv[12],
		Mun:       csv[13],
		County:    csv[14],
		Location: GPS{
			Lat:  lat,
			Lon: long,
		},
	}
	return r, nil
}

func main() {

	client, _ := elastic.NewClient()
	bulkRequest := client.Bulk()

	var batch Batch

	lineCount := 0

	csvHandle, err := os.Open("../Provinces/NovaScotia/Civic_Points.csv")
	if err != nil {
		log.Fatalln("File not valid, could not open")
	}

	Reader := csv.NewReader(csvHandle)

	// Drop header
	_, err = Reader.Read()

	for {

		// Read each record from csv
		csvRecord, err := Reader.Read()

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		batch.AddRecordFromCSV(csvRecord)

		lineCount += 1

		if lineCount > 5000 {
			batch.GenerateBatchRequest(bulkRequest)
			fmt.Println("Request to Process: ",bulkRequest.NumberOfActions())
			bulkResponse, err := bulkRequest.Do(context.Background())
			if err != nil {
				log.Fatalln(err)
			}
			if bulkResponse.Errors {
				failedResults := bulkResponse.Failed()
				fmt.Println(failedResults)
			}
			batch.EmptyRecords()
			lineCount = 0
		}
	}
	fmt.Println("Done, Finished: ", lineCount)
}
