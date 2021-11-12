package main

import (

	"github.com/olivere/elastic/v7"
)

type Record struct {

	PntID		uint32	`json:"pntID"`
	SegID		uint32	`json:"segID"`
	CivicNum	uint32	`json:"civicNum"`
	CommID		uint32	`json:"commID"`

	Civsuffix	string	`json:"civsuffix"`
	UnitNum		string	`json:"unitNum"`
	Add_loc		string	`json:"add_loc"`
	Strprefix	string	`json:"strprefix"`
	Strname		string	`json:"strname"`
	Strsuffix	string	`json:"strsuffix"`
	Strdir		string	`json:"strdir"`
	Comm		string	`json:"comm"`
	Mun			string	`json:"mun"`
	County		string	`json:"county"`

	Location 	GPS `json:"location"`
}

func (r Record) ProduceBulkInsert(indexName string) (*elastic.BulkIndexRequest) {
	BulkRequest := elastic.NewBulkIndexRequest().Index(indexName).Type("_doc").Doc(r)
	return BulkRequest
}

type GPS struct {
	Lat			float64	`json:"lat"`
	Lon		float64	`json:"lon"`
}

type Batch struct {
	Records []Record
}

func (b *Batch) AddRecordFromCSV(csv []string) bool {
	record, err := ConvertRecordToStruct(csv)

	if err != nil {
		return false
	}

	b.Records = append(b.Records, record)
	return true
}


func (b *Batch) GenerateBatchRequest(bulk *elastic.BulkService)  {
	for _, record := range b.Records {
		bulk.Add(record.ProduceBulkInsert("civic"))
	}
}

func (b *Batch) EmptyRecords()  {
	b.Records = []Record{}
}