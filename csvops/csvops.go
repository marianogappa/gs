package csvops

import (
	"bufio"
	"encoding/csv"
	"os"

	"github.com/marianogappa/gs/gcsv"
	"github.com/marianogappa/sqlparser/query"
)

type CSVOps struct {
	path string
}

func New(path string) CSVOps {
	return CSVOps{path}
}

func (c CSVOps) Query(queries []query.Query) (gcsv.CSV, error) {
	csv, err := c.readAll()
	if err != nil {
		return gcsv.CSV{}, err
	}

	for _, q := range queries {
		if csv, err = csv.Execute(q); err != nil {
			return gcsv.CSV{}, err
		}
		if q.Type == query.Select {
			return csv, nil
		}
	}

	return gcsv.CSV{}, c.writeAll(csv)
}

func (c CSVOps) readAll() (gcsv.CSV, error) {
	csvFile, err := os.Open(c.path)
	if err != nil {
		return gcsv.CSV{}, err
	}
	defer csvFile.Close()
	reader := csv.NewReader(bufio.NewReader(csvFile))
	records, err := reader.ReadAll()
	if err != nil {
		return gcsv.CSV{}, err
	}
	return gcsv.New(records[0], records[1:]), nil
}

func (c CSVOps) writeAll(gcsv gcsv.CSV) error {
	if err := os.Truncate(c.path, 0); err != nil {
		return err
	}
	f, err := os.OpenFile(c.path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.Write(gcsv.Fields)
	for _, line := range gcsv.Lines {
		w.Write(line)
	}
	w.Flush()
	return f.Close()
}
