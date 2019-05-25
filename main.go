package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/marianogappa/gs/csvops"
	"github.com/marianogappa/gs/gcsv"
	"github.com/marianogappa/sqlparser"
	"github.com/marianogappa/sqlparser/query"
)

func main() {
	dataDir := mustReadDataDir()
	dataFolders := mustReadDataFolders(dataDir)
	handlers := map[string]http.Handler{
		"/list":  handlerList{dataFolders},
		"/query": handlerQuery{dataFolders},
	}

	mustServeHandlers(handlers, ":8080")
}

func mustReadDataDir() string {
	dataDir := os.Getenv("WG_DATA_DIR")
	if dataDir == "" {
		log.Fatal("Please set DATA_DIR env")
	}
	return dataDir
}

func mustServeHandlers(handlers map[string]http.Handler, addr string) {
	for pattern, handler := range handlers {
		http.Handle(pattern, handler)
	}
	log.Fatal(http.ListenAndServe(addr, nil))
}

func enableCORS(w *http.ResponseWriter) {
	(*w).Header().Set("Access-Control-Allow-Origin", "*")
}

func mustReadDataFolders(dataDir string) map[string]string {
	dataFolders := map[string]string{}

	files, err := ioutil.ReadDir(dataDir)
	if err != nil {
		log.Fatalf("mustReadDataFolders: error reading DATA_DIR: %v", err)
	}

	for _, f := range files {
		if f.IsDir() && !strings.HasPrefix(f.Name(), ".") {
			dataFolders[f.Name()] = fmt.Sprintf("%v/%v", dataDir, f.Name())
		}
	}
	return dataFolders
}

type handlerList struct {
	dataFolders map[string]string
}

func (h handlerList) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	enableCORS(&w)

	keys := []string{}
	for key := range h.dataFolders {
		keys = append(keys, key)
	}
	enc := json.NewEncoder(w)

	response := map[string]interface{}{
		"status": "success",
		"data":   keys,
	}
	enc.Encode(response)
}

type handlerQuery struct {
	dataFolders map[string]string
}

func (h handlerQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	enableCORS(&w)
	enc := json.NewEncoder(w)

	var sqls []string
	if err := json.NewDecoder(r.Body).Decode(&sqls); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fErr := fmt.Sprintf("invalid JSON decoding queries: %v", err)
		enc.Encode(map[string]interface{}{"status": "fail", "message": fErr})
		return
	}

	qs, err := sqlparser.ParseMany(sqls)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fErr := fmt.Sprintf("error parsing queries: %v", err)
		enc.Encode(map[string]interface{}{"status": "fail", "message": fErr})
		return
	}

	qsByTable := map[string][]query.Query{}
	tableHasSelect := map[string]bool{}
	for _, q := range qs {
		if _, ok := h.dataFolders[q.TableName]; !ok {
			w.WriteHeader(http.StatusBadRequest)
			fErr := fmt.Sprintf("[%v] not found", q.TableName)
			enc.Encode(map[string]interface{}{"status": "fail", "message": fErr})
			return
		}
		if tableHasSelect[q.TableName] {
			w.WriteHeader(http.StatusBadRequest)
			fErr := fmt.Sprintf("[%v] can accept at most one SELECT at a time\n", q.TableName)
			enc.Encode(map[string]interface{}{"status": "fail", "message": fErr})
			return
		}
		if _, ok := qsByTable[q.TableName]; ok && q.Type == query.Select {
			w.WriteHeader(http.StatusBadRequest)
			fErr := fmt.Sprintf("[%v] cannot accept SELECT and non-SELECT queries together\n", q.TableName)
			enc.Encode(map[string]interface{}{"status": "fail", "message": fErr})
			return
		}
		if q.Type == query.Select {
			tableHasSelect[q.TableName] = true
		}
		qsByTable[q.TableName] = append(qsByTable[q.TableName], q)
	}

	results := map[string]gcsv.CSV{}
	for tableName, tqs := range qsByTable {
		t := csvops.New(fmt.Sprintf("%v/%v.csv", h.dataFolders[tableName], tableName)) // !!
		results[tableName], err = t.Query(tqs)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fErr := fmt.Sprintf("[%v] error running query: %v\n", tqs[0].TableName, err)
			enc.Encode(map[string]interface{}{"status": "error", "message": fErr})
			return
		}
	}

	response := map[string]interface{}{
		"status": "success",
		"data":   results,
	}
	enc.Encode(response)
}
