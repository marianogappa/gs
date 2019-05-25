package gcsv

import (
	"fmt"

	"github.com/marianogappa/sqlparser/query"
)

type CSV struct {
	FieldIndex map[string]int `json:"fieldIndex"`
	Fields     []string       `json:"fields"`
	Lines      [][]string     `json:"lines"`
}

func New(fields []string, lines [][]string) CSV {
	fieldIndex := map[string]int{}
	for i, f := range fields {
		fieldIndex[f] = i
	}
	return CSV{FieldIndex: fieldIndex, Fields: fields, Lines: lines}
}

func (c CSV) Execute(q query.Query) (CSV, error) {
	newCSV := c.cloneStructure()
	switch q.Type {
	case query.Select:
		isAll := false
		for _, f := range q.Fields {
			if f == "*" {
				isAll = true
			}
		}
		if !isAll { // check if some field doesn't exist
			for _, f := range q.Fields {
				if _, ok := newCSV.FieldIndex[f]; !ok {
					return CSV{}, fmt.Errorf("Field [%v] doesn't exist on table [%v]", f, q.TableName)
				}
			}
		}
		for i := range c.Lines {
			if ok, err := c.satisfiesConditions(i, q.Conditions); ok && err == nil {
				if isAll { // if SELECT *
					newCSV.Lines = append(newCSV.Lines, c.Lines[i])
					continue
				}
				line := []string{}
				for _, f := range q.Fields { // construct line based on selected fields
					line = append(line, c.Lines[i][newCSV.FieldIndex[f]])
				}
				newCSV.Lines = append(newCSV.Lines, line)
			}
		}
		if !isAll {
			newCSV.Fields = q.Fields
			newCSV.FieldIndex = map[string]int{}
			for i, f := range q.Fields {
				newCSV.FieldIndex[f] = i
			}
		}
	case query.Update:
		for i := range c.Lines {
			if ok, err := c.satisfiesConditions(i, q.Conditions); ok && err == nil {
				newLine := []string{}
				for j, field := range c.Fields {
					if v, ok := q.Updates[field]; ok {
						newLine = append(newLine, v)
					} else {
						newLine = append(newLine, c.Lines[i][j])
					}
				}
				newCSV.Lines = append(newCSV.Lines, newLine)
			} else {
				newCSV.Lines = append(newCSV.Lines, c.Lines[i])
			}
		}
	case query.Insert:
		tableIndexToQueryIndex := map[int]int{}
		for i, f := range q.Fields { // check if some field doesn't exist
			if _, ok := newCSV.FieldIndex[f]; !ok {
				return CSV{}, fmt.Errorf("Field [%v] doesn't exist on table [%v]", f, q.TableName)
			}
			tableIndexToQueryIndex[newCSV.FieldIndex[f]] = i
		}
		newCSV.Lines = append(newCSV.Lines, c.Lines...)
		for _, insert := range q.Inserts { // construct each insert respecting table fields and order
			line := []string{}
			for tableIndex := range newCSV.Fields {
				queryIndex, ok := tableIndexToQueryIndex[tableIndex]
				if !ok {
					line = append(line, "")
					continue
				}
				line = append(line, insert[queryIndex])
			}
			newCSV.Lines = append(newCSV.Lines, line)
		}
	case query.Delete:
		for i := range c.Lines {
			if ok, err := c.satisfiesConditions(i, q.Conditions); !ok || err != nil {
				newCSV.Lines = append(newCSV.Lines, c.Lines[i])
			}
		}
	}
	return newCSV, nil
}

func (c CSV) satisfiesConditions(i int, conds []query.Condition) (bool, error) {
	for _, cond := range conds {
		if ok, err := c.satisfiesCondition(i, cond); !ok || err != nil {
			return ok, err
		}
	}
	return true, nil
}

func (c CSV) satisfiesCondition(i int, cond query.Condition) (bool, error) {
	sOp1 := cond.Operand1
	if cond.Operand1IsField {
		sOp1 = c.Lines[i][c.FieldIndex[cond.Operand1]]
	}
	sOp2 := cond.Operand2
	if cond.Operand2IsField {
		sOp2 = c.Lines[i][c.FieldIndex[cond.Operand2]]
	}
	switch cond.Operator {
	case query.Eq:
		return sOp1 == sOp2, nil
	case query.Ne:
		return sOp1 != sOp2, nil
	case query.Gt:
		return sOp1 > sOp2, nil
	case query.Lt:
		return sOp1 < sOp2, nil
	case query.Gte:
		return sOp1 >= sOp2, nil
	case query.Lte:
		return sOp1 <= sOp2, nil
		// case query.Regexp:
		// 	return regexp.MatchString(sOp2, sOp1)
	}
	return false, nil
}

func (c CSV) cloneStructure() CSV {
	cloneMSI := func(msi map[string]int) map[string]int {
		newMSI := map[string]int{}
		for k, v := range msi {
			newMSI[k] = v
		}
		return newMSI
	}
	cloneSS := func(ss []string) []string {
		newSS := make([]string, len(ss))
		copy(newSS, ss)
		return newSS
	}
	return CSV{FieldIndex: cloneMSI(c.FieldIndex), Fields: cloneSS(c.Fields)}
}
