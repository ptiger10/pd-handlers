package google

import (
	"fmt"

	"github.com/ptiger10/pd"
	"github.com/ptiger10/pd/dataframe"
	"github.com/ptiger10/pd/series"
	"google.golang.org/api/sheets/v4"
)

// SheetHandler writes to/reads from a Google Sheets workbook using a Service object.
//
// Most Sheet methods take a _range, which is a Sheets range in A1 notation
// (e.g., "Sheet1!A1:C5" or "Sheet1!A:A").
// A sheet name by itself (e.g., "Sheet1") is valid A1 notation and refers to all the data in that Sheet.
type SheetHandler struct {
	Service       *sheets.Service
	SpreadsheetID string
	HeaderRows    int
	IndexCols     int
}

// ReadDataFrame converts data in the range into a DataFrame.
func (h SheetHandler) ReadDataFrame(_range string) (*dataframe.DataFrame, error) {
	resp, err := h.Service.Spreadsheets.Values.
		BatchGet(h.SpreadsheetID).Ranges(_range).MajorDimension("ROWS").Do()
	if err != nil {
		return dataframe.MustNew(nil), fmt.Errorf("readDataFrame(): retrieving data from %s: %s", _range, err)
	}
	// ducks error because inputs are controlled
	df, _ := pd.ReadInterface(
		resp.ValueRanges[0].Values, pd.ReadOptions{
			HeaderRows: h.HeaderRows, IndexCols: h.IndexCols},
	)
	return df, nil
}

// ReadSeries converts the first column in the range into a Series.
func (h SheetHandler) ReadSeries(_range string) (*series.Series, error) {
	df, err := h.ReadDataFrame(_range)
	if err != nil {
		return series.MustNew(nil), fmt.Errorf("readSeries(): %v", err)
	}
	return df.ColAt(0), nil
}

// WriteDataFrame writes a DataFrame into a Sheet beginning at the specified range.
// If clear is true, the entire sheet is cleared first (i.e., the sheet is overwritten completely).
func (h SheetHandler) WriteDataFrame(_range string, df *dataframe.DataFrame, clear bool) error {
	vr := sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         df.Export(),
	}
	if clear {
		_, err := h.Service.Spreadsheets.Values.
			Clear(h.SpreadsheetID, _range, &sheets.ClearValuesRequest{}).Do()
		if err != nil {
			return fmt.Errorf("writeDataFrame().Clear(): %v", err)
		}
	}
	_, err := h.Service.Spreadsheets.Values.
		Update(h.SpreadsheetID, _range, &vr).ValueInputOption("RAW").Do()
	if err != nil {
		return fmt.Errorf("writeDataFrame().Update(): %v", err)
	}
	return nil
}

// Append appends the last row of a DataFrame as the next row in a range.
func (h SheetHandler) Append(_range string, df *dataframe.DataFrame) error {
	if df.Len() == 0 {
		return fmt.Errorf("Append(): df cannot be empty")
	}
	vr := sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         [][]interface{}{df.Row(df.Len() - 1).Values},
	}
	_, err := h.Service.Spreadsheets.Values.
		Append(h.SpreadsheetID, _range, &vr).ValueInputOption("RAW").Do()
	if err != nil {
		return fmt.Errorf("Append: %v", err)
	}
	return nil
}
