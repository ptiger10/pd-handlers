# pd-handlers
Connectors for converting other data sources in/out of GoPandas DataFrames and Series.

## Google Sheets
* Follow Google's documentation to [create a client](https://godoc.org/google.golang.org/api/sheets/v4) (referred to here as a "Service") in your preferred manner.*
* Create a SheetHandler with the Service and the ID of the target Sheet (for read methods, you may optionally specify the number of header rows or index columns in the target Sheet).

`
handler := SheetHandler{Service: ..., SpreadsheetID: ...}
`

*If authenticating using a Google service account (`option.WithCredentialsFile`) remember to Share the target Google Sheet(s) with the email address identified in the service account (e.g., foo@bar.iam.gserviceaccount.com) and give it Edit access. 
