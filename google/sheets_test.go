package google

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/ptiger10/pd/dataframe"
	"github.com/ptiger10/pd/series"
	"google.golang.org/api/option"
	"google.golang.org/api/sheets/v4"
)

var service *sheets.Service

// srv returns a fully scoped spreadsheet service singleton.
func srv() *sheets.Service {
	if service == nil {
		creds := option.WithCredentialsFile("credentials/client_secret.json")
		ctx := context.Background()
		var err error
		service, err = sheets.NewService(ctx, creds)
		if err != nil {
			log.Fatalf("srv(): sheets.NewService(): %s", err)
		}
	}
	return service
}

func readFile() string {
	var testSheet struct {
		SpreadsheetID string `json:"spreadsheetId"`
	}
	b, err := ioutil.ReadFile("credentials/test_sheet.json")
	if err != nil {
		log.Fatalf("readTest(): %s", err)
	}
	json.Unmarshal(b, &testSheet)
	return testSheet.SpreadsheetID
}

var handler SheetHandler

func TestMain(m *testing.M) {

	handler = SheetHandler{
		Service:       srv(),
		SpreadsheetID: readFile(),
		HeaderRows:    1,
	}
	code := m.Run()
	os.Exit(code)
}

func Test_readDataFrame(t *testing.T) {
	type args struct {
		_range string
	}
	tests := []struct {
		name    string
		args    args
		want    *dataframe.DataFrame
		wantErr bool
	}{
		{
			name: "pass",
			args: args{
				_range: "Read",
			},
			want: dataframe.MustNew(
				[]interface{}{
					[]string{"1", "2", "3", "4"},
					[]string{"baz", "qux", "quux", "quuz"},
				},
				dataframe.Config{Col: []string{"foo", "bar"}},
			),
			wantErr: false,
		},
		{"fail", args{_range: "Sheet11"}, dataframe.MustNew(nil), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := handler.ReadDataFrame(tt.args._range)
			if (err != nil) != tt.wantErr {
				t.Errorf("readDataFrame() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !dataframe.Equal(got, tt.want) {
				t.Errorf("readDataFrame() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readSeries(t *testing.T) {
	type args struct {
		_range string
	}
	tests := []struct {
		name    string
		args    args
		want    *series.Series
		wantErr bool
	}{
		{
			name: "pass",
			args: args{
				_range: "Read",
			},
			want:    series.MustNew([]string{"1", "2", "3", "4"}, series.Config{Name: "foo"}),
			wantErr: false,
		},
		{"fail", args{_range: "Sheet10"}, series.MustNew(nil), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := handler.ReadSeries(tt.args._range)
			if (err != nil) != tt.wantErr {
				t.Errorf("readSeries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !series.Equal(got, tt.want) {
				t.Errorf("readSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_writeDataFrame(t *testing.T) {
	type args struct {
		_range string
		data   *dataframe.DataFrame
		clear  bool
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "write",
			args: args{
				_range: "Write",
				data: dataframe.MustNew(
					[]interface{}{
						[]string{"1", "2", "3", "4"},
						[]string{"baz", "qux", "quux", "quuz"},
					},
				),
				clear: false,
			}, wantErr: false,
		},
		{"overwrite",
			args{
				_range: "OverwriteAll",
				data: dataframe.MustNew(
					[]interface{}{[]string{"1", "2", "3", "4"}},
				),
				clear: true,
			}, false},
		{"fail", args{"Sheet11", dataframe.MustNew([]interface{}{"foo"}), false}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handler.WriteDataFrame(tt.args._range, tt.args.data, tt.args.clear)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeDataFrame() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_append(t *testing.T) {
	type args struct {
		_range string
		data   *dataframe.DataFrame
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "append",
			args: args{
				_range: "Append",
				data: dataframe.MustNew(
					[]interface{}{
						[]string{"1", "2"},
						[]string{"bar", "baz"},
					},
				),
			}, wantErr: false,
		},
		{"fail", args{"Sheet1", dataframe.MustNew([]interface{}{"foo"})}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handler.Append(tt.args._range, tt.args.data)
			if (err != nil) != tt.wantErr {
				t.Errorf("writeDataFrame() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
