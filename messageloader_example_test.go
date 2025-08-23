package protobq_test

import (
	"context"
	"os"

	"cloud.google.com/go/bigquery"
	protobq "github.com/way-platform/protobq-go"
	"google.golang.org/genproto/googleapis/example/library/v1"
)

func ExampleMessageLoader() {
	// 1. Connect to BigQuery.
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, os.Getenv("GOOGLE_CLOUD_PROJECT"))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	// 2. Run a query.
	query := client.Query(`
		SELECT
			"George Orwell" as author,
			"1984" as title,
	`)
	it, err := query.Read(ctx)
	if err != nil {
		panic(err)
	}
	// 3. Load the result into a protobuf message.
	var book library.Book
	if err := it.Next(&protobq.MessageLoader{
		Message: &book,
	}); err != nil {
		panic(err)
	}
	// 4. Profit.
}
