# Read protobufs from BigQuery with Go

[![PkgGoDev](https://pkg.go.dev/badge/github.com/way-platform/protobq-go)](https://pkg.go.dev/github.com/way-platform/protobq-go)
[![GoReportCard](https://goreportcard.com/badge/github.com/way-platform/protobq-go)](https://goreportcard.com/report/github.com/way-platform/protobq-go)
[![CI](https://github.com/way-platform/protobq-go/actions/workflows/release.yaml/badge.svg)](https://github.com/way-platform/protobq-go/actions/workflows/release.yaml)

Read protobufs from BigQuery with
[protobq.MessageLoader](https://pkg.go.dev/github.com/way-platform/protobq-go#MessageLoader).

## Writing protobufs to BigQuery

Protobuf messages can be written directly to BigQuery using the [storage write
API](https://cloud.google.com/bigquery/docs/supported-data-types#supported_protocol_buffer_data_types)
or [Pub/Sub BigQuery
subscriptions](https://cloud.google.com/pubsub/docs/bigquery#protocol-buffer-types).

BigQuery schemas for protobuf messages can be generated with
[protoc-gen-bq-schema](https://github.com/GoogleCloudPlatform/protoc-gen-bq-schema),
and Pub/Sub schemas can be generated with
[protoc-gen-pubsub](https://github.com/bufbuild/protoschema-plugins?tab=readme-ov-file#pubsub-protobuf-schema).

## Reading protobufs from BigQuery

[protobq.MessageLoader](https://pkg.go.dev/github.com/way-platform/protobq-go#MessageLoader)
implements the
[bigquery.ValueLoader](https://pkg.go.dev/cloud.google.com/go/bigquery#ValueLoader)
interface, and uses [the protobuf reflection
APIs](https://pkg.go.dev/google.golang.org/protobuf/reflect/protoreflect) to
reconstruct a protobuf message as it was written to BigQuery.

### Supported types

The goal of this library is simple:

If you were able to put the protobuf message in,
[protobq.MessageLoader](https://pkg.go.dev/github.com/way-platform/protobq-go#MessageLoader)
should be able to get it out.

For information about type mappings, see [docs/types.md](./docs/types.md).

### Example

```go
package main

import (
	"context"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/way-platform/protobq-go"
	"google.golang.org/genproto/googleapis/example/library/v1"
)

func main() {
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
```

## License

This SDK is published under the [MIT License](./LICENSE).

## Security

Security researchers, see the [Security Policy](https://github.com/way-platform/protobq-go?tab=security-ov-file#readme).

## Code of Conduct

Be nice. For more info, see the [Code of Conduct](https://github.com/way-platform/protobq-go?tab=coc-ov-file#readme).
