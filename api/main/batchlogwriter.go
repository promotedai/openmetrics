package main // "github.com/promotedai/metrics/api/main"

import (
	"context"
	"fmt"

	"github.com/promotedai/schema-internal/generated/go/proto/event"
	"google.golang.org/protobuf/encoding/protojson"
)

type BatchLogWriter interface {
	WriteLogRequest(ctx context.Context, logRequest *event.LogRequest) error
	Close() error
}

type PrintBatchLogWriter struct {
}

func (w *PrintBatchLogWriter) WriteLogRequest(ctx context.Context, logRequest *event.LogRequest) error {
	valueJson, err := protojson.Marshal(logRequest)
	if err == nil {
		fmt.Printf("PrintBatchLogWriter.WriteLogRequest(). valueJson=%s\n", valueJson)
	} else {
		fmt.Printf("PrintBatchLogWriter.WriteLogRequest(). err=%s\n", err)
	}
	return err
}

func (w *PrintBatchLogWriter) Close() error {
	return nil
}

// NoopBatchLogWriter does no operation.  Not even printing.
type NoopBatchLogWriter struct {
}

func (w *NoopBatchLogWriter) WriteLogRequest(ctx context.Context, logRequest *event.LogRequest) error {
	return nil
}

func (w *NoopBatchLogWriter) Close() error {
	return nil
}
