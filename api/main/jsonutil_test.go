package main // "github.com/promotedai/metrics/api/main"

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTryToJsonString(t *testing.T) {
	expectedRequest := newTestLogRequest()
	// Just make sure it returns some string.
	// protojson does not guarantee the output will stay consistent over time.
	assert.NotEqual(t, 0, len(tryToJsonString(&expectedRequest)))
}
