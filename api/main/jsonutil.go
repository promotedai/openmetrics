package main // "github.com/promotedai/metrics/api/main"

import (
	"fmt"

	"github.com/promotedai/schema-internal/generated/go/proto/event"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protojson"
)

func tryToJsonString(logRequest *event.LogRequest) string {
	json, err := protojson.Marshal(logRequest)
	if err == nil {
		return string(json)
	} else {
		stringValue := fmt.Sprintf("%+v", logRequest)
		log.Warningf("protojson.Marshal failed with an error; error=%s, logRequest=%s\n", err.Error(), stringValue)
		return stringValue
	}
}
