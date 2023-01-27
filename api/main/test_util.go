// Copied and modified from go-delivery.
package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/nsf/jsondiff"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func AssertProtoEquals(t *testing.T, expected proto.Message, actual proto.Message) {
	expectedJSON, _ := protojson.Marshal(expected)
	AssertProtoEqualsJSONBytes(t, expectedJSON, actual)
}

func AssertProtoEqualsJSON(t *testing.T, expectedJSON string, m proto.Message) {
	AssertProtoEqualsJSONBytes(t, []byte(expectedJSON), m)
}

func AssertProtoEqualsJSONBytes(t *testing.T, expectedJSON []byte, m proto.Message) {
	diffOpts := jsondiff.DefaultConsoleOptions()
	messageJSON, _ := protojson.Marshal(m)
	res, diff := jsondiff.Compare(expectedJSON, []byte(messageJSON), &diffOpts)
	if res != jsondiff.FullMatch {
		t.Errorf("protos differ: %s", diff)
	}
}

func AssertProtoEqualsTestDataFile(t *testing.T, relPath string, m proto.Message) {
	json, _ := ioutil.ReadFile(getTestDataPath(relPath))
	AssertProtoEqualsJSONBytes(t, json, m)
}

func DumpJSON(m proto.Message, relPath string) error {
	opts := protojson.MarshalOptions{Multiline: true}
	protoBytes, err := opts.Marshal(m)
	if err != nil {
		return nil
	}

	f, err := os.Create(getTestDataPath(relPath))
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = f.Write(protoBytes)

	return err
}

func getTestDataPath(relPath string) string {
	wd, _ := os.Getwd()
	return filepath.Join(wd, "testdata", relPath)
}
