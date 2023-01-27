package main // "github.com/promotedai/metrics/api/main"

import (
	"os"
	"strconv"
	"strings"
)

func getEnvOrDefault(envVar string, defaultValue string) (value string) {
	var found bool
	if value, found = os.LookupEnv(envVar); !found {
		value = defaultValue
	}
	return
}

// Panics if bad
func unsafeParseBool(input string, emptyDefault bool) bool {
	trimmedInput := strings.TrimSpace(input)
	if trimmedInput == "" {
		return emptyDefault
	}
	value, err := strconv.ParseBool(trimmedInput)
	if err != nil {
		panic(err)
	}
	return value
}
