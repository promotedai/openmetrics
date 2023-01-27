package main // "github.com/promotedai/metrics/api/main"

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnsafeParseBool_GoodInput(t *testing.T) {
	assert.False(t, unsafeParseBool("", false))
	assert.False(t, unsafeParseBool("  ", false))
	assert.False(t, unsafeParseBool("f", false))
	assert.False(t, unsafeParseBool("F", false))
	assert.False(t, unsafeParseBool("false", false))
	assert.False(t, unsafeParseBool("False", false))
	assert.True(t, unsafeParseBool("t", false))
	assert.True(t, unsafeParseBool("T", false))
	assert.True(t, unsafeParseBool("true", false))
	assert.True(t, unsafeParseBool("True", false))
	assert.True(t, unsafeParseBool("", true))
	assert.False(t, unsafeParseBool("false", true))
	assert.False(t, unsafeParseBool("False", true))
	assert.True(t, unsafeParseBool("true", true))
	assert.True(t, unsafeParseBool("True", true))
	assert.True(t, unsafeParseBool("  True   ", false))
}

func TestUnsafeParseBool_BadInput(t *testing.T) {
	assert.Panics(t, func() { unsafeParseBool("Blah", true) })
	assert.Panics(t, func() { unsafeParseBool("ShouldBreak", true) })
}
