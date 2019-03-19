package jobs

import (
	"os"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

// Context job context
type Context map[string]string

// NewContextFromEnv create context from env
func NewContextFromEnv() *Context {
	ctx := Context(make(map[string]string))

	lines := os.Environ()
	for _, line := range lines {
		index := strings.Index(line, "=")
		if index < 0 {
			continue
		}

		ctx.Set(line[:index], line[index+1:])
	}

	return &ctx
}

// Clone clone context
func (c Context) Clone() *Context {
	ctx := Context(make(map[string]string))
	for key, value := range c {
		ctx.Set(key, value)
	}

	return &ctx
}

// Set set key value
func (c Context) Set(key, value string) {
	c[key] = value
}

// Int get int value
func (c Context) Int(key string) (int, error) {
	value, found := c[key]
	if !found {
		return 0, ErrKeyNotFound
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		zap.L().Error("parse value to int failed",
			zap.Error(err),
			zap.String("value", value))
		return 0, err
	}

	return intValue, nil
}

// IntDefault get int value or default
func (c Context) IntDefault(key string, defaultValue int) int {
	value, err := c.Int(key)
	if err != nil {
		return defaultValue
	}

	return value
}

// Expand expand expression by context
func (c Context) Expand(expression string) string {
	return os.Expand(expression, func(key string) string {
		value, found := c[key]
		if !found {
			return key
		}

		return value
	})
}
