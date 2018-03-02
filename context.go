package main

import (
	"context"
	"fmt"

	"github.com/go-errors/errors"
)

// Context 上下文
type Context struct {
	ctx context.Context
}

// New 新建上下文
func New() *Context {
	return &Context{}
}

// String 获取字符串
func (c Context) String(value map[string]interface{}) (string, error) {

	if value["type"] == nil {
		return "", errors.New("invalid string type")
	}

	_type, ok := value["type"].(string)
	if !ok {
		return "", errors.Errorf("invalid string type: %+v", value["type"])
	}

	switch _type {
	case "static":
		return c.staticString(value)
	case "format":
		return c.formatString(value)
	default:
		return "", errors.Errorf("invalid string type: %s", _type)
	}
}

// staticString 静态字符串
func (c Context) staticString(value map[string]interface{}) (string, error) {

	if value["value"] == nil {
		return "", errors.New("invalid static string value")
	}

	stringValue, ok := value["value"].(string)
	if !ok {
		return "", errors.Errorf("invalid static string value: %+v", value["value"])
	}

	return stringValue, nil
}

// formatString 格式化字符串
func (c Context) formatString(value map[string]interface{}) (string, error) {

	if value["pattern"] == nil {
		return "", errors.New("invalid format string pattern")
	}

	pattern, ok := value["pattern"].(string)
	if !ok {
		return "", errors.Errorf("invalid format string pattern: %+v", value["pattern"])
	}

	if value["key"] == nil {
		return "", errors.New("invalid format string key")
	}

	key, ok := value["key"].([]interface{})
	if !ok {
		return "", errors.Errorf("invalid format string key: %+v", value["key"])
	}

	return fmt.Sprintf(pattern, key...), nil
}
