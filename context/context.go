package context

import (
	"errors"
	"fmt"
)

// Context 上下文
type Context struct {
	context map[string]string
}

// New 新建上下文
func New() *Context {
	return &Context{context: make(map[string]string)}
}

// Clone 克隆
func (c Context) Clone() *Context {
	context := New()
	for key, value := range c.context {
		context.Set(key, value)
	}

	return context
}

// Set 设置值
func (c *Context) Set(key, value string) error {
	if key == "" {
		return errors.New("key is empty")
	}

	c.context[key] = value
	return nil
}

// Get 获取指定key的值
func (c Context) Get(key string) (string, error) {
	value, found := c.context[key]
	if !found {
		return "", fmt.Errorf("key %s not found", key)
	}

	return value, nil
}
