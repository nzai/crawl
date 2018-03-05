package config

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-errors/errors"
	"github.com/nzai/crawl/context"
	"github.com/nzai/go-utility/io"
	"github.com/nzai/go-utility/path"
)

const (
	defaultConfigFile = "config.json"
)

// Config 配置
type Config struct {
	config map[string]interface{}
}

// New 新建配置
func New(config map[string]interface{}) *Config {
	return &Config{config: config}
}

// OpenFile 从文件中读取配置
func OpenFile(filePath string) (*Config, error) {

	if filePath == "" {
		dir, err := path.GetStartupDir()
		if err != nil {
			return nil, err
		}

		filePath = filepath.Join(dir, defaultConfigFile)
	}

	if !io.IsExists(filePath) {
		return nil, errors.Errorf("%s 不存在", filePath)
	}

	//	读取文件
	buffer, err := io.ReadAllBytes(filePath)
	if err != nil {
		return nil, err
	}

	//	解析配置项
	var config map[string]interface{}
	err = json.Unmarshal(buffer, &config)
	if err != nil {
		return nil, err
	}

	return &Config{config: config}, nil
}

// Get 获取指定key的值
func (c Config) Get(key string) (interface{}, error) {

	value, found := c.config[key]
	if !found {
		return "", errors.Errorf("key %s not found", key)
	}

	return value, nil
}

// String 获取指定key的字符串值
func (c Config) String(key string) (string, error) {

	v, err := c.Get(key)
	if err != nil {
		return "", err
	}

	value, ok := v.(string)
	if !ok {
		return "", errors.Errorf("key %s value %+v is not a string", key, v)
	}

	return value, nil
}

// Int 获取指定key的整形值
func (c Config) Int(key string) (int, error) {

	v, err := c.Get(key)
	if err != nil {
		return 0, err
	}

	value, ok := v.(int)
	if !ok {
		return 0, errors.Errorf("key %s value %+v is not a int", key, v)
	}

	return value, nil
}

// Strings 获取指定key的字符串数组
func (c Config) Strings(key string) ([]string, error) {

	v, err := c.Get(key)
	if err != nil {
		return nil, err
	}

	value, ok := v.([]string)
	if !ok {
		return nil, errors.Errorf("key %s value %+v is not a string array", key, v)
	}

	return value, nil
}

// Config 获取指定key的配置
func (c Config) Config(key string) (*Config, error) {

	v, err := c.Get(key)
	if err != nil {
		return nil, err
	}

	value, ok := v.(map[string]interface{})
	if !ok {
		return nil, errors.Errorf("key %s value %+v is not a map[string]interface{}", key, v)
	}

	return New(value), nil
}

// Configs 获取指定key的配置队列
func (c Config) Configs(key string) ([]*Config, error) {

	var value interface{}
	if key == "" {
		value = c.config
	} else {
		v, err := c.Get(key)
		if err != nil {
			return nil, err
		}
		value = v
	}

	array, ok := value.([]interface{})
	if !ok {
		return nil, errors.Errorf("key %s value %+v is not a interface{} array", key, value)
	}

	configs := make([]*Config, len(array))
	for index, item := range array {

		config, ok := item.(map[string]interface{})
		if !ok {
			return nil, errors.Errorf("key %s value %+v is not a map[string]interface{}", key, value)
		}

		configs[index] = New(config)
	}

	return configs, nil
}

// Duration 获取指定key的时间间隔
func (c Config) Duration(key string) (time.Duration, error) {

	value, err := c.String(key)
	if err != nil {
		return time.Nanosecond, err
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return time.Nanosecond, errors.New(err)
	}

	return duration, nil
}

// StringParameter 解析字符串参数
func (c Config) StringParameter(key string, ctx *context.Context) (string, error) {

	config, err := c.Config(key)
	if err != nil {
		return "", err
	}

	_type, err := config.String("type")
	if err != nil {
		return "", err
	}

	switch _type {
	case "static":
		return config.String("value")
	case "format":
		return config.formatString(ctx)
	default:
		return "", errors.Errorf("invalid string type: %s", _type)
	}
}

// formatString 获取格式化字符串
func (c Config) formatString(ctx *context.Context) (string, error) {

	pattern, err := c.String("pattern")
	if err != nil {
		return "", err
	}

	keys, err := c.Strings("keys")
	if err != nil {
		return "", err
	}

	values := make([]interface{}, 0, len(keys))
	for _, key := range keys {

		value, err := ctx.Get(key)
		if err != nil {
			return "", err
		}

		values = append(values, value)
	}

	return fmt.Sprintf(pattern, values...), nil
}

// IntParameter 解析整形参数
func (c Config) IntParameter(key string, ctx *context.Context) (int, error) {

	config, err := c.Config(key)
	if err != nil {
		return 0, err
	}

	_type, err := config.String("type")
	if err != nil {
		return 0, err
	}

	switch _type {
	case "static":
		return config.Int("value")
	case "context":
		return config.formatInt(ctx)
	default:
		return 0, errors.Errorf("invalid int type: %s", _type)
	}
}

// formatInt 获取格式化整形
func (c Config) formatInt(ctx *context.Context) (int, error) {

	key, err := c.String("key")
	if err != nil {
		return 0, err
	}

	value, err := ctx.Get(key)
	if err != nil {
		return 0, err
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return 0, errors.New(err)
	}

	return intValue, nil
}
