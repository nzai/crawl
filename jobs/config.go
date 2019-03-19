package jobs

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/BurntSushi/toml"
	"go.uber.org/zap"
)

var (
	// ErrKeyNotFound key not found error
	ErrKeyNotFound = errors.New("key not found")
)

// Config job config
type Config map[string]interface{}

// ReadFile read jobs from toml file
func ReadFile(filePath string) ([]Job, error) {
	_, err := os.Stat(filePath)
	if err != nil {
		zap.L().Error("file not found", zap.Error(err), zap.String("path", filePath))
		return nil, err
	}

	// unmarshal toml
	c := new(Config)
	_, err = toml.DecodeFile(filePath, c)
	if err != nil {
		zap.L().Error("unmarshal job file failed", zap.Error(err), zap.String("path", filePath))
		return nil, err
	}

	return c.ToJobs()
}

// Get get raw value
func (c Config) Get(key string) (interface{}, error) {
	value, found := c[key]
	if !found {
		zap.L().Error("key not found", zap.String("key", key))
		return "", ErrKeyNotFound
	}

	return value, nil
}

// String get string value
func (c Config) String(key string) (string, error) {
	v, err := c.Get(key)
	if err != nil {
		return "", err
	}

	value, ok := v.(string)
	if !ok {
		zap.L().Error("invalid value type", zap.String("key", key), zap.Any("value", v))
		return "", fmt.Errorf("key [%s] value %+v is not a string", key, v)
	}

	return value, nil
}

// StringDefault get string value or default
func (c Config) StringDefault(key, defaultValue string) string {
	value, err := c.String(key)
	if err != nil {
		return defaultValue
	}

	return value
}

// Int get int value
func (c Config) Int(key string) (int, error) {
	v, err := c.Get(key)
	if err != nil {
		return 0, err
	}

	value, ok := v.(float64)
	if !ok {
		zap.L().Error("invalid value type", zap.String("key", key), zap.Any("value", v))
		return 0, fmt.Errorf("key [%s] value %+v is not a int, type:%s", key, v, reflect.TypeOf(v))
	}

	return int(value), nil
}

// IntDefault get int value or default
func (c Config) IntDefault(key string, defaultValue int) int {
	value, err := c.Int(key)
	if err != nil {
		return defaultValue
	}

	return value
}

// Bool get boolean value
func (c Config) Bool(key string) (bool, error) {
	v, err := c.Get(key)
	if err != nil {
		return false, err
	}

	value, ok := v.(bool)
	if !ok {
		zap.L().Error("invalid value type", zap.String("key", key), zap.Any("value", v))
		return false, fmt.Errorf("key [%s] value %+v is not a bool, type:%s", key, v, reflect.TypeOf(v))
	}

	return value, nil
}

// BoolDefault get boolean value or default
func (c Config) BoolDefault(key string, defaultValue bool) bool {
	value, err := c.Bool(key)
	if err != nil {
		return defaultValue
	}

	return value
}

// Duration get time duration
func (c Config) Duration(key string) (time.Duration, error) {
	value, err := c.String(key)
	if err != nil {
		return time.Nanosecond, err
	}

	duration, err := time.ParseDuration(value)
	if err != nil {
		return time.Nanosecond, err
	}

	return duration, nil
}

// DurationDefault get time duration or default
func (c Config) DurationDefault(key string, defaultValue time.Duration) time.Duration {
	value, err := c.Duration(key)
	if err != nil {
		return defaultValue
	}

	return value
}

// Strings get string slice
func (c Config) Strings(key string) ([]string, error) {
	v, err := c.Get(key)
	if err != nil {
		return nil, err
	}

	array, ok := v.([]interface{})
	if !ok {
		zap.L().Error("invalid value type", zap.String("key", key), zap.Any("value", v))
		return nil, fmt.Errorf("key [%s] value %+v is not a array", key, v)
	}

	values := make([]string, len(array))
	for index, value := range array {
		stringValue, ok := value.(string)
		if !ok {
			zap.L().Error("invalid value type", zap.String("key", key), zap.Any("value", value))
			return nil, fmt.Errorf("key [%s] value %+v is not a string", key, v)
		}

		values[index] = stringValue
	}

	return values, nil
}

// ToJobs parse config to jobs
func (c Config) ToJobs() ([]Job, error) {
	var jobs []Job
	var job Job
	var err error
	for key, value := range c {
		c, ok := value.(map[string]interface{})
		if !ok {
			continue
		}

		config := Config(c)
		switch key {
		case "fetch":
			job, err = NewFetch(&config)
		default:
			continue
		}

		if err != nil {
			return nil, err
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}
