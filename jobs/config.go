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
func ReadFile(filePath string) ([]*Job, error) {
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
		// zap.L().Error("key not found", zap.String("key", key))
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

	int64Value, ok := v.(int64)
	if ok {
		return int(int64Value), nil
	}

	zap.L().Error("invalid value type", zap.String("key", key), zap.Any("value", v))
	return 0, fmt.Errorf("key [%s] value %+v is not a int, type:%s", key, v, reflect.TypeOf(v))
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
func (c Config) ToJobs() ([]*Job, error) {
	var jobs []*Job
	for key, value := range c {
		object, ok := value.(map[string]interface{})
		if ok {
			config := Config(object)
			job, err := c.toJob(key, &config)
			if err != nil {
				return nil, err
			}

			if job != nil {
				jobs = append(jobs, job)
			}
			continue
		}

		objects, ok := value.([]map[string]interface{})
		if ok {
			for _, object := range objects {
				config := Config(object)
				job, err := c.toJob(key, &config)
				if err != nil {
					return nil, err
				}

				if job != nil {
					jobs = append(jobs, job)
				}
			}
		}
	}

	return jobs, nil
}

// ToJobs parse config to jobs
func (c *Config) toJob(key string, conf *Config) (*Job, error) {
	switch key {
	case "fetch":
		return conf.toConditionJob(newFetch, (*c)["fetch_else"])
	case "match":
		return conf.toConditionJob(newMatch, (*c)["match_else"])
	case "range":
		return conf.toSequenceJob(newRange)
	case "execute":
		return conf.toSequenceJob(newExecute)
	case "replace":
		return conf.toSequenceJob(newReplace)
	case "exists":
		return conf.toConditionJob(newExists, (*c)["exists_else"])
	case "list":
		return conf.toSequenceJob(newList)
	case "list_dir":
		return conf.toSequenceJob(newListDir)
	case "oss_exists":
		return conf.toConditionJob(newOssExists, nil)
	case "oss_upload":
		return conf.toSequenceJob(newOssUpload)
	case "oss_download":
		return conf.toSequenceJob(newOssDownload)
	case "fetch_else", "match_else", "exists_else":
		return nil, nil
	default:
		zap.L().Error("invalid action", zap.String("action", key))
		return nil, ErrInvalidAction
	}
}

// ToJtoElseJobobs parse config to else jobs
func (c *Config) toSequenceJob(fun func(c *Config) (interface{}, error)) (*Job, error) {
	action, err := fun(c)
	if err != nil {
		return nil, err
	}

	subJobs, err := c.ToJobs()
	if err != nil {
		return nil, err
	}

	return &Job{Action: action, Jobs: subJobs}, nil
}

// toConditionJob parse config to else jobs
func (c *Config) toConditionJob(fun func(*Config) (interface{}, error), elseValue interface{}) (*Job, error) {
	action, err := fun(c)
	if err != nil {
		return nil, err
	}

	subJobs, err := c.ToJobs()
	if err != nil {
		return nil, err
	}

	if elseValue == nil {
		return &Job{Action: action, Jobs: subJobs}, nil
	}

	object, ok := elseValue.(map[string]interface{})
	if !ok {
		return &Job{Action: action, Jobs: subJobs}, nil
	}

	config := Config(object)
	elseJobs, err := config.ToJobs()
	if err != nil {
		return nil, err
	}

	return &Job{Action: action, Jobs: subJobs, ElseJobs: elseJobs}, nil
}
