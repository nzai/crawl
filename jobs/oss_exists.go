package jobs

import (
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"go.uber.org/zap"
)

// OssExists ossExists external command
type OssExists struct {
	endPoint   string
	keyID      string
	keySecret  string
	bucket     string
	key        string
	toContinue bool
	debug      bool
}

// newOssExists create ossExists action
func newOssExists(c *Config) (*OssExists, error) {
	endPoint, err := c.String("endpoint")
	if err != nil {
		return nil, err
	}

	keyID, err := c.String("key_id")
	if err != nil {
		return nil, err
	}

	keySecret, err := c.String("key_secret")
	if err != nil {
		return nil, err
	}

	bucket, err := c.String("bucket")
	if err != nil {
		return nil, err
	}

	key, err := c.String("key")
	if err != nil {
		return nil, err
	}

	_continue, err := c.Bool("continue")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &OssExists{
		endPoint:   endPoint,
		keyID:      keyID,
		keySecret:  keySecret,
		bucket:     bucket,
		key:        key,
		toContinue: _continue,
		debug:      debug,
	}, nil
}

// Do do job
func (s OssExists) Do(ctx *Context) (bool, error) {
	client, err := oss.New(s.endPoint, s.keyID, s.keySecret)
	if err != nil {
		zap.L().Error("create new aliyun oss client failed",
			zap.Error(err),
			zap.String("endpoint", s.endPoint),
			zap.String("accessKeyID", s.keyID),
			zap.String("accessKeySecret", s.keySecret))
		return false, err
	}

	bucket, err := client.Bucket(s.bucket)
	if err != nil {
		zap.L().Error("get aliyun oss bucket failed",
			zap.Error(err),
			zap.String("bucket", s.bucket))
		return false, err
	}

	key := ctx.Expand(s.key)
	exists, err := bucket.IsObjectExist(key)
	if err != nil {
		zap.L().Error("check object exists failed",
			zap.Error(err),
			zap.String("bucket", s.bucket),
			zap.String("key", key))
		return false, err
	}

	_continue := exists
	if !s.toContinue {
		_continue = !_continue
	}

	if s.debug {
		zap.L().Debug("aliyun oss object exists",
			zap.String("bucket", s.bucket),
			zap.String("key", key),
			zap.Bool("continue", _continue))
	}

	return _continue, nil
}
