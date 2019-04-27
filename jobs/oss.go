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
func newOssExists(c *Config) (interface{}, error) {
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
		status := "aliyun oss object exists"
		if !exists {
			status = "aliyun oss object not exists"
		}

		zap.L().Debug(status,
			zap.String("bucket", s.bucket),
			zap.String("key", key),
			zap.Bool("continue", _continue))
	}

	return _continue, nil
}

// OssUpload ossUpload external command
type OssUpload struct {
	endPoint  string
	keyID     string
	keySecret string
	bucket    string
	key       string
	path      string
	debug     bool
}

// newOssUpload create ossUpload action
func newOssUpload(c *Config) (interface{}, error) {
	path, err := c.String("path")
	if err != nil {
		return nil, err
	}

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

	debug := c.BoolDefault("debug", false)

	return &OssUpload{
		path:      path,
		endPoint:  endPoint,
		keyID:     keyID,
		keySecret: keySecret,
		bucket:    bucket,
		key:       key,
		debug:     debug,
	}, nil
}

// Do do job
func (s OssUpload) Do(ctx *Context) error {
	client, err := oss.New(s.endPoint, s.keyID, s.keySecret)
	if err != nil {
		zap.L().Error("create new aliyun oss client failed",
			zap.Error(err),
			zap.String("endpoint", s.endPoint),
			zap.String("accessKeyID", s.keyID),
			zap.String("accessKeySecret", s.keySecret))
		return err
	}

	bucket, err := client.Bucket(s.bucket)
	if err != nil {
		zap.L().Error("get aliyun oss bucket failed",
			zap.Error(err),
			zap.String("bucket", s.bucket))
		return err
	}

	key := ctx.Expand(s.key)
	path := ctx.Expand(s.path)

	err = bucket.UploadFile(key, path, 1024*1024)
	if err != nil {
		zap.L().Error("upload file to aliyun oss bucket failed",
			zap.Error(err),
			zap.String("path", path),
			zap.String("bucket", s.bucket),
			zap.String("key", key))
		return err
	}

	if s.debug {
		zap.L().Debug("upload file to aliyun oss bucket success",
			zap.String("path", path),
			zap.String("bucket", s.bucket),
			zap.String("key", key))
	}

	return nil
}

// OssDownload ossDownload external command
type OssDownload struct {
	endPoint  string
	keyID     string
	keySecret string
	bucket    string
	key       string
	path      string
	debug     bool
}

// newOssDownload create ossDownload action
func newOssDownload(c *Config) (interface{}, error) {
	path, err := c.String("path")
	if err != nil {
		return nil, err
	}

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

	debug := c.BoolDefault("debug", false)

	return &OssDownload{
		path:      path,
		endPoint:  endPoint,
		keyID:     keyID,
		keySecret: keySecret,
		bucket:    bucket,
		key:       key,
		debug:     debug,
	}, nil
}

// Do do job
func (s OssDownload) Do(ctx *Context) error {
	client, err := oss.New(s.endPoint, s.keyID, s.keySecret)
	if err != nil {
		zap.L().Error("create new aliyun oss client failed",
			zap.Error(err),
			zap.String("endpoint", s.endPoint),
			zap.String("accessKeyID", s.keyID),
			zap.String("accessKeySecret", s.keySecret))
		return err
	}

	bucket, err := client.Bucket(s.bucket)
	if err != nil {
		zap.L().Error("get aliyun oss bucket failed",
			zap.Error(err),
			zap.String("bucket", s.bucket))
		return err
	}

	key := ctx.Expand(s.key)
	path := ctx.Expand(s.path)

	err = bucket.GetObjectToFile(key, path)
	if err != nil {
		zap.L().Error("download file from aliyun oss bucket failed",
			zap.Error(err),
			zap.String("path", path),
			zap.String("bucket", s.bucket),
			zap.String("key", key))
		return err
	}

	if s.debug {
		zap.L().Debug("download file from aliyun oss bucket success",
			zap.String("path", path),
			zap.String("bucket", s.bucket),
			zap.String("key", key))
	}

	return nil
}
