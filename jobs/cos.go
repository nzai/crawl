package jobs

import (
	"context"
	"net/http"
	"net/url"
	"time"

	"github.com/tencentyun/cos-go-sdk-v5"
	"go.uber.org/zap"
)

// CosExists cosExists external command
type CosExists struct {
	endPoint   string
	keyID      string
	keySecret  string
	key        string
	toContinue bool
	debug      bool
}

// newCosExists create cosExists action
func newCosExists(c *Config) (interface{}, error) {
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

	key, err := c.String("key")
	if err != nil {
		return nil, err
	}

	_continue, err := c.Bool("continue")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &CosExists{
		endPoint:   endPoint,
		keyID:      keyID,
		keySecret:  keySecret,
		key:        key,
		toContinue: _continue,
		debug:      debug,
	}, nil
}

// Do do job
func (s CosExists) Do(ctx *Context) (bool, error) {
	u, _ := url.Parse(s.endPoint)
	client := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Timeout: 30 * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID:  s.keyID,
			SecretKey: s.keySecret,
		},
	})

	exists := false
	key := ctx.Expand(s.key)
	response, err := client.Object.Head(context.TODO(), key, nil)
	if err != nil {
		e, ok := err.(*cos.ErrorResponse)
		if !ok {
			zap.L().Error("head object failed",
				zap.Error(err),
				zap.String("endPoint", s.endPoint),
				zap.String("key", key))
			return false, err
		}

		switch e.Response.StatusCode {
		case http.StatusOK:
			exists = true
		case http.StatusNotFound:
			exists = false
		default:
			zap.L().Error("head object failed",
				zap.Error(err),
				zap.String("endPoint", s.endPoint),
				zap.String("key", key))
			return false, err
		}
	}
	defer response.Body.Close()

	_continue := exists
	if !s.toContinue {
		_continue = !_continue
	}

	if s.debug {
		status := "tencent cloud cos object exists"
		if !exists {
			status = "tencent cloud cos object not exists"
		}

		zap.L().Debug(status,
			zap.String("endPoint", s.endPoint),
			zap.String("key", key),
			zap.Bool("continue", _continue))
	}

	return _continue, nil
}

// CosUpload cosUpload external command
type CosUpload struct {
	endPoint  string
	keyID     string
	keySecret string
	key       string
	path      string
	debug     bool
}

// newCosUpload create cosUpload action
func newCosUpload(c *Config) (interface{}, error) {
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

	key, err := c.String("key")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &CosUpload{
		path:      path,
		endPoint:  endPoint,
		keyID:     keyID,
		keySecret: keySecret,
		key:       key,
		debug:     debug,
	}, nil
}

// Do do job
func (s CosUpload) Do(ctx *Context) error {
	u, _ := url.Parse(s.endPoint)
	client := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Timeout: 30 * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID:  s.keyID,
			SecretKey: s.keySecret,
		},
	})

	key := ctx.Expand(s.key)
	path := ctx.Expand(s.path)

	_, response, err := client.Object.Upload(context.TODO(), key, path, nil)
	if err != nil {
		zap.L().Error("upload file to tencent cloud cos bucket failed",
			zap.Error(err),
			zap.String("path", path),
			zap.String("endPoint", s.endPoint),
			zap.String("key", key))
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		zap.L().Error("upload file to tencent cloud cos bucket failed",
			zap.String("path", path),
			zap.String("endPoint", s.endPoint),
			zap.String("key", key),
			zap.Int("status code", response.StatusCode),
			zap.String("status text", response.Status))
	}

	if s.debug {
		zap.L().Debug("upload file to tencent cloud cos bucket success",
			zap.String("path", path),
			zap.String("endPoint", s.endPoint),
			zap.String("key", key))
	}

	return nil
}

// CosDownload cosDownload external command
type CosDownload struct {
	endPoint  string
	keyID     string
	keySecret string
	key       string
	path      string
	debug     bool
}

// newCosDownload create cosDownload action
func newCosDownload(c *Config) (interface{}, error) {
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

	key, err := c.String("key")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &CosDownload{
		path:      path,
		endPoint:  endPoint,
		keyID:     keyID,
		keySecret: keySecret,
		key:       key,
		debug:     debug,
	}, nil
}

// Do do job
func (s CosDownload) Do(ctx *Context) error {
	u, _ := url.Parse(s.endPoint)
	client := cos.NewClient(&cos.BaseURL{BucketURL: u}, &http.Client{
		Timeout: 30 * time.Second,
		Transport: &cos.AuthorizationTransport{
			SecretID:  s.keyID,
			SecretKey: s.keySecret,
		},
	})

	key := ctx.Expand(s.key)
	path := ctx.Expand(s.path)

	response, err := client.Object.GetToFile(context.TODO(), key, path, nil)
	if err != nil {
		zap.L().Error("download file from tencent cloud cos bucket failed",
			zap.Error(err),
			zap.String("path", path),
			zap.String("endPoint", s.endPoint),
			zap.String("key", key))
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		zap.L().Error("download file from tencent cloud cos bucket failed",
			zap.String("path", path),
			zap.String("endPoint", s.endPoint),
			zap.String("key", key),
			zap.Int("status code", response.StatusCode),
			zap.String("status text", response.Status))
	}

	if s.debug {
		zap.L().Debug("download file from tencent cloud cos bucket success",
			zap.String("path", path),
			zap.String("endPoint", s.endPoint),
			zap.String("key", key))
	}

	return nil
}
