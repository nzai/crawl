package jobs

import (
	"os"

	"go.uber.org/zap"
)

// Exists exists external command
type Exists struct {
	path       string
	toContinue bool
	debug      bool
}

// newExists create exists action
func newExists(c *Config) (interface{}, error) {
	path, err := c.String("path")
	if err != nil {
		return nil, err
	}

	_continue, err := c.Bool("continue")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &Exists{
		path:       path,
		toContinue: _continue,
		debug:      debug,
	}, nil
}

// Do do job
func (s Exists) Do(ctx *Context) (bool, error) {
	path := ctx.Expand(s.path)
	_, err := os.Stat(path)
	exists := err == nil

	_continue := exists
	if !s.toContinue {
		_continue = !_continue
	}

	if s.debug {
		status := "file exists"
		if !exists {
			status = "file not exists"
		}

		zap.L().Debug(status,
			zap.String("path", path),
			zap.Bool("exists", err == nil),
			zap.Bool("continue", _continue))
	}

	return _continue, nil
}
