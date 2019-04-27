package jobs

import (
	"strings"

	"go.uber.org/zap"
)

// Replace replace external command
type Replace struct {
	expression string
	old        string
	new        string
	set        string
	debug      bool
}

// newReplace create replace action
func newReplace(c *Config) (interface{}, error) {
	expression, err := c.String("expression")
	if err != nil {
		return nil, err
	}

	old, err := c.String("old")
	if err != nil {
		return nil, err
	}

	new, err := c.String("new")
	if err != nil {
		return nil, err
	}

	set, err := c.String("set")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &Replace{
		expression: expression,
		old:        old,
		new:        new,
		set:        set,
		debug:      debug,
	}, nil
}

// Do do job
func (s Replace) Do(ctx *Context) error {
	expression := ctx.Expand(s.expression)
	newExpression := strings.Replace(expression, s.old, s.new, -1)

	ctx.Set(s.set, newExpression)

	if s.debug {
		zap.L().Debug("replaced",
			zap.String("expression", expression),
			zap.String("newExpression", newExpression),
			zap.String("key", s.set))
	}

	return nil
}
