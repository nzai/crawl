package jobs

import (
	"errors"
	"strconv"
	"strings"

	"go.uber.org/zap"
)

var (
	// ErrInvalidRangeExpression invalid range expression
	ErrInvalidRangeExpression = errors.New("invalid range expression")
)

// Range for range
type Range struct {
	start string
	end   string
	set   string
	debug bool
}

// newRange create range action
func newRange(c *Config) (*Range, error) {
	expression, err := c.String("expression")
	if err != nil {
		return nil, err
	}

	pos := strings.Index(expression, "-")
	if pos < 0 {
		return nil, ErrInvalidRangeExpression
	}

	start := expression[:pos]
	end := expression[pos+1:]

	set, err := c.String("set")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &Range{
		start: start,
		end:   end,
		set:   set,
		debug: debug,
	}, nil
}

// Do do job
func (s Range) Do(ctx *Context) ([]*Context, error) {
	start, err := strconv.Atoi(ctx.Expand(s.start))
	if err != nil {
		return nil, ErrInvalidRangeExpression
	}

	end, err := strconv.Atoi(ctx.Expand(s.end))
	if err != nil {
		return nil, ErrInvalidRangeExpression
	}

	if start > end {
		return nil, nil
	}

	if s.debug {
		zap.L().Debug("range expand success", zap.Int("start", start), zap.Int("end", end))
	}

	ctxs := make([]*Context, 0, end-start+1)
	for index := start; index <= end; index++ {
		cloneCtx := ctx.Clone()
		cloneCtx.Set(s.set, strconv.Itoa(index))
		ctxs = append(ctxs, cloneCtx)
	}

	return ctxs, nil
}
