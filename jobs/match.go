package jobs

import (
	"regexp"

	"go.uber.org/zap"
)

// Match http get html and match regexp
type Match struct {
	content string
	regexp  *regexp.Regexp
	sets    []string
	debug   bool
}

// newMatch create match action
func newMatch(c *Config) (interface{}, error) {
	content, err := c.String("content")
	if err != nil {
		return nil, err
	}

	expression, err := c.String("regexp")
	if err != nil {
		return nil, err
	}

	regex, err := regexp.Compile(expression)
	if err != nil {
		zap.L().Error("compile regex expression failed",
			zap.Error(err),
			zap.String("expression", expression))
		return nil, err
	}

	sets, err := c.Strings("sets")
	if err != nil {
		return nil, err
	}

	debug := c.BoolDefault("debug", false)

	return &Match{
		content: content,
		regexp:  regex,
		sets:    sets,
		debug:   debug,
	}, nil
}

// Do do job
func (s Match) Do(ctx *Context) ([]*Context, error) {
	content := ctx.Expand(s.content)

	groups := s.regexp.FindAllStringSubmatch(content, -1)
	if s.debug {
		zap.L().Debug("match content success",
			zap.String("content", content),
			zap.String("expression", s.regexp.String()),
			zap.Int("matches", len(groups)))
	}

	ctxs := make([]*Context, len(groups))
	for index, group := range groups {
		if len(s.sets) != len(group)-1 {
			return nil, ErrKeyCountInvalid
		}

		cloneCtx := ctx.Clone()
		for keyIndex, key := range s.sets {
			cloneCtx.Set(key, group[keyIndex+1])

			if s.debug {
				zap.L().Debug("set match context success",
					zap.String("key", key),
					zap.String("value", group[keyIndex+1]))
			}
		}

		ctxs[index] = cloneCtx
	}

	return ctxs, nil
}
