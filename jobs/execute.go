package jobs

import (
	"os"
	"os/exec"

	"go.uber.org/zap"
)

// Execute execute external command
type Execute struct {
	command string
	args    []string
	dir     string
	debug   bool
}

// newExecute create execute action
func newExecute(c *Config) (*Execute, error) {
	command, err := c.String("command")
	if err != nil {
		return nil, err
	}

	args, err := c.Strings("args")
	if err != nil {
		return nil, err
	}

	dir := c.StringDefault("dir", "")

	debug := c.BoolDefault("debug", false)

	return &Execute{
		command: command,
		args:    args,
		dir:     dir,
		debug:   debug,
	}, nil
}

// Do do job
func (s Execute) Do(ctx *Context) error {
	args := make([]string, len(s.args))
	for index, arg := range s.args {
		args[index] = ctx.Expand(arg)
	}

	dir := ctx.Expand(s.dir)

	if s.debug {
		zap.L().Debug("execute command",
			zap.String("command", s.command),
			zap.Strings("args", args),
			zap.String("dir", dir))
	}

	cmd := exec.Command(s.command, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Dir = dir

	err := cmd.Run()
	if err != nil {
		ee, ok := err.(*exec.ExitError)
		if !ok {
			zap.L().Error("execute command failed",
				zap.Error(err),
				// zap.ByteString("output", output),
				zap.String("command", s.command),
				zap.Strings("args", args))
			return err
		}

		zap.L().Error("execute command failed",
			zap.Error(ee),
			// zap.ByteString("output", output),
			zap.String("command", s.command),
			zap.Strings("args", args))
		return err
	}

	if s.debug {
		zap.L().Debug("execute command success",
			zap.String("command", s.command),
			zap.Strings("args", args),
			zap.String("dir", dir))
	}

	return nil
}
