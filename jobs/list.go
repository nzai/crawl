package jobs

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"go.uber.org/zap"
)

// List list files
type List struct {
	path      string
	pattern   string
	recursive bool
	pathSet   string
	nameSet   string
	parallel  int
	files     []string
	debug     bool
}

// newList create list action
func newList(c *Config) (interface{}, error) {
	path, err := c.String("path")
	if err != nil {
		return nil, err
	}

	pattern, err := c.String("pattern")
	if err != nil {
		return nil, err
	}

	recursive := c.BoolDefault("recursive", false)

	pathSet, err := c.String("path_set")
	if err != nil {
		return nil, err
	}

	nameSet, err := c.String("name_set")
	if err != nil {
		return nil, err
	}

	parallel := c.IntDefault("parallel", 0)

	debug := c.BoolDefault("debug", false)

	return &List{
		path:      path,
		pattern:   pattern,
		recursive: recursive,
		pathSet:   pathSet,
		nameSet:   nameSet,
		parallel:  parallel,
		debug:     debug,
	}, nil
}

// Do do job
func (s List) Do(ctx *Context) ([]*Context, error) {
	dir := ctx.Expand(s.path)
	var err error
	if s.recursive {
		err = s.work(dir)
	} else {
		err = s.glob(dir)
	}

	if err != nil {
		return nil, err
	}

	ctxs := make([]*Context, len(s.files))
	for index, file := range s.files {
		cloneCtx := ctx.Clone()
		cloneCtx.Set(s.pathSet, file)
		cloneCtx.Set(s.nameSet, filepath.Base(file))

		ctxs[index] = cloneCtx
	}

	return ctxs, nil
}

func (s *List) glob(dir string) error {
	files, err := filepath.Glob(filepath.Join(dir, s.pattern))
	if err != nil {
		zap.L().Warn("glob dir failed",
			zap.Error(err),
			zap.String("dir", dir))
		return err
	}

	if s.debug {
		zap.L().Debug("glob dir success",
			zap.String("dir", dir),
			zap.String("pattern", s.pattern),
			zap.Int("matches", len(files)),
			zap.Strings("files", files))
	}

	s.files = append(s.files, files...)

	return nil
}

func (s *List) work(dir string) error {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			zap.L().Warn("walk dir failed",
				zap.Error(err),
				zap.String("dir", dir))
			return err
		}

		if info.IsDir() {
			return nil
		}

		match, err := filepath.Match(s.pattern, info.Name())
		if err != nil {
			zap.L().Warn("match file failed",
				zap.Error(err),
				zap.String("dir", dir),
				zap.String("name", info.Name()),
				zap.String("pattern", s.pattern))
			return err
		}

		if !match {
			return nil
		}

		s.files = append(s.files, path)

		if s.debug {
			zap.L().Debug("find file",
				zap.String("path", path),
				zap.String("file", info.Name()))
		}

		return nil
	})
	if err != nil {
		zap.L().Error("work dir failed",
			zap.Error(err),
			zap.String("dir", dir))
		return err
	}

	return nil
}

// ListDir list dirs
type ListDir struct {
	path      string
	pattern   string
	recursive bool
	pathSet   string
	nameSet   string
	parallel  int
	dirs      []string
	debug     bool
}

// newListDir create list dir action
func newListDir(c *Config) (interface{}, error) {
	path, err := c.String("path")
	if err != nil {
		return nil, err
	}

	pattern, err := c.String("pattern")
	if err != nil {
		return nil, err
	}

	recursive := c.BoolDefault("recursive", false)

	pathSet, err := c.String("path_set")
	if err != nil {
		return nil, err
	}

	nameSet, err := c.String("name_set")
	if err != nil {
		return nil, err
	}

	parallel := c.IntDefault("parallel", 0)

	debug := c.BoolDefault("debug", false)

	return &ListDir{
		path:      path,
		pattern:   pattern,
		recursive: recursive,
		pathSet:   pathSet,
		nameSet:   nameSet,
		parallel:  parallel,
		debug:     debug,
	}, nil
}

// Do do job
func (s ListDir) Do(ctx *Context) ([]*Context, error) {
	dir := ctx.Expand(s.path)
	var err error
	if s.recursive {
		err = s.work(dir)
	} else {
		err = s.glob(dir)
	}

	if err != nil {
		return nil, err
	}

	ctxs := make([]*Context, len(s.dirs))
	for index, file := range s.dirs {
		cloneCtx := ctx.Clone()
		cloneCtx.Set(s.pathSet, file)
		cloneCtx.Set(s.nameSet, filepath.Base(file))

		ctxs[index] = cloneCtx
	}

	return ctxs, nil
}

func (s *ListDir) glob(dir string) error {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		zap.L().Warn("read dir failed",
			zap.Error(err),
			zap.String("dir", dir))
		return err
	}

	dirs := make([]string, 0, len(fis))
	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}

		match, err := filepath.Match(s.pattern, fi.Name())
		if err != nil {
			zap.L().Warn("match dir failed",
				zap.Error(err),
				zap.String("dir", dir),
				zap.String("name", fi.Name()),
				zap.String("pattern", s.pattern))
			return err
		}

		if !match {
			continue
		}

		_dir := filepath.Join(dir, fi.Name())
		if s.debug {
			zap.L().Debug("find dir success",
				zap.String("dir", _dir))
		}

		dirs = append(dirs, _dir)
	}

	s.dirs = append(s.dirs, dirs...)

	return nil
}

func (s *ListDir) work(dir string) error {
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			zap.L().Warn("walk dir failed",
				zap.Error(err),
				zap.String("dir", dir))
			return err
		}

		if !info.IsDir() {
			return nil
		}

		match, err := filepath.Match(s.pattern, info.Name())
		if err != nil {
			zap.L().Warn("match dir failed",
				zap.Error(err),
				zap.String("dir", dir),
				zap.String("name", info.Name()),
				zap.String("pattern", s.pattern))
			return err
		}

		if !match {
			return nil
		}

		s.dirs = append(s.dirs, path)

		if s.debug {
			zap.L().Debug("find dir",
				zap.String("path", path),
				zap.String("dir", info.Name()))
		}

		return nil
	})
	if err != nil {
		zap.L().Error("work dir failed",
			zap.Error(err),
			zap.String("dir", dir))
		return err
	}

	return nil
}
