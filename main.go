package main

import (
	"flag"
	"time"

	"github.com/nzai/crawl/jobs"
	"go.uber.org/zap"
)

var (
	jobPath  = flag.String("job", "job.toml", "job toml file")
	rootPath = flag.String("root", ".", "download root dir")
)

func main() {
	c := zap.NewDevelopmentConfig()
	c.DisableStacktrace = true

	logger, _ := c.Build()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	flag.Parse()

	start := time.Now()

	_jobs, err := jobs.ReadFile(*jobPath)
	if err != nil {
		zap.L().Fatal("read job file failed", zap.Error(err), zap.String("path", *jobPath))
	}

	ctx := jobs.NewContextFromEnv()

	for _, job := range _jobs {
		err = job.Do(ctx)
		if err != nil {
			zap.L().Fatal("do job failed", zap.Error(err))
		}
	}

	zap.L().Info("crawl success", zap.Duration("in", time.Now().Sub(start)))
}
