package main

import (
	"flag"
	"os"
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

	if *rootPath == "." {
		dir, err := os.Getwd()
		if err != nil {
			zap.L().Fatal("get current dir failed", zap.Error(err))
		}

		*rootPath = dir
	}

	zap.L().Info("arguments parse success",
		zap.String("jobPath", *jobPath),
		zap.String("rootPath", *rootPath))

	start := time.Now()

	_jobs, err := jobs.ReadFile(*jobPath)
	if err != nil {
		zap.L().Fatal("read job file failed", zap.Error(err), zap.String("path", *jobPath))
	}

	ctx := jobs.NewContextFromEnv(*rootPath)

	for _, job := range _jobs {
		err = job.Execute(ctx)
		if err != nil {
			zap.L().Fatal("do job failed", zap.Error(err))
		}
	}

	zap.L().Info("crawl success", zap.Duration("in", time.Now().Sub(start)))
}
