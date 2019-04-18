package main

import (
	"fmt"
	"os"
	"time"

	"github.com/nzai/crawl/jobs"
	"go.uber.org/zap"
)

func main() {
	c := zap.NewDevelopmentConfig()
	c.DisableStacktrace = true

	logger, _ := c.Build()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	if len(os.Args) < 2 {
		fmt.Println("usage:\n\tcrawl your_job_config.toml")
		os.Exit(1)
	}

	rootPath, err := os.Getwd()
	if err != nil {
		zap.L().Fatal("get current dir failed", zap.Error(err))
	}

	jobPath := os.Args[1]

	zap.L().Info("arguments parse success",
		zap.String("jobPath", jobPath),
		zap.String("rootPath", rootPath))

	start := time.Now()

	_jobs, err := jobs.ReadFile(jobPath)
	if err != nil {
		zap.L().Fatal("read job file failed", zap.Error(err), zap.String("path", jobPath))
	}

	ctx := jobs.NewContextFromEnv(rootPath)

	for _, job := range _jobs {
		err = job.Execute(ctx)
		if err != nil {
			zap.L().Fatal("do job failed", zap.Error(err))
		}
	}

	zap.L().Info("crawl success", zap.Duration("in", time.Now().Sub(start)))
}
