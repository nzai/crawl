package jobs

import (
	"errors"
	"reflect"
	"sync"

	"go.uber.org/zap"
)

var (
	// ErrInvalidAction invalid action
	ErrInvalidAction = errors.New("invalid action")
)

// Job crawl job
type Job struct {
	Action interface{}
	Jobs   []*Job
}

// Execute execute job
func (s Job) Execute(ctx *Context) error {
	switch s.Action.(type) {
	case SingleContextAction:
		return s.executeSingleContextAction(ctx)
	case MultipleContextAction:
		return s.executeMultipleContextAction(ctx)
	case ConditionContextAction:
		return s.executeConditionContextAction(ctx)
	default:
		zap.L().Error("invalid action", zap.String("type", reflect.TypeOf(s.Action).String()))
		return ErrInvalidAction
	}
}

func (s Job) executeSingleContextAction(ctx *Context) error {
	action := s.Action.(SingleContextAction)
	err := action.Do(ctx)
	if err != nil {
		return err
	}

	for _, job := range s.Jobs {
		err = job.Execute(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s Job) executeMultipleContextAction(ctx *Context) error {
	action := s.Action.(MultipleContextAction)
	ctxs, err := action.Do(ctx)
	if err != nil {
		return err
	}

	parallel := ctx.IntDefault("parallel", 0)

	for _, job := range s.Jobs {
		ch := make(chan bool, parallel)
		wg := new(sync.WaitGroup)
		wg.Add(len(ctxs))

		for _, _ctx := range ctxs {
			go func(c *Context) {
				err = job.Execute(c)
				if err != nil {
					zap.L().Error("do job failed", zap.Error(err))
				}

				<-ch
				wg.Done()
			}(_ctx)

			ch <- true
		}

		wg.Wait()
	}

	return nil
}

func (s Job) executeConditionContextAction(ctx *Context) error {
	action := s.Action.(ConditionContextAction)
	_continue, err := action.Do(ctx)
	if err != nil {
		return err
	}

	if !_continue {
		return nil
	}

	for _, job := range s.Jobs {
		err = job.Execute(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
