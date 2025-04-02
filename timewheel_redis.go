package timewheel

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	thttp "TimeWheel/package/http"
	"TimeWheel/package/util"

	"github.com/redis/go-redis/v9"
)

type RTimeWheel interface {
	AddTask(ctx context.Context, url, method, key string, req interface{}, headers map[string]string, executedTime time.Time) error
	RemoveTask(ctx context.Context, key string) error
	Stop()
}

type RTask struct {
	CallbackURL     string            `json:"callback_url"`
	CallbackMethod  string            `json:"callback_method"`
	CallbackReq     interface{}       `json:"callback_req"`
	CallbackHeaders map[string]string `json:"callback_headers"`
	Key             string            `json:"key"`
}

type rTimeWheel struct {
	sync.Once
	httpClient  *thttp.Client
	redisClient *redis.Client
	stopc       chan struct{}
	ticker      *time.Ticker
}

func NewRTimeWheel(httpClient *thttp.Client, redisClient *redis.Client) RTimeWheel {
	rwt := &rTimeWheel{
		httpClient:  httpClient,
		redisClient: redisClient,
		stopc:       make(chan struct{}),
		ticker:      time.NewTicker(time.Second),
	}
	go rwt.run()
	return rwt
}

func (rtw *rTimeWheel) run() {
	for {
		select {
		case <-rtw.ticker.C:
			go rtw.excuteTasks()
		case <-rtw.stopc:
			return
		}
	}
}

func (rtw *rTimeWheel) PreCheckTask(task *RTask) error {
	if !strings.HasPrefix(task.CallbackURL, "http://") || !strings.HasPrefix(task.CallbackURL, "https://") {
		return fmt.Errorf("invalid callback url: %s", task.CallbackURL)
	}
	if task.CallbackMethod != "GET" && task.CallbackMethod != "POST" {
		return fmt.Errorf("invalid callback method: %s", task.CallbackMethod)
	}
	return nil
}

func (rtw *rTimeWheel) excuteTasks() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("panic: %v\n", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tasks, err := rtw.getTaskList(ctx)
	if err != nil {
		log.Printf("get task list failed: %v", err)
		return
	}

	var wg sync.WaitGroup
	for _, task := range tasks {
		wg.Add(1)
		go func(task *RTask) {
			defer func() {
				if err := recover(); err != nil {
					fmt.Printf("panic: %v\n", err)
				}
				wg.Done()
			}()
			if err := rtw.excuteTask(ctx, task); err != nil {
				log.Printf("excute task failed: %v", err)
			}
		}(task)
	}
	wg.Wait()
}

func (rtw *rTimeWheel) excuteTask(ctx context.Context, task *RTask) error {
	return rtw.httpClient.JSONDo(ctx, task.CallbackMethod, task.CallbackURL, task.CallbackHeaders, task.CallbackReq, nil)
}

func (rtw *rTimeWheel) getTaskList(ctx context.Context) ([]*RTask, error) {
	tasks := make([]*RTask, 0)
	now := time.Now()
	minuteSetKey := rtw.getMinuteSetKey(now)
	deleteSetKey := rtw.getDeleteSetKey(now)
	nowSecond := util.GetTimeSecond(now)
	score1 := string(nowSecond.Unix())
	score2 := string(nowSecond.Add(time.Second).Unix())
	reply, err := rtw.redisClient.Eval(ctx, LuaZrangeTasks, []string{minuteSetKey, deleteSetKey, score1, score2}).Result()
	if err != nil {
		return nil, fmt.Errorf("get task list failed: %v", err)
	}

	replyList, ok := reply.([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid reply type: %T", reply)
	}

	deleteSet, ok := replyList[0].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid delete set type: %T", replyList[0])
	}

	ds := make(map[string]struct{}, len(deleteSet))
	for _, v := range deleteSet {
		key, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("invalid delete key type: %T", v)
		}
		ds[key] = struct{}{}
	}

	for i := 1; i < len(replyList); i++ {
		taskJson, ok := replyList[i].(string)
		if !ok {
			return nil, fmt.Errorf("invalid task json type: %T", replyList[i])
		}

		task := &RTask{}
		if err := json.Unmarshal([]byte(taskJson), task); err != nil {
			log.Printf("unmarshal task failed: %v", err)
			continue
		}

		if _, ok := ds[task.Key]; ok {
			continue
		}

		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (rtw *rTimeWheel) AddTask(ctx context.Context, url, method, key string, req interface{}, headers map[string]string, executedTime time.Time) error {
	task := &RTask{
		CallbackURL:     url,
		CallbackMethod:  method,
		CallbackReq:     req,
		CallbackHeaders: headers,
		Key:             key,
	}
	if err := rtw.PreCheckTask(task); err != nil {
		return err
	}

	taskJson, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task failed: %v", err)
	}

	_, err = rtw.redisClient.Eval(ctx, LuaAddTasks, []string{
		rtw.getMinuteSetKey(executedTime),
		rtw.getDeleteSetKey(executedTime),
		string(executedTime.Unix()),
		string(taskJson),
		key,
	}).Result()

	if err != nil {
		return fmt.Errorf("add task failed: %v", err)
	}

	return nil
}

func (rtw *rTimeWheel) getMinuteSetKey(t time.Time) string {
	return fmt.Sprintf("timewheel_{%s}", util.GetTimeMinuteStr(t))
}

func (rtw *rTimeWheel) getDeleteSetKey(t time.Time) string {
	return fmt.Sprintf("timewheel_{%s}", util.GetTimeMinuteStr(t))
}

func (rtw *rTimeWheel) RemoveTask(ctx context.Context, key string) error {
	_, err := rtw.redisClient.Eval(ctx, LuaDeleteTask, []string{
		rtw.getDeleteSetKey(time.Now()),
		key,
	}).Result()
	if err != nil {
		return fmt.Errorf("remove task failed: %v", err)
	}
	return nil
}

func (rtw *rTimeWheel) Stop() {
	rtw.Once.Do(func() {
		close(rtw.stopc)
		rtw.ticker.Stop()
	})
}
