package timewheel

import (
	thttp "TimeWheel/package/http"
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func Test_timeWheel(t *testing.T) {
	timeWheel := NewTimeWheel(WithIntervalAndSlotNum(500*time.Millisecond, 10))
	timeWheel.StartTimeWheel()
	defer timeWheel.Stop()

	t.Errorf("test1 add time, %v", time.Now())
	t.Errorf("test1 execute time, %v", time.Now().Add(time.Second))
	timeWheel.AddTask(func() {
		t.Errorf("test1, %v", time.Now())
	}, "test1", time.Now().Add(time.Second))

	timeWheel.AddTask(func() {
		t.Errorf("test1, %v", time.Now())
	}, "test1", time.Now().Add(5*time.Second))

	t.Errorf("test2 add time, %v", time.Now())
	t.Errorf("test2 execute time, %v", time.Now().Add(3*time.Second))
	timeWheel.AddTask(func() {
		t.Errorf("test2, %v", time.Now())
	}, "test2", time.Now().Add(3*time.Second))

	<-time.After(6 * time.Second)
}

var (
	callbackURL     = "url"
	callbackMethod  = "method"
	callbackReq     = "req"
	callbackHeaders = map[string]string{
		"key": "value",
	}
)

func Test_rTimeWheel_AddTask(t *testing.T) {
	rtw := NewRTimeWheel(thttp.NewClient(), redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // 没有密码，默认值
		DB:       0,  // 默认DB 0
	}))
	defer rtw.Stop()

	err := rtw.AddTask(context.Background(), callbackURL, callbackMethod, "test1", callbackReq, callbackHeaders, time.Now().Add(time.Second))
	if err != nil {
		t.Errorf("add task failed: %v", err)
	}
}
