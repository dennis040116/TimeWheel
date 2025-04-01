package timewheel

import (
	"testing"
	"time"
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
