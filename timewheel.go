package timewheel

import (
	"container/list"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

type failTasks struct {
	sync.Map
}

var ft failTasks

func GetFailTask(tw TimeWheel, key string) (func(), error) {
	if !tw.(*timeWheel).useFailTask {
		return nil, errors.New("fail task not enabled")
	}
	if v, ok := ft.Load(key); ok {
		return v.(func()), nil
	}
	return nil, errors.New("fail task not found")
}

type TimeWheel interface {
	AddTask(task func(), key string, executeTime time.Time)
	RemoveTask(key string)
	Stop()
	StartTimeWheel()
}

type tElement struct {
	key   string
	task  func()
	pos   int
	cycle int
}

type timeWheelConfig struct {
	slot_interval  time.Duration
	slotNum        int
	ticker         *time.Ticker
	stopc          chan struct{}
	addTaskChan    chan *tElement
	removeTaskChan chan string
	slots          []*list.List
	curSlot        int
	keyMap         map[string]*list.Element
	useFailTask    bool
}

type timeWheel struct {
	sync.Once
	timeWheelConfig
}

type Config func(config *timeWheelConfig)

func WithIntervalAndSlotNum(interval time.Duration, slotNum int) Config {
	return func(config *timeWheelConfig) {
		config.slot_interval = interval
		config.slotNum = slotNum
	}
}

func WithFailTask() Config {
	return func(config *timeWheelConfig) {
		config.useFailTask = true
	}
}

func repair(config *timeWheelConfig) {
	if config.slot_interval <= 0 {
		config.slot_interval = time.Second
	}
	if config.slotNum <= 0 {
		config.slotNum = 60
	}

	if config.ticker == nil {
		config.ticker = time.NewTicker(config.slot_interval)
	}

	if config.slots == nil {
		config.slots = make([]*list.List, 0, config.slotNum)
		for i := 0; i < config.slotNum; i++ {
			config.slots = append(config.slots, list.New())
		}
	}

	if config.keyMap == nil {
		config.keyMap = make(map[string]*list.Element)
	}

}

func NewTimeWheel(configs ...Config) TimeWheel {
	tw := &timeWheel{}
	for _, config := range configs {
		config(&tw.timeWheelConfig)
	}
	repair(&tw.timeWheelConfig)

	return tw
}

func (tw *timeWheel) AddTask(task func(), key string, executeTime time.Time) {
	if tw.addTaskChan == nil {
		panic("timeWheel is not started")
	}
	pos, cycle := tw.calculatePosAndCycle(executeTime)
	if cycle < 0 || pos < 0 {
		log.Printf("executeTime is invalid")
		return
	}
	t := &tElement{
		key:   key,
		task:  task,
		pos:   pos,
		cycle: cycle,
	}
	tw.addTaskChan <- t
}

func (tw *timeWheel) calculatePosAndCycle(executeTime time.Time) (int, int) {
	delay := int(time.Until(executeTime))
	cycle := delay / (tw.slotNum * int(tw.slot_interval))
	pos := (tw.curSlot + delay/int(tw.slot_interval)) % tw.slotNum
	return pos, cycle
}

func (tw *timeWheel) RemoveTask(key string) {
	if tw.removeTaskChan == nil {
		panic("timeWheel is not started")
	}
	tw.removeTaskChan <- key
}

func (tw *timeWheel) Stop() {
	if tw.stopc == nil {
		panic("timeWheel is not started")
	}
	tw.Do(func() {
		close(tw.stopc)
		tw.ticker.Stop()
	})
}

func (tw *timeWheel) StartTimeWheel() {
	tw.Do(func() {
		tw.stopc = make(chan struct{})
		tw.addTaskChan = make(chan *tElement)
		tw.removeTaskChan = make(chan string)
		go tw.run()
	})
}

func (tw *timeWheel) onTicker() {
	defer tw.incurSlot()
	l := tw.slots[tw.curSlot]
	tw.executeList(l)
}

func (tw *timeWheel) incurSlot() {
	tw.curSlot = (tw.curSlot + 1) % tw.slotNum
}

func (tw *timeWheel) executeList(l *list.List) {
	for e := l.Front(); e != nil; {
		t := e.Value.(*tElement)
		if t.cycle > 0 {
			t.cycle--
			e = e.Next()
			continue
		}
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("[key:%v task fail]panic: %v", t.key, err)
					if tw.useFailTask {
						ft.Store(t.key, t.task)
					}
				}
			}()
			t.task()
		}()
		next := e.Next()
		l.Remove(e)
		delete(tw.keyMap, t.key)
		e = next
	}
}

func (tw *timeWheel) addTask(t *tElement) error {
	if _, ok := tw.keyMap[t.key]; ok {
		return fmt.Errorf("key %s already exists", t.key)
	}
	list := tw.slots[t.pos]
	tw.keyMap[t.key] = list.PushBack(t)
	return nil
}

func (tw *timeWheel) removeTask(key string) error {
	if e, ok := tw.keyMap[key]; ok {
		t := e.Value.(*tElement)
		list := tw.slots[t.pos]
		list.Remove(e)
		delete(tw.keyMap, key)
		return nil
	}
	return fmt.Errorf("key %s not found", key)
}

func (tw *timeWheel) run() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("timeWheel run panic: %v", err)
		}
	}()

	for {
		select {
		case <-tw.ticker.C:
			tw.onTicker()
		case t := <-tw.addTaskChan:
			error := tw.addTask(t)
			if error != nil {
				log.Printf("add task error: %v", error)
			}
		case key := <-tw.removeTaskChan:
			error := tw.removeTask(key)
			if error != nil {
				log.Printf("remove task error: %v", error)
			}
		case <-tw.stopc:
			return
		}
	}
}
