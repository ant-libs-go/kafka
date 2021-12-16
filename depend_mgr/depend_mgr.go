/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-06-07 13:34:48
# File Name: depend_mgr.go
# Description:
####################################################################### */

package depend_mgr

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ant-libs-go/looper"
	"github.com/ant-libs-go/safe_stop"
	"github.com/ant-libs-go/util"
	"github.com/cihub/seelog"
	rds "github.com/gomodule/redigo/redis"
)

type Options struct {
	frontTopic    string
	checkInterval time.Duration
	delayOffset   int64
	timeout       time.Duration
}

type Option func(o *Options)

func WithCheckInterval(inp time.Duration) Option {
	return func(o *Options) {
		o.checkInterval = inp
	}
}

func WithDelayOffset(inp int64) Option {
	return func(o *Options) {
		o.delayOffset = inp
	}
}

func WithTimeout(inp time.Duration) Option {
	return func(o *Options) {
		o.timeout = inp
	}
}

type DependMgr struct {
	cli  *rds.Pool
	lock sync.RWMutex
	key  string
	opts map[string]*Options
	m    map[string]map[int32]int64 // topic、partition、offset

	loopHandle *looper.Looper
}

func New(cli *rds.Pool, key string) *DependMgr {
	return &DependMgr{
		cli:  cli,
		key:  key,
		opts: map[string]*Options{},
		m:    map[string]map[int32]int64{}}
}

func (this *DependMgr) Add(topic string, frontTopic string, opts ...Option) *DependMgr {
	this.opts[topic] = &Options{frontTopic: frontTopic, checkInterval: 1 * time.Second}
	for _, one := range opts {
		one(this.opts[topic])
	}
	return this
}

func (this *DependMgr) Start() {
	this.loopHandle = looper.New()
	this.loopHandle.AddFunc("loader", time.Second, nil, 0, func() {
		safe_stop.Lock(1)
		defer safe_stop.Unlock()
		this.load()
		this.flush()
	})
	this.loopHandle.Start()
}

func (this *DependMgr) Stop() {
	if this.loopHandle == nil {
		return
	}
	this.loopHandle.Stop()
}

func (this *DependMgr) options(topic string) (r *Options) {
	var ok bool
	if r, ok = this.opts[topic]; !ok {
		r = &Options{}
	}
	return
}

func (this *DependMgr) load() {
	conn := this.cli.Get()
	defer conn.Close()

	vals, err := rds.StringMap(conn.Do("HGETALL", this.key))
	if err != nil {
		seelog.Infof("[DEPEND_MGR] load fail, %s", err)
		return
	}
	seelog.Infof(fmt.Sprintf("[DEPEND_MGR] load: %+v", vals))

	for k, v := range vals {
		t := strings.Split(k, "-")
		topic := t[0]
		partition := util.StrToInt32(t[1], 0)
		offset := util.StrToInt64(v, 0)

		this.MarkTopicOffset(topic, partition, offset)
	}
}

func (this *DependMgr) flush() {
	this.lock.RLock()
	defer this.lock.RUnlock()

	seelog.Infof(fmt.Sprintf("[DEPEND_MGR] flush: %+v", this.m))

	if len(this.m) == 0 {
		return
	}

	d := make([]interface{}, 0, 10)
	d = append(d, this.key)

	for topic, v := range this.m {
		for partition, offset := range v {
			d = append(d, fmt.Sprintf("%s-%d", topic, partition), util.Int64ToStr(offset))
		}
	}

	conn := this.cli.Get()
	defer conn.Close()

	if _, err := conn.Do("HMSET", d...); err != nil {
		seelog.Infof("[DEPEND_MGR] flush fail, %s", err)
		return
	}
}

// unsafe
func (this *DependMgr) markTopicOffset(topic string, partition int32, offset int64) {
	if _, ok := this.m[topic]; !ok {
		this.m[topic] = map[int32]int64{}
	}
	if _, ok := this.m[topic][partition]; !ok {
		this.m[topic][partition] = 0
	}
	this.m[topic][partition] = util.MaxInt64(this.m[topic][partition], offset)
}

func (this *DependMgr) MarkTopicOffset(topic string, partition int32, offset int64) {
	this.lock.Lock()
	defer this.lock.Unlock()

	this.markTopicOffset(topic, partition, offset)
}

func (this *DependMgr) MarkTopicOffsetForAllPartition(topic string, offset int64) {
	this.lock.Lock()
	defer this.lock.Unlock()

	for partition, _ := range this.m[topic] {
		this.markTopicOffset(topic, partition, offset)
	}
}

// unsafe
func (this *DependMgr) getTopicOffset(topic string) (r int64) {
	if _, ok := this.m[topic]; !ok {
		return
	}

	for _, v := range this.m[topic] {
		if r == 0 {
			r = v
			continue
		}
		r = util.MinInt64(r, v)
	}
	return
}

func (this *DependMgr) GetTopicOffset(topic string) (r int64) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	r = this.getTopicOffset(topic)
	return
}

func (this *DependMgr) GetFrontTopicOffset(topic string) (r int64) {
	this.lock.RLock()
	defer this.lock.RUnlock()

	r = this.getTopicOffset(this.options(topic).frontTopic)
	return
}

func (this *DependMgr) Wait(topic string, offset int64) (isTimeout bool) {
	if len(this.options(topic).frontTopic) == 0 {
		return
	}

	// 当超时时间为0时代表不超时，为实现方便设置极值为30天
	timeout := time.NewTimer(util.If(this.options(topic).timeout > 0, this.options(topic).timeout, time.Hour*24*30).(time.Duration))
	defer timeout.Stop()

	timer := time.NewTimer(0)
	defer timer.Stop()

	retry := 0

WHILE:
	select {
	case <-timeout.C:
		isTimeout = true
	case <-timer.C:
		// 当前topic的offset 晚于 前置topic的offset + 需延迟的offset量时，继续等待
		if offset > this.GetFrontTopicOffset(topic)+this.options(topic).delayOffset {
			retry++
			timer.Reset(this.options(topic).checkInterval)
			seelog.Infof("[DEPEND_MGR] %s(%d) it has depend %s(%d) unable to start, wait %s, try %d times",
				topic, offset, this.options(topic).frontTopic, this.GetFrontTopicOffset(topic), this.options(topic).checkInterval, retry)
			goto WHILE
		}
	}
	return
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
