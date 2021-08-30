/* ######################################################################
# Author: (zhengfei@dianzhong.com)
# Created Time: 2021-06-07 13:34:48
# File Name: logics/depend_mgr/depend_mgr.go
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

const DEPEND_KEY = "union_event_tracing_depend"

type DependMgr struct {
	m          map[string]map[int32]int64
	cli        rds.Pool
	lock       sync.RWMutex
	relations  map[string]string
	loopHandle *looper.Looper
}

func New(cli rds.Pool, relations map[string]string) *DependMgr {
	o := &DependMgr{}
	o.cli = cli
	o.m = map[string]map[int32]int64{}
	o.relations = relations
	return o
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

func (this *DependMgr) load() {
	conn := this.cli.Get()
	defer conn.Close()

	vals, err := rds.StringMap(conn.Do("HGETALL", DEPEND_KEY))
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
	d = append(d, DEPEND_KEY)

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

func (this *DependMgr) MarkTopicOffset(topic string, partition int32, offset int64) {
	this.lock.Lock()
	defer this.lock.Unlock()

	if _, ok := this.m[topic]; !ok {
		this.m[topic] = map[int32]int64{}
	}
	if _, ok := this.m[topic][partition]; !ok {
		this.m[topic][partition] = 0
	}
	this.m[topic][partition] = util.MaxInt64(this.m[topic][partition], offset)
}

func (this *DependMgr) GetFrontTopicOffset(topic string) (r int64) {
	r = time.Now().UnixNano()

	frontTopic := this.relations[topic]
	if len(frontTopic) == 0 {
		return
	}

	this.lock.RLock()
	defer this.lock.RUnlock()

	if _, ok := this.m[frontTopic]; !ok {
		return
	}

	for _, v := range this.m[frontTopic] {
		r = util.MinInt64(r, v)
	}
	return
}

func (this *DependMgr) Wait(topic string, offset int64) {
	for {
		frontTopicOffset := this.GetFrontTopicOffset(topic)
		if offset < frontTopicOffset {
			break
		}
		seelog.Infof("topic#%s(%d) it has depend(%d) unable to start, wait 5 second", topic, offset, frontTopicOffset)
		time.Sleep(5 * time.Second)
	}
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
