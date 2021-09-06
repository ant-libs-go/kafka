/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-15 13:25:37
# File Name: consumer_mgr.go
# Description:
####################################################################### */

package consumer

import (
	"fmt"
	"sync"

	"github.com/ant-libs-go/config"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	once      sync.Once
	lock      sync.RWMutex
	consumers map[string]*KafkaConsumer
)

func init() {
	consumers = map[string]*KafkaConsumer{}
}

type kafkaConfig struct {
	Kafka *struct {
		Cfgs map[string]*Cfg `toml:"consumer"`
	} `toml:"kafka"`
}

type Cfg struct {
	Addrs            []string `toml:"addrs"`
	Topics           []string `toml:"topics"`
	GroupId          string   `toml:"group_id"`
	ConsumeWorkerNum int      `toml:"consume_worker_num"` // 消费者并发数，参与分区分配，默认1
	ReceiveWorkerNum int      `toml:"receive_worker_num"` // 业务实际并发数，默认10
}

func DefaultConsumerReceive(fn func(string, int, string, *kafka.Message) error) (err error) {
	return Receive("default", fn)
}

func CloseDefaultConsumer() {
	CloseConsumer("default")
}

func Receive(name string, fn func(string, int, string, *kafka.Message) error) (err error) {
	var consumer *KafkaConsumer
	if consumer, err = SafeConsumer(name); err != nil {
		return
	}
	consumer.Receive(fn)
	return
}

func Consumer(name string) (r *KafkaConsumer) {
	var err error
	if r, err = getConsumer(name); err != nil {
		panic(err)
	}
	return
}

func SafeConsumer(name string) (r *KafkaConsumer, err error) {
	return getConsumer(name)
}

func CloseConsumer(name string) {
	consumer, _ := SafeConsumer(name)
	if consumer == nil {
		return
	}
	consumer.Close()
}

func getConsumer(name string) (r *KafkaConsumer, err error) {
	lock.RLock()
	r = consumers[name]
	lock.RUnlock()
	if r == nil {
		r, err = addConsumer(name)
	}
	return
}

func addConsumer(name string) (r *KafkaConsumer, err error) {
	var cfg *Cfg
	if cfg, err = LoadCfg(name); err != nil {
		return
	}
	if r, err = NewKafkaConsumer(cfg); err != nil {
		return
	}

	lock.Lock()
	consumers[name] = r
	lock.Unlock()
	return
}

func LoadCfg(name string) (r *Cfg, err error) {
	var cfgs map[string]*Cfg
	if cfgs, err = LoadCfgs(); err != nil {
		return
	}
	if r = cfgs[name]; r == nil {
		err = fmt.Errorf("kafka#%s not configed", name)
		return
	}
	return
}

func LoadCfgs() (r map[string]*Cfg, err error) {
	r = map[string]*Cfg{}

	cfg := &kafkaConfig{}
	once.Do(func() {
		_, err = config.Load(cfg)
	})

	cfg = config.Get(cfg).(*kafkaConfig)
	if err == nil && (cfg.Kafka == nil || cfg.Kafka.Cfgs == nil || len(cfg.Kafka.Cfgs) == 0) {
		err = fmt.Errorf("not configed")
	}
	if err != nil {
		err = fmt.Errorf("kafka load cfgs error, %s", err)
		return
	}
	r = cfg.Kafka.Cfgs
	return
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
