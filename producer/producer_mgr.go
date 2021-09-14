/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-14 21:59:21
# File Name: producer_mgr.go
# Description:
####################################################################### */

package producer

import (
	"fmt"
	"sync"

	"github.com/ant-libs-go/config"
	"github.com/ant-libs-go/config/options"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	once      sync.Once
	lock      sync.RWMutex
	producers map[string]*KafkaProducer
)

func init() {
	producers = map[string]*KafkaProducer{}
}

type kafkaConfig struct {
	Kafka *struct {
		Cfgs map[string]*Cfg `toml:"producer"`
	} `toml:"kafka"`
}

type Cfg struct {
	Addrs             []string `toml:"addrs"`
	Acks              int      `toml:"acks"`                // 等待服务器完成到如何进度在响应
	Topic             string   `toml:"topic"`               // 默认topic.当不指定topic时候使用该值
	Partitioner       string   `toml:"partitioner"`         // 指定分区选择器
	ReturnSuccesses   bool     `toml:"return_successes"`    // 是否等待成功的响应,仅RequireAcks设置不是NoReponse才有效
	ReturnErrors      bool     `toml:"return_errors"`       // 是否等待失败的响应,仅RequireAcks设置不是NoReponse才有效
	ReturnFeedbackNum int      `toml:"return_feedback_num"` // 等待响应的并发数
}

func DefaultProducerPublish(body string, key string, partition int32) (err error) {
	return Publish("default", body, key, partition)
}

func CloseDefaultProducer() {
	CloseProducer("default")
}

func Publish(name string, body string, key string, partition int32) (err error) {
	var producer *KafkaProducer
	if producer, err = SafeProducer(name); err != nil {
		return
	}
	producer.Publish(body, key, partition)
	return
}

func SetSucFeedback(name string, fn func(*kafka.Message, string)) (err error) {
	var producer *KafkaProducer
	if producer, err = SafeProducer(name); err != nil {
		return
	}
	producer.SetSucFeedback(fn)
	return
}

func SetFailFeedback(name string, fn func(*kafka.Message, string)) (err error) {
	var producer *KafkaProducer
	if producer, err = SafeProducer(name); err != nil {
		return
	}
	producer.SetFailFeedback(fn)
	return
}

func Producer(name string) (r *KafkaProducer) {
	var err error
	if r, err = getProducer(name); err != nil {
		panic(err)
	}
	return
}

func SafeProducer(name string) (r *KafkaProducer, err error) {
	return getProducer(name)
}

func CloseProducer(name string) {
	producer, _ := SafeProducer(name)
	if producer == nil {
		return
	}
	producer.Close()
}

func getProducer(name string) (r *KafkaProducer, err error) {
	lock.RLock()
	r = producers[name]
	lock.RUnlock()
	if r == nil {
		r, err = addProducer(name)
	}
	return
}

func addProducer(name string) (r *KafkaProducer, err error) {
	var cfg *Cfg
	if cfg, err = loadCfg(name); err != nil {
		return
	}
	if r, err = NewKafkaProducer(cfg); err != nil {
		return
	}

	lock.Lock()
	producers[name] = r
	lock.Unlock()
	return
}

func loadCfg(name string) (r *Cfg, err error) {
	var cfgs map[string]*Cfg
	if cfgs, err = loadCfgs(); err != nil {
		return
	}
	if r = cfgs[name]; r == nil {
		err = fmt.Errorf("kafka#%s not configed", name)
		return
	}
	return
}

func loadCfgs() (r map[string]*Cfg, err error) {
	r = map[string]*Cfg{}

	once.Do(func() {
		config.Get(&kafkaConfig{}, options.WithOpOnChangeFn(func(cfg interface{}) {
			lock.Lock()
			defer lock.Unlock()
			producers = map[string]*KafkaProducer{}
		}))
	})

	cfg := config.Get(&kafkaConfig{}).(*kafkaConfig)
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
