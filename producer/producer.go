/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-14 16:49:22
# File Name: producer.go
# Description:
####################################################################### */

package producer

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/ant-libs-go/util"
	"github.com/cihub/seelog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducer struct {
	cfg          *Cfg
	instance     *kafka.Producer
	partitioner  Partitioner
	numPartition int32
	sucFeedback  func(*kafka.Message, string)
	failFeedback func(*kafka.Message, string)
}

var (
	DefaultSucFeedbackFn = func(suc *kafka.Message, body string) {
		seelog.Infof("[KAFKA PRODUCER] publish success: %s, message: %s", suc.TopicPartition.String(), body)
	}
	DefaultFailFeedbackFn = func(fail *kafka.Message, body string) {
		seelog.Errorf("[KAFKA PRODUCER] publish fail: %s, message: %s", fail.TopicPartition.String(), body)
	}
)

func NewKafkaProducer(cfg *Cfg) (r *KafkaProducer, err error) {
	r = &KafkaProducer{cfg: cfg}
	r.partitioner = r.parsePartitioner()()

	// see: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	kcfg := &kafka.ConfigMap{}
	kcfg.SetKey("api.version.request", "true")
	kcfg.SetKey("acks", cfg.Acks)
	kcfg.SetKey("retries", 3)
	kcfg.SetKey("retry.backoff.ms", 1000)
	kcfg.SetKey("linger.ms", 10)
	kcfg.SetKey("security.protocol", "plaintext")
	kcfg.SetKey("message.max.bytes", 1048576)
	kcfg.SetKey("bootstrap.servers", strings.Join(cfg.Addrs, ","))

	if r.instance, err = kafka.NewProducer(kcfg); err != nil {
		return
	}

	var mgrCli *kafka.AdminClient
	if mgrCli, err = kafka.NewAdminClientFromProducer(r.instance); err != nil {
		return
	}
	var metadata *kafka.Metadata
	if metadata, err = mgrCli.GetMetadata(&cfg.Topic, false, 10000); err != nil {
		return
	}
	if _, ok := metadata.Topics[cfg.Topic]; !ok {
		err = fmt.Errorf("[KAFKA PRODUCER] topic#%s no partition", cfg.Topic)
		return
	}
	r.numPartition = int32(len(metadata.Topics[cfg.Topic].Partitions))

	rand.Seed(time.Now().UnixNano())

	if cfg.ReturnSuccesses == true {
		r.SetSucFeedback(DefaultSucFeedbackFn)
	}
	if cfg.ReturnErrors == true {
		r.SetFailFeedback(DefaultFailFeedbackFn)
	}

	for i := 0; i < util.If(cfg.ReturnFeedbackNum > 0, cfg.ReturnFeedbackNum, 10).(int); i++ {
		go r.feedback()
	}
	return
}

func (this *KafkaProducer) parsePartitioner() (r PartitionerConstructor) {
	r = NewHashPartitioner

	if v, ok := map[string]PartitionerConstructor{
		"manual": NewManualPartitioner,     // 手动选择分区
		"random": NewRandomPartitioner,     // 随机选择分区
		"round":  NewRoundRobinPartitioner, // 环形选择分区
		"hash":   NewHashPartitioner,       // hash选择分区，即使用msg中的key生成hash
	}[this.cfg.Partitioner]; ok {
		r = v
	}
	return
}

func (this *KafkaProducer) feedback() {
	for event := range this.instance.Events() {
		switch obj := event.(type) {
		case *kafka.Message:
			if obj.TopicPartition.Error == nil {
				util.IfDo(this.sucFeedback != nil, func() { this.sucFeedback(obj, string(obj.Value)) })
			} else {
				util.IfDo(this.failFeedback != nil, func() { this.failFeedback(obj, string(obj.Value)) })
			}
		case kafka.Error:
			seelog.Errorf("[KAFKA PRODUCER] notice error: %s", obj.Error())
		default:
			seelog.Infof("[KAFKA PRODUCER] ignored event: %s", obj)
		}
	}
}

func (this *KafkaProducer) Publish(body string, key string, partition int32) {
	d := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &this.cfg.Topic,
			Partition: partition},
		Key:       []byte(key),
		Value:     []byte(body),
		Timestamp: time.Now(),
	}
	d.TopicPartition.Partition, _ = this.partitioner.Partition(d, this.numPartition)
	this.instance.ProduceChannel() <- d
}

func (this *KafkaProducer) SetSucFeedback(fn func(*kafka.Message, string)) {
	this.sucFeedback = fn
}

func (this *KafkaProducer) SetFailFeedback(fn func(*kafka.Message, string)) {
	this.failFeedback = fn
}

func (this *KafkaProducer) Close() {
	// Wait for message deliveries before shutting down
	this.instance.Flush(15 * 1000)
	this.instance.Close()
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
