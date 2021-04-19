/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-14 16:49:22
# File Name: producer.go
# Description:
####################################################################### */

package producer

import (
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ant-libs-go/util"
	"github.com/cihub/seelog"
)

type KafkaProducer struct {
	cfg          *Cfg
	instance     sarama.AsyncProducer
	sucFeedback  func(*sarama.ProducerMessage, string)
	failFeedback func(*sarama.ProducerError, string)
}

var (
	DefaultSucFeedbackFn = func(suc *sarama.ProducerMessage, msg string) {
		// seelog.Errorf("[KAFKA] producer publish success, topic: %s, message: %s", suc.Topic, msg)
	}
	DefaultFailFeedbackFn = func(fail *sarama.ProducerError, msg string) {
		seelog.Errorf("[KAFKA] producer publish err: %s, topic: %s, message: %s", fail.Error(), fail.Msg.Topic, msg)
	}
)

func NewKafkaProducer(cfg *Cfg) (r *KafkaProducer, err error) {
	r = &KafkaProducer{cfg: cfg}

	kcfg := sarama.NewConfig()
	kcfg.Producer.RequiredAcks = sarama.RequiredAcks(cfg.Acks)
	kcfg.Producer.Partitioner = sarama.NewHashPartitioner // hash分区
	kcfg.Producer.Return.Successes = cfg.ReturnSuccesses
	kcfg.Producer.Return.Errors = cfg.ReturnErrors
	kcfg.Version = sarama.V0_10_2_1

	if r.instance, err = sarama.NewAsyncProducer(cfg.Addrs, kcfg); err != nil {
		return
	}
	rand.Seed(time.Now().UnixNano())

	if kcfg.Producer.Return.Successes == true {
		r.SetSucFeedback(DefaultSucFeedbackFn)
	}
	if kcfg.Producer.Return.Errors == true {
		r.SetFailFeedback(DefaultFailFeedbackFn)
	}

	for i := 0; i < util.If(cfg.ReturnFeedbackNum > 0, cfg.ReturnFeedbackNum, 10).(int); i++ {
		go r.feedback()
	}
	return
}

func (this *KafkaProducer) feedback() {
	for {
		select {
		case suc, ok := <-this.instance.Successes():
			if !ok {
				continue
			}
			if this.sucFeedback == nil {
				continue
			}
			msg, _ := suc.Value.Encode()
			this.sucFeedback(suc, string(msg))
		case fail, ok := <-this.instance.Errors():
			if !ok {
				continue
			}
			if this.failFeedback == nil {
				continue
			}
			msg, _ := fail.Msg.Value.Encode()
			this.failFeedback(fail, string(msg))
		}
	}
}

func (this *KafkaProducer) Publish(topic string, key string, msg string) {
	if len(topic) == 0 {
		topic = this.cfg.Topic
	}
	this.instance.Input() <- &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(msg),
		Timestamp: time.Now()}
}

func (this *KafkaProducer) SetSucFeedback(fn func(*sarama.ProducerMessage, string)) {
	this.sucFeedback = fn
}

func (this *KafkaProducer) SetFailFeedback(fn func(*sarama.ProducerError, string)) {
	this.failFeedback = fn
}

func (this *KafkaProducer) Close() {
	this.instance.AsyncClose()
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
