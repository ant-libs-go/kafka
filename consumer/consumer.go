/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-15 13:27:01
# File Name: kafka/consumer/consumer.go
# Description:
####################################################################### */

package consumer

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/ant-libs-go/util"
	"github.com/cihub/seelog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var DefaultReceiveFn = func(topic string, worker int, body string, msg *kafka.Message) (err error) {
	seelog.Infof("[KAFKA] consumer receive message, topic: %s, worker: %d, partition:%d, offset:%d, key:%s, body:%s, tm:%s", topic, worker, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), body, msg.Timestamp.Format("2006-01-02 15:04:05"))
	return
}

var DefaultReceiveSelector = func(topic string, key string, receiveWorkerNum int, msg *kafka.Message) (r int) {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(receiveWorkerNum)
}

type entry struct {
	instance *kafka.Consumer
	msgChs   []chan *kafka.Message
}

type KafkaConsumer struct {
	cfg             *Cfg
	entries         []*entry
	receiveFn       func(string, int, string, *kafka.Message) (err error)
	receiveSelector func(topic string, key string, receiveWorkerNum int, msg *kafka.Message) (r int)
}

func NewKafkaConsumer(cfg *Cfg) (r *KafkaConsumer, err error) {
	r = &KafkaConsumer{
		cfg:       cfg,
		entries:   make([]*entry, 0, 10),
		receiveFn: DefaultReceiveFn}
	r.cfg.ConsumeWorkerNum = util.If(r.cfg.ConsumeWorkerNum > 0, r.cfg.ConsumeWorkerNum, 1).(int)
	r.cfg.ReceiveWorkerNum = util.If(r.cfg.ReceiveWorkerNum > 0, r.cfg.ReceiveWorkerNum, 10).(int)
	r.SetReceiveSelector(DefaultReceiveSelector)

	kcfg := &kafka.ConfigMap{}
	kcfg.SetKey("api.version.request", "true")
	kcfg.SetKey("auto.offset.reset", "latest")
	kcfg.SetKey("heartbeat.interval.ms", 3000)
	kcfg.SetKey("session.timeout.ms", 30000)
	kcfg.SetKey("max.poll.interval.ms", 120000)
	kcfg.SetKey("fetch.max.bytes", 1024000)
	kcfg.SetKey("max.partition.fetch.bytes", 256000)
	kcfg.SetKey("go.events.channel.enable", true)
	kcfg.SetKey("go.application.rebalance.enable", true)
	kcfg.SetKey("security.protocol", "plaintext")
	kcfg.SetKey("bootstrap.servers", strings.Join(cfg.Addrs, ","))
	kcfg.SetKey("group.id", cfg.GroupId)

	for i := 0; i < r.cfg.ConsumeWorkerNum; i++ {
		entry := &entry{msgChs: make([]chan *kafka.Message, 0, 10)}
		for i := 0; i < cfg.ReceiveWorkerNum; i++ {
			entry.msgChs = append(entry.msgChs, make(chan *kafka.Message, 500))
		}
		if entry.instance, err = kafka.NewConsumer(kcfg); err != nil {
			return
		}
		entry.instance.SubscribeTopics(cfg.Topics, nil)
		r.entries = append(r.entries, entry)
		go r.feedback(entry)
	}
	return
}

func (this *KafkaConsumer) SetReceiveSelector(fn func(topic string, key string, receiveWorkerNum int, msg *kafka.Message) int) {
	this.receiveSelector = fn
}

func (this *KafkaConsumer) feedback(entry *entry) {
	for event := range entry.instance.Events() {
		switch obj := event.(type) {
		case *kafka.Message:
			entry.msgChs[this.receiveSelector(*obj.TopicPartition.Topic, string(obj.Key), this.cfg.ReceiveWorkerNum, obj)] <- obj
		case kafka.AssignedPartitions:
			entry.instance.Assign(obj.Partitions)
		case kafka.RevokedPartitions:
			entry.instance.Unassign()
		case kafka.Error:
			seelog.Errorf("[KAFKA] consumer notice error: %s", obj.Error())
		default:
			fmt.Printf("[KAFKA] ignored event: %s\n", obj)
		}
	}
}

func (this *KafkaConsumer) receive(idx int, entry *entry) {
	for {
		select {
		case msg, ok := <-entry.msgChs[idx]:
			if ok == false {
				return
			}
			if this.receiveFn == nil {
				continue
			}
			for {
				if err := this.receiveFn(*msg.TopicPartition.Topic, idx, string(msg.Value), msg); err == nil {
					break
				}
				time.Sleep(time.Second)
			}
			entry.instance.CommitMessage(msg) // mark message as processed
		}
	}
}

func (this *KafkaConsumer) Receive(rcvr func(string, int, string, *kafka.Message) error) (err error) {
	this.receiveFn = rcvr

	for _, one := range this.entries {
		for idx := 0; idx < this.cfg.ReceiveWorkerNum; idx++ {
			go func(idx int, entry *entry) {
				this.receive(idx, entry)
			}(idx, one)
		}
	}
	return
}

func (this *KafkaConsumer) Close() {
	for _, entry := range this.entries {
		entry.instance.Close()
		for _, msgCh := range entry.msgChs {
			close(msgCh)
		}
	}
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
