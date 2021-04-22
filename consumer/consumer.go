/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-15 13:27:01
# File Name: kafka/consumer/consumer.go
# Description:
####################################################################### */

package consumer

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/ant-libs-go/util"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/cihub/seelog"
)

var DefaultReceiveFn = func(topic string, body string, msg *sarama.ConsumerMessage) (err error) {
	seelog.Infof("[KAFKA] consumer receive message, topic: %s, partition:%d, offset:%d, key:%s, body:%s, tm:%s", topic, msg.Partition, msg.Offset, string(msg.Key), body, msg.Timestamp.Format("2006-01-02 15:04:05"))
	return
}

type entry struct {
	instance *cluster.Consumer
	msgCh    chan *sarama.ConsumerMessage
}

type KafkaConsumer struct {
	cfg       *Cfg
	entries   []*entry
	receiveFn func(string, string, *sarama.ConsumerMessage) (err error)
}

func NewKafkaConsumer(cfg *Cfg) (r *KafkaConsumer, err error) {
	r = &KafkaConsumer{
		cfg:       cfg,
		entries:   make([]*entry, 0, 1000),
		receiveFn: DefaultReceiveFn}
	r.cfg.ConsumeWorkerNum = util.If(r.cfg.ConsumeWorkerNum > 0, r.cfg.ConsumeWorkerNum, 1).(int)
	r.cfg.ReceiveWorkerNum = util.If(r.cfg.ReceiveWorkerNum > 0, r.cfg.ReceiveWorkerNum, 10).(int)

	kcfg := cluster.NewConfig()
	kcfg.Group.Return.Notifications = true
	kcfg.Consumer.Return.Errors = true
	kcfg.Consumer.Offsets.AutoCommit.Enable = true
	kcfg.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	kcfg.Consumer.Offsets.CommitInterval = 1 * time.Second
	kcfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	for i := 0; i < r.cfg.ConsumeWorkerNum; i++ {
		entry := &entry{msgCh: make(chan *sarama.ConsumerMessage, 1000)}
		if entry.instance, err = cluster.NewConsumer(cfg.Addrs, cfg.GroupId, cfg.Topics, kcfg); err != nil {
			return
		}
		r.entries = append(r.entries, entry)
		go r.feedback(entry)
	}
	return
}

func (this *KafkaConsumer) feedback(entry *entry) {
	for {
		select {
		case msg, ok := <-entry.instance.Messages():
			if !ok {
				continue
			}
			entry.msgCh <- msg
		case err := <-entry.instance.Errors():
			if err == nil {
				continue
			}
			seelog.Errorf("[KAFKA] consumer notice error: %s", err.Error())
		case ntf := <-entry.instance.Notifications():
			if ntf == nil {
				continue
			}
			seelog.Infof("[KAFKA] consumer rebalance: %+v", ntf)
		}
	}
}

func (this *KafkaConsumer) receive(entry *entry) {
	for {
		select {
		case msg, ok := <-entry.msgCh:
			if ok == false {
				return
			}
			if this.receiveFn == nil {
				continue
			}
			for {
				if err := this.receiveFn(msg.Topic, string(msg.Value), msg); err == nil {
					break
				}
				time.Sleep(time.Second)
			}
			entry.instance.MarkOffset(msg, "") // mark message as processed
		}
	}
}

func (this *KafkaConsumer) Receive(rcvr func(string, string, *sarama.ConsumerMessage) error) (err error) {
	this.receiveFn = rcvr

	for _, entry := range this.entries {
		for i := 0; i < this.cfg.ReceiveWorkerNum; i++ {
			go this.receive(entry)
		}
	}
	return
}

func (this *KafkaConsumer) Close() {
	for _, entry := range this.entries {
		entry.instance.Close()
		close(entry.msgCh)
	}
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
