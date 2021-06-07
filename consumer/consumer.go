/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-15 13:27:01
# File Name: kafka/consumer/consumer.go
# Description:
####################################################################### */

package consumer

import (
	"math/rand"
	"time"

	"github.com/Shopify/sarama"
	"github.com/ant-libs-go/util"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/cihub/seelog"
)

var DefaultReceiveFn = func(topic string, worker int, body string, msg *sarama.ConsumerMessage) (err error) {
	seelog.Infof("[KAFKA] consumer receive message, topic: %s, worker: %d, partition:%d, offset:%d, key:%s, body:%s, tm:%s", topic, worker, msg.Partition, msg.Offset, string(msg.Key), body, msg.Timestamp.Format("2006-01-02 15:04:05"))
	return
}

var DefaultReceiveSelector = func(topic string, key string, receiveWorkerNum int) (r int) {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(receiveWorkerNum)
}

type entry struct {
	instance *cluster.Consumer
	msgChs   []chan *sarama.ConsumerMessage
}

type KafkaConsumer struct {
	cfg             *Cfg
	entries         []*entry
	receiveFn       func(string, int, string, *sarama.ConsumerMessage) (err error)
	receiveSelector func(topic string, key string, receiveWorkerNum int) (r int)
}

func NewKafkaConsumer(cfg *Cfg) (r *KafkaConsumer, err error) {
	r = &KafkaConsumer{
		cfg:       cfg,
		entries:   make([]*entry, 0, 10),
		receiveFn: DefaultReceiveFn}
	r.cfg.ConsumeWorkerNum = util.If(r.cfg.ConsumeWorkerNum > 0, r.cfg.ConsumeWorkerNum, 1).(int)
	r.cfg.ReceiveWorkerNum = util.If(r.cfg.ReceiveWorkerNum > 0, r.cfg.ReceiveWorkerNum, 10).(int)
	r.SetReceiveSelector(DefaultReceiveSelector)

	kcfg := cluster.NewConfig()
	kcfg.Group.Return.Notifications = true
	kcfg.Consumer.Return.Errors = true
	kcfg.Consumer.Offsets.AutoCommit.Enable = true
	kcfg.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	kcfg.Consumer.Offsets.CommitInterval = 1 * time.Second
	kcfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	for i := 0; i < r.cfg.ConsumeWorkerNum; i++ {
		entry := &entry{msgChs: make([]chan *sarama.ConsumerMessage, 0, 10)}
		for i := 0; i < cfg.ReceiveWorkerNum; i++ {
			entry.msgChs = append(entry.msgChs, make(chan *sarama.ConsumerMessage, 500))
		}
		if entry.instance, err = cluster.NewConsumer(cfg.Addrs, cfg.GroupId, cfg.Topics, kcfg); err != nil {
			return
		}
		r.entries = append(r.entries, entry)
		go r.feedback(entry)
	}
	return
}

func (this *KafkaConsumer) SetReceiveSelector(fn func(topic string, key string, receiveWorkerNum int) int) {
	this.receiveSelector = fn
}

func (this *KafkaConsumer) feedback(entry *entry) {
	for {
		select {
		case msg, ok := <-entry.instance.Messages():
			if !ok {
				continue
			}
			entry.msgChs[this.receiveSelector(msg.Topic, string(msg.Key), this.cfg.ReceiveWorkerNum)] <- msg
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
				if err := this.receiveFn(msg.Topic, idx, string(msg.Value), msg); err == nil {
					break
				}
				time.Sleep(time.Second)
			}
			entry.instance.MarkOffset(msg, "") // mark message as processed
		}
	}
}

func (this *KafkaConsumer) Receive(rcvr func(string, int, string, *sarama.ConsumerMessage) error) (err error) {
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
