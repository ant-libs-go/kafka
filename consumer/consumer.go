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

type KafkaConsumer struct {
	cfg       *Cfg
	instance  *cluster.Consumer
	msgCh     chan *sarama.ConsumerMessage
	receiveFn func(string, string, *sarama.ConsumerMessage) (err error)
}

func NewKafkaConsumer(cfg *Cfg) (r *KafkaConsumer, err error) {
	r = &KafkaConsumer{
		cfg:       cfg,
		msgCh:     make(chan *sarama.ConsumerMessage, 1000),
		receiveFn: DefaultReceiveFn}

	kcfg := cluster.NewConfig()
	kcfg.Group.Return.Notifications = true
	kcfg.Consumer.Return.Errors = true
	kcfg.Consumer.Offsets.AutoCommit.Enable = true
	kcfg.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	kcfg.Consumer.Offsets.CommitInterval = 1 * time.Second
	kcfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	if r.instance, err = cluster.NewConsumer(cfg.Addrs, cfg.GroupId, cfg.Topics, kcfg); err != nil {
		return
	}
	go r.feedback()
	return
}

func (this *KafkaConsumer) feedback() {
	for {
		select {
		case msg, ok := <-this.instance.Messages():
			if !ok {
				continue
			}
			this.msgCh <- msg
		case err := <-this.instance.Errors():
			if err == nil {
				continue
			}
			seelog.Errorf("[KAFKA] consumer notice error: %s", err.Error())
		case ntf := <-this.instance.Notifications():
			if ntf == nil {
				continue
			}
			seelog.Infof("[KAFKA] consumer rebalance: %+v", ntf)
		}
	}
}

func (this *KafkaConsumer) receive() {
	for {
		select {
		case msg, ok := <-this.msgCh:
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
			this.instance.MarkOffset(msg, "") // mark message as processed
		}
	}
}

func (this *KafkaConsumer) Receive(rcvr func(string, string, *sarama.ConsumerMessage) error) (err error) {
	this.receiveFn = rcvr

	for i := 0; i < util.If(this.cfg.ReceiveWorkerNum > 0, this.cfg.ReceiveWorkerNum, 10).(int); i++ {
		go this.receive()
	}
	return
}

func (this *KafkaConsumer) Close() {
	this.instance.Close()
	close(this.msgCh)
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
