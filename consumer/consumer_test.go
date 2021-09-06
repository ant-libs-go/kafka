/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-15 19:01:12
# File Name: consumer_test.go
# Description:
####################################################################### */

package consumer

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ant-libs-go/config"
	"github.com/ant-libs-go/config/options"
	"github.com/ant-libs-go/config/parser"
	"github.com/cihub/seelog"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestMain(m *testing.M) {
	config.New(parser.NewTomlParser(),
		options.WithCfgSource("./consumer.toml"),
		options.WithCheckInterval(1))
	os.Exit(m.Run())
}

func TestBasic(t *testing.T) {
	err := DefaultConsumerReceive(func(topic string, worker int, body string, msg *kafka.Message) error {
		seelog.Infof("[KAFKA] consumer receive message, topic: %s, worker: %d, partition:%d, offset:%d, key:%s, body:%s, tm:%s", topic, worker, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), body, msg.Timestamp.Format("2006-01-02 15:04:05"))
		if body == "testtesttest==9" {
			return fmt.Errorf("err")
		}
		return nil
	})
	if err != nil {
		fmt.Println("consumer receive err:", err)
	}
	time.Sleep(60 * time.Minute)
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
