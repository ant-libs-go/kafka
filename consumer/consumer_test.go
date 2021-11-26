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
	config.NewConfig(parser.NewTomlParser(),
		options.WithCfgSource("./consumer.toml"),
		options.WithCheckInterval(1))
	os.Exit(m.Run())
}

func TestBasic(t *testing.T) {
	err := StartDefaultConsumer(func(consumeWorkerIdx int, receiveWorkerIdx int, topic string, body []byte, msg *kafka.Message) error {
		seelog.Infof("[KAFKA CONSUMER] receive message: %s#%d|%d, tm:%s, key:%s, body:%s", msg.TopicPartition.String(), consumeWorkerIdx, receiveWorkerIdx, msg.Timestamp.Format("2006-01-02 15:04:05"), string(msg.Key), string(body))
		if string(body) == "testtesttest==9" {
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
