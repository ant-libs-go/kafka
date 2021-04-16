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

	"github.com/Shopify/sarama"
	"github.com/ant-libs-go/config"
	"github.com/ant-libs-go/config/options"
	"github.com/ant-libs-go/config/parser"
)

var globalCfg *config.Config

func TestMain(m *testing.M) {
	config.New(parser.NewTomlParser(),
		options.WithCfgSource("./consumer.toml"),
		options.WithCheckInterval(1))
	os.Exit(m.Run())
}

func TestBasic(t *testing.T) {
	err := DefaultConsumerReceive(func(topic string, body []byte, msg *sarama.ConsumerMessage) {
		fmt.Println("topic:", topic, "body:", string(body))
	})
	if err != nil {
		fmt.Println("consumer receive err:", err)
	}
	time.Sleep(60 * time.Minute)
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
