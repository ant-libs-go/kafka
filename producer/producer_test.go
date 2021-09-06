/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-04-15 17:52:32
# File Name: producer_test.go
# Description:
####################################################################### */

package producer

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ant-libs-go/config"
	"github.com/ant-libs-go/config/options"
	"github.com/ant-libs-go/config/parser"
)

func TestMain(m *testing.M) {
	config.New(parser.NewTomlParser(),
		options.WithCfgSource("./producer.toml"),
		options.WithCheckInterval(1))
	os.Exit(m.Run())
}

func TestBasic(t *testing.T) {
	i := 0
	for {
		err := DefaultProducerPublish(fmt.Sprintf("===>%d", i), fmt.Sprintf("testtesttest==%d", i), 0)
		if err != nil {
			fmt.Println("publish err:", err)
		}
		i++
		//time.Sleep(time.Millisecond)
		time.Sleep(time.Second)
	}
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
