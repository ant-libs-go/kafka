# Kafka

基于confluent-kafka-go封装的kafka库

[![License](https://img.shields.io/:license-apache%202-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![GoDoc](https://godoc.org/github.com/ant-libs-go/kafka?status.png)](http://godoc.org/github.com/ant-libs-go/kafka)
[![Go Report Card](https://goreportcard.com/badge/github.com/ant-libs-go/kafka)](https://goreportcard.com/report/github.com/ant-libs-go/kafka)

# 特性

* 简化Consumer实例初始化流程，基于配置自动对Consumer进行初始化且启动
* 简化Producer实例初始化流程，基于配置自动对Producer进行初始化且启动

## 安装

	go get github.com/ant-libs-go/http

# KafkaConsumer 快速开始

* toml 配置文件
    ```
    [kafka.consumer.default]
        addrs = ["127.0.0.1:9092"]
        topics = ["business"]
        group_id = "consumertest"
        consume_worker_num = 1
        receive_worker_num = 1
    ```

* 使用方法

	```golang
    // 初始化config包，参考config模块
    code...

    // 如下方式可以直接使用kafka consumer实例
    err := kafka.Receive("default", func(topic string, worker int, body string, msg *kafka.Message) error {
        fmt.Println(body)
    }); 
    if err != nil {
        fmt.Printf("[ERROR] Build kafka consumer error: %s\n", err)
        os.Exit(-1)
    }

    // 停止kafka consumer
    kafka.CloseConsumer("default")
    ```

# KafkaProducer 快速开始

* toml 配置文件
    ```
    [kafka.producer.default]
        addrs = ["127.0.0.1:9092"]
        acks = 1
        topic = "business"
        partitioner = "hash"
        return_successes = true
        return_errors = true
        return_feedback_num = 10
    ```

* 使用方法
	```golang
    // 初始化config包，参考config模块
    code...

    err := kafka.Publish("default", "bodystring", "keystring", 0)
    if err == nil {
        fmt.Println(err)
    }
    ```
