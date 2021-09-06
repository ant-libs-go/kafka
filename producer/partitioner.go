/* ######################################################################
# Author: (zfly1207@126.com)
# Created Time: 2021-07-28 15:38:16
# File Name: partitioner.go
# Description:
####################################################################### */

package producer

import (
	"hash"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Partitioner interface {
	// Partition takes a message and partition count and chooses a partition
	Partition(message *kafka.Message, numPartitions int32) (r int32, err error)
}

type PartitionerConstructor func() Partitioner

type manualPartitioner struct{}

func NewManualPartitioner() Partitioner {
	return &manualPartitioner{}
}

func (this *manualPartitioner) Partition(message *kafka.Message, numPartitions int32) (r int32, err error) {
	r = message.TopicPartition.Partition
	return
}

type randomPartitioner struct {
	generator *rand.Rand
}

func NewRandomPartitioner() Partitioner {
	o := &randomPartitioner{}
	o.generator = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))
	return o
}

func (this *randomPartitioner) Partition(message *kafka.Message, numPartitions int32) (r int32, err error) {
	r = int32(this.generator.Intn(int(numPartitions)))
	return
}

type roundRobinPartitioner struct {
	partition int32
}

func NewRoundRobinPartitioner() Partitioner {
	return &roundRobinPartitioner{}
}

func (this *roundRobinPartitioner) Partition(message *kafka.Message, numPartitions int32) (r int32, err error) {
	if this.partition >= numPartitions {
		this.partition = 0
	}
	r = this.partition
	this.partition++
	return
}

type hashPartitioner struct {
	random       Partitioner
	hasher       hash.Hash32
	referenceAbs bool
}

func NewHashPartitioner() Partitioner {
	o := &hashPartitioner{}
	o.random = NewRandomPartitioner()
	o.hasher = fnv.New32a()
	o.referenceAbs = false
	return o
}

func (this *hashPartitioner) Partition(message *kafka.Message, numPartitions int32) (r int32, err error) {
	if len(message.Key) == 0 {
		return this.random.Partition(message, numPartitions)
	}
	this.hasher.Reset()
	_, err = this.hasher.Write(message.Key)
	if err != nil {
		return -1, err
	}
	// TODO
	if this.referenceAbs {
		r = (int32(this.hasher.Sum32()) & 0x7fffffff) % numPartitions
	} else {
		r = int32(this.hasher.Sum32()) % numPartitions
		if r < 0 {
			r = -r
		}
	}
	return
}

// vim: set noexpandtab ts=4 sts=4 sw=4 :
