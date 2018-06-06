package brokers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"strings"

	"github.com/liticer/machinery/v1/common"
	"github.com/liticer/machinery/v1/config"
	"github.com/liticer/machinery/v1/log"
	"github.com/liticer/machinery/v1/tasks"
	"github.com/RichardKnop/redsync"
	"gopkg.in/redis.v5"
)

// RedisV2Broker represents a Redis broker
type RedisV2Broker struct {
	reader            common.RedisClientInterface
	writer            common.RedisClientInterface
	stopReceivingChan chan int
	stopDelayedChan   chan int
	processingWG      sync.WaitGroup // use wait group to make sure task processing completes on interrupt signal
	receivingWG       sync.WaitGroup
	delayedWG         sync.WaitGroup
	redsync           *redsync.Redsync
	Broker
}

// NewRedisV2Broker creates new RedisV2Broker instance
func NewRedisV2Broker(cnf *config.Config, reader common.RedisClientInterface, writer common.RedisClientInterface) Interface {
	return &RedisV2Broker{Broker: New(cnf), reader: reader, writer: writer}
}

// StartConsuming enters a loop and waits for incoming messages
func (b *RedisV2Broker) StartConsuming(consumerTag string, concurrency int, taskProcessor TaskProcessor) (bool, error) {
	b.startConsuming(consumerTag, taskProcessor)

	// Ping the server to make sure connection is live
	if err := b.reader.Ping().Err(); err != nil {
		b.retryFunc(b.retryStopChan)
		return b.retry, err
	}

	// Channels and wait groups used to properly close down goroutines
	b.stopReceivingChan = make(chan int)
	b.stopDelayedChan = make(chan int)
	b.receivingWG.Add(1)
	b.delayedWG.Add(1)

	// Channel to which we will push tasks ready for processing by worker
	deliveries := make(chan []byte)
	pool := make(chan struct{}, concurrency)

	// initialize worker pool with maxWorkers workers
	go func() {
		for i := 0; i < concurrency; i++ {
			pool <- struct{}{}
		}
	}()

	// Helper function to return true if parallel task processing slots still available,
	// false when we are already executing maximum allowed concurrent tasks
	var concurrencyAvailable = func() bool {
		return concurrency == 0 || (len(pool)-len(deliveries) > 0)
	}

	// Timer is added otherwise when the pools were all active it will spin the for loop
	var (
		timerDuration = time.Duration(100000000 * time.Nanosecond) // 100 miliseconds
		timer         = time.NewTimer(0)
	)
	// A receivig goroutine keeps popping messages from the queue by BLPOP
	// If the message is valid and can be unmarshaled into a proper structure
	// we send it to the deliveries channel
	go func() {
		defer b.receivingWG.Done()

		log.INFO.Print("[*] Waiting for messages. To exit press CTRL+C")

		for {
			select {
			// A way to stop this goroutine from b.StopConsuming
			case <-b.stopReceivingChan:
				return
			case <-timer.C:
				// If concurrency is limited, limit the tasks being pulled off the queue
				// until a pool is available
				if concurrencyAvailable() {
					task, err := b.nextTask(b.cnf.DefaultQueue)
					if err != nil {
						// something went wrong, wait a bit before continuing the loop
						timer.Reset(timerDuration)
						continue
					}

					deliveries <- task
				}
				if concurrencyAvailable() {
					// parallel task processing slots still available, continue loop immediately
					timer.Reset(0)
				} else {
					// using all parallel task processing slots, wait a bit before continuing the loop
					timer.Reset(timerDuration)
				}
			}
		}
	}()

	if err := b.consume(deliveries, pool, concurrency, taskProcessor); err != nil {
		return b.retry, err
	}

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()

	return b.retry, nil
}

// StopConsuming quits the loop
func (b *RedisV2Broker) StopConsuming() {
	// Stop the receiving goroutine
	b.stopReceivingChan <- 1
	// Waiting for the receiving goroutine to have stopped
	b.receivingWG.Wait()

	// Stop the delayed tasks goroutine
	b.stopDelayedChan <- 1
	// Waiting for the delayed tasks goroutine to have stopped
	b.delayedWG.Wait()

	b.stopConsuming()

	// Waiting for any tasks being processed to finish
	b.processingWG.Wait()
}

// Publish places a new message on the default queue
func (b *RedisV2Broker) Publish(signature *tasks.Signature) error {
	// Adjust routing key (this decides which queue the message will be published to)
	AdjustRoutingKey(b, signature)

	msg, err := json.Marshal(signature)
	if err != nil {
		return fmt.Errorf("JSON marshal error: %s", err)
	}

	return b.writer.RPush(signature.RoutingKey, msg).Err()
}

// GetPendingTasks returns a slice of task signatures waiting in the queue
func (b *RedisV2Broker) GetPendingTasks(queue string) ([]*tasks.Signature, error) {

	if queue == "" {
		queue = b.cnf.DefaultQueue
	}
	cmd := b.reader.LRange(queue, 0, 10)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	results, err := cmd.Result()
	if err != nil {
		return nil, err
	}

	taskSignatures := make([]*tasks.Signature, len(results))
	for i, result := range results {
		signature := new(tasks.Signature)
		decoder := json.NewDecoder(strings.NewReader(result))
		decoder.UseNumber()
		if err := decoder.Decode(signature); err != nil {
			return nil, err
		}
		taskSignatures[i] = signature
	}
	return taskSignatures, nil
}

// consume takes delivered messages from the channel and manages a worker pool
// to process tasks concurrently
func (b *RedisV2Broker) consume(deliveries <-chan []byte, pool chan struct{}, concurrency int, taskProcessor TaskProcessor) error {
	errorsChan := make(chan error, concurrency*2)

	for {
		select {
		case err := <-errorsChan:
			return err
		case d := <-deliveries:
			if concurrency > 0 {
				// get worker from pool (blocks until one is available)
				<-pool
			}

			b.processingWG.Add(1)

			// Consume the task inside a gotourine so multiple tasks
			// can be processed concurrently
			go func() {
				if err := b.consumeOne(d, taskProcessor); err != nil {
					errorsChan <- err
				}

				b.processingWG.Done()

				if concurrency > 0 {
					// give worker back to pool
					pool <- struct{}{}
				}
			}()
		case <-b.Broker.stopChan:
			return nil
		}
	}
}

// consumeOne processes a single message using TaskProcessor
func (b *RedisV2Broker) consumeOne(delivery []byte, taskProcessor TaskProcessor) error {
	signature := new(tasks.Signature)
	decoder := json.NewDecoder(bytes.NewReader(delivery))
	decoder.UseNumber()
	if err := decoder.Decode(signature); err != nil {
		return NewErrCouldNotUnmarshaTaskSignature(delivery, err)
	}

	// If the task is not registered, we requeue it,
	// there might be different workers for processing specific tasks
	if !b.IsTaskRegistered(signature.Name) {
		b.writer.RPush(b.cnf.DefaultQueue, delivery)
		return nil
	}

	log.INFO.Printf("Received new message: %s", delivery)

	return taskProcessor.Process(signature)
}

// nextTask pops next available task from the default queue
func (b *RedisV2Broker) nextTask(queue string) (result []byte, err error) {

	items, err := b.writer.BLPop(1, queue).Result()
	if err != nil {
		return []byte{}, err
	}

	// items[0] - the name of the key where an element was popped
	// items[1] - the value of the popped element
	if len(items) != 2 {
		return []byte{}, redis.Nil
	}

	return []byte(items[1]), nil
}
